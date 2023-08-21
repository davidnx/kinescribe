using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoLock;
using Kinescribe.Internals;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kinescribe
{
    public class StreamSubscriber : IStreamSubscriber
    {
        private readonly IAmazonDynamoDB _dynamoClient;
        private readonly IAmazonDynamoDBStreams _streamsClient;
        private readonly ILoggerFactory _loggerFactory;
        private readonly StreamSubscriberOptions _options;
        private readonly ILogger<StreamSubscriber> _logger;

        private readonly IShardTableProvisioner _provisioner;
        private readonly IShardStateClient _shardStateClient;
        private readonly IDistributedLockManager _lockManager;

        private readonly object _syncRoot = new object();
        private int _numExecutions;
        private CancellationTokenSource _lockManagerCts;
        private Task _lockManagerTask;

        public StreamSubscriber(
            IAmazonDynamoDB dynamoClient,
            IAmazonDynamoDBStreams streamsClient,
            IOptions<StreamSubscriberOptions> options,
            ILoggerFactory loggerFactory)
            : this(
                  dynamoClient,
                  streamsClient,
                  options,
                  loggerFactory,
                  new ShardTableProvisioner(dynamoClient, options, loggerFactory.CreateLogger<ShardTableProvisioner>()),
                  new ShardStateClient(dynamoClient, options.Value.ShardTableName),
                  new DynamoDbLockManager(dynamoClient, Options.Create(options.Value.LockOptions), loggerFactory))
        {
        }

        /// <summary>
        /// This is intended for unit tests.
        /// </summary>
        public StreamSubscriber(
            IAmazonDynamoDB dynamoClient,
            IAmazonDynamoDBStreams streamsClient,
            IOptions<StreamSubscriberOptions> options,
            ILoggerFactory loggerFactory,
            IShardTableProvisioner provisioner,
            IShardStateClient shardStateClient,
            IDistributedLockManager lockManager)
        {
            _dynamoClient = dynamoClient ?? throw new ArgumentNullException(nameof(dynamoClient));
            _streamsClient = streamsClient ?? throw new ArgumentNullException(nameof(streamsClient));
            _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            _options = options?.Value ?? throw new ArgumentNullException(nameof(options));

            _logger = loggerFactory.CreateLogger<StreamSubscriber>();
            _provisioner = provisioner ?? throw new ArgumentNullException(nameof(provisioner));
            _shardStateClient = shardStateClient ?? throw new ArgumentNullException(nameof(shardStateClient));
            _lockManager = lockManager ?? throw new ArgumentNullException(nameof(lockManager));
        }

        public async Task ExecuteAsync(string appName, string tableName, Func<Record, CancellationToken, Task> action, CancellationToken cancellation, int batchSize = 100)
        {
            await _provisioner.ProvisionAsync(cancellation);

            var streamArn = await GetStreamArnForTable(tableName, cancellation);

            // Start _lockManager on-demand only while there's an active ExecuteAsync call happening...
            lock (_syncRoot)
            {
                if (_numExecutions++ == 0)
                {
                    _logger.LogInformation("Starting distributed lock manager");
                    Debug.Assert(_lockManagerCts == null && _lockManagerTask == null);
                    _lockManagerCts = new CancellationTokenSource();
                    _lockManagerTask = _lockManager.ExecuteAsync(_lockManagerCts.Token);
                }
            }

            try
            {
                while (!cancellation.IsCancellationRequested)
                {
                    string lockId = $"stream/{Uri.EscapeDataString(streamArn)}/app/{Uri.EscapeDataString(appName)}/global";
                    var acquisition = await _lockManager.AcquireLockAsync(lockId, cancellation);
                    if (!acquisition.Acquired)
                    {
                        _logger.LogInformation("Distributed lock {lockId} is taken", lockId);
                        await Task.Delay(_options.LockAcquisitionInterval, cancellation);
                        continue;
                    }

                    _logger.LogInformation("Acquired distributed lock {lockId}", lockId);
                    using var gracefulCancellation = new GracefulCancellation(cancellation, _options.TeardownTimeout);
                    try
                    {
                        using (acquisition.LockLost.Register(() =>
                        {
                            _logger.LogWarning("Distributed lock was lost, forcefully aborting all operations");
                            gracefulCancellation.Abort();
                        }))
                        {
                            await ExecuteCoreAsync(appName, streamArn, action, batchSize, gracefulCancellation);
                        }
                    }
                    catch (OperationCanceledException) when (gracefulCancellation.StoppingToken.IsCancellationRequested)
                    {
                        // Graceful shutdown or we lost the distributed lock, either way wrap up and then try again...
                    }
                    catch (Exception ex)
                    {
                        if (ex is BenignStartOverException benignEx)
                        {
                            _logger.LogInformation("Outer loop iteration terminated: {message}. Will start over...", benignEx.Message);
                        }
                        else
                        {
                            _logger.LogWarning(ex, "Outer loop failed (this could be benign). Will start over...");
                            await Task.Delay(_options.SnoozeTime, gracefulCancellation.StoppingToken);
                        }
                    }
                    finally
                    {
                        if (!gracefulCancellation.GracefulToken.IsCancellationRequested)
                        {
                            try
                            {
                                await acquisition.ReleaseLockAsync(gracefulCancellation.GracefulToken);
                            }
                            catch (OperationCanceledException ex) when (gracefulCancellation.GracefulToken.IsCancellationRequested)
                            {
                                _logger.LogWarning(ex, "Unable to release distributed lock {lockId} within the graceful iteration teardown period", lockId);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogWarning(ex, "Failed to release distributed lock {lockId}", lockId);
                            }
                        }
                    }
                }
            }
            finally
            {
                Task lockManagerTaskToWait = null;
                lock (_syncRoot)
                {
                    if (--_numExecutions == 0)
                    {
                        Debug.Assert(_lockManagerCts != null && _lockManagerTask != null);
                        lockManagerTaskToWait = _lockManagerTask;

                        _logger.LogInformation("Stopping distributed lock manager");
                        _lockManagerCts.Cancel();
                        _lockManagerCts = null;
                        _lockManagerTask = null;
                    }
                }

                if (lockManagerTaskToWait != null)
                {
                    try
                    {
                        await lockManagerTaskToWait;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to stop distributed lock manager");
                    }
                }
            }
        }

        private async Task<string> GetStreamArnForTable(string tableName, CancellationToken cancellation)
        {
            var tableDescription = await _dynamoClient.DescribeTableAsync(tableName, cancellation);
            var streamArn = tableDescription.Table.LatestStreamArn;
            if (string.IsNullOrEmpty(streamArn))
            {
                throw new InvalidOperationException($"Could not determine stream ARN for table {tableName}");
            }

            _logger.LogInformation("Found stream ARN for table {tableName}: {streamArn}", tableName, streamArn);
            return streamArn;
        }

        private async Task ExecuteCoreAsync(string appName, string streamArn, Func<Record, CancellationToken, Task> action, int batchSize, GracefulCancellation cancellation)
        {
            var shards = await ListShards(streamArn, cancellation.StoppingToken);
            var tree = new ShardTree(shards);
            _logger.LogInformation("Found {numShards} shards, {numRoots} roots, {numLeaves} leaves", shards.Count, tree.Roots.Length, tree.NumLeaves);
            _logger.LogDebug("Shard tree:\n{tree}", tree);

            int numReachedLeaf = 0;
            var processorOptions = new ShardTreeNodeProcessorOptions
            {
                AppName = appName,
                StreamArn = streamArn,
                Action = action,
                OnReachedLeaf = (node) =>
                {
                    int currentNumReachedLeaf = Interlocked.Increment(ref numReachedLeaf);
                    _logger.LogDebug("Reached {numReachedLeaf} of {numLeaves} leaves", currentNumReachedLeaf, tree.NumLeaves);
                    if (currentNumReachedLeaf >= tree.NumLeaves)
                    {
                        _logger.LogInformation("Reached all leaves ({numLeaves})", tree.NumLeaves);
                    }
                },
                OnFailure = (node, ex) =>
                {
                    if (!cancellation.StoppingToken.IsCancellationRequested)
                    {
                        if (ex is not BenignStartOverException)
                        {
                            _logger.LogError(ex, "Processing failed for shard '{shardId}', stopping gracefully...", node?.ShardId);
                        }

                        cancellation.RequestStop();
                    }
                },
                BatchSize = batchSize,
                SnoozeTime = _options.SnoozeTime,
                RetryCallbackSnoozeTime = _options.RetryCallbackSnoozeTime,
                MaxCheckpointLagInterval = _options.MaxCheckpointLagInterval,
                MaxCheckpointLagRecords = _options.MaxCheckpointLagRecords,
            };
            var processor = new ShardNodeProcessor(_streamsClient, _shardStateClient, processorOptions, _loggerFactory.CreateLogger<ShardNodeProcessor>());
            var tasks = new List<Task>(tree.Roots.Length);
            foreach (var root in tree.Roots)
            {
                tasks.Add(processor.ProcessAsync(root, cancellation));
            }

            await Task.WhenAll(tasks);
        }

        private async Task<List<Shard>> ListShards(string streamArn, CancellationToken cancellation)
        {
            var shards = new List<Shard>();
            string exclusiveStartShardId = null;
            do
            {
                var request = new DescribeStreamRequest
                {
                    StreamArn = streamArn,
                    ExclusiveStartShardId = exclusiveStartShardId,
                };
                var resp = await _streamsClient.DescribeStreamAsync(request, cancellation);
                shards.AddRange(resp.StreamDescription.Shards);
                exclusiveStartShardId = resp.StreamDescription.LastEvaluatedShardId;
            } while (!string.IsNullOrEmpty(exclusiveStartShardId));
            return shards;
        }
    }
}
