using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Microsoft.Extensions.Logging;

namespace Kinescribe.Internals
{
    /// <summary>
    /// Handles a node of a shard tree, including all of its children.
    /// Parent nodes are consumed to completion, children are then processed in parallel recursively.
    /// </summary>
    internal class ShardNodeProcessor
    {
        private readonly IAmazonDynamoDBStreams _streamsClient;
        private readonly IShardStateClient _shardStateClient;
        private readonly ShardTreeNodeProcessorOptions _options;
        private readonly ILogger<ShardNodeProcessor> _logger;

        public ShardNodeProcessor(
            IAmazonDynamoDBStreams streamsClient,
            IShardStateClient shardStateClient,
            ShardTreeNodeProcessorOptions options,
            ILogger<ShardNodeProcessor> logger)
        {
            _streamsClient = streamsClient ?? throw new ArgumentNullException(nameof(streamsClient));
            _shardStateClient = shardStateClient ?? throw new ArgumentNullException(nameof(shardStateClient));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task ProcessAsync(ShardNode node, GracefulCancellation cancellation)
        {
            _ = node ?? throw new ArgumentNullException(nameof(node));

            try
            {
                if (node.Children.Length == 0)
                {
                    _options.OnReachedLeaf?.Invoke(node);
                }

                await ProcessSingleNodeAsync(node, cancellation);
                if (node.Children.Length == 0)
                {
                    // TODO: Only refresh children for this shard instead of forcing everything to start over??
                    _logger.LogWarning($"Reached end of leaf shard '{node.ShardId}'. This likely means the shard topology changed and new children were added, so we should update the list of shards");
                    throw new InvalidOperationException($"Reached end of leaf shard {node.ShardId}, shard list has to be refreshed");
                }
                else
                {
                    var childTasks = new List<Task>(node.Children.Length);
                    foreach (var child in node.Children)
                    {
                        childTasks.Add(((Func<Task>)(async () =>
                        {
                            await Task.Yield();
                            await ProcessAsync(child, cancellation);
                        }))());
                    }

                    await Task.WhenAll(childTasks);
                }
            }
            catch (Exception ex)
            {
                if (ex is OperationCanceledException && cancellation.StoppingToken.IsCancellationRequested)
                {
                    // Graceful cancellation, or teardown after a failure.
                    // Allow this to bubble out, but don't report a global failure.
                }
                else
                {
                    _options.OnFailure(node, ex);
                }

                throw;
            }
        }

        private async Task ProcessSingleNodeAsync(ShardNode node, GracefulCancellation cancellation)
        {
            _logger.LogDebug("Processing shard {shardId}", node.ShardId);
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                var path = node.ShardId;
                var parent = node.Parent;
                while (parent != null)
                {
                    path = parent.ShardId + "/" + path;
                    parent = parent.Parent;
                }

                _logger.LogTrace("Processing shard with path {path}", path);
            }

            // Note: could be null if we weren't tracking this yet...
            var shardState = await _shardStateClient.GetAsync(app: _options.AppName, streamArn: _options.StreamArn, shardId: node.ShardId, cancellation.StoppingToken);
            var checkpointLagTimer = Stopwatch.StartNew();
            long checkpointLagCounter = 0;
            ShardStateDto unsavedNewState = null;

            while (true)
            {
                // Using `long` instead of `TimeSpan` so that we can get the compiler to ascertain this is definiteively assigned in all code paths
                long delayTicks;

                try
                {
                    try
                    {
                        var batchResult = await GetBatch(node, shardState, cancellation.StoppingToken);
                        if (batchResult == null)
                        {
                            _logger.LogDebug("Shard was already finished: {shardId}", node.ShardId);
                            return;
                        }

                        var (resp, currentIterator) = batchResult.Value;
                        Debug.Assert(resp != null && currentIterator != null);

                        string lastSequence = null;
                        bool error = false;
                        foreach (var record in resp.Records)
                        {
                            try
                            {
                                await _options.Action(record, cancellation.StoppingToken);
                                lastSequence = record.Dynamodb.SequenceNumber;
                                checkpointLagCounter++;
                            }
                            catch (OperationCanceledException ex) when (cancellation.StoppingToken.IsCancellationRequested)
                            {
                                _logger.LogWarning(ex, "Consumer was canceled while processing stream record {sequenceNumber}. This shard will be retried later...", record.Dynamodb.SequenceNumber);
                                error = true;
                                break;
                            }
                            catch (Exception ex)
                            {
                                _logger.LogWarning(ex, "Consumer encountered an error processing stream record {sequenceNumber}. This shard will be retried later...", record.Dynamodb.SequenceNumber);
                                error = true;
                                break;
                            }
                        }

                        ShardStateDto newState = null;
                        if (error)
                        {
                            // Checkpoint only the progress we made, if any...
                            if (lastSequence != null)
                            {
                                newState = new ShardStateDto
                                {
                                    App = _options.AppName,
                                    StreamArn = _options.StreamArn,
                                    ShardId = node.ShardId,
                                    NextIterator = currentIterator,
                                    LastSequenceNumber = lastSequence ?? shardState?.LastSequenceNumber,
                                };
                            }
                        }
                        else
                        {
                            newState = new ShardStateDto
                            {
                                App = _options.AppName,
                                StreamArn = _options.StreamArn,
                                ShardId = node.ShardId,
                                NextIterator = resp.NextShardIterator,
                                LastSequenceNumber = lastSequence ?? shardState?.LastSequenceNumber,
                            };
                        }

                        if (newState != null)
                        {
                            if (string.IsNullOrEmpty(resp.NextShardIterator) ||// finished this shard
                                checkpointLagTimer.Elapsed >= _options.MaxCheckpointLagInterval ||
                                checkpointLagCounter >= _options.MaxCheckpointLagRecords)
                            {
                                // NOTE: Reset to null here, before attempting to actually save, so we don't end up retrying during a graceful shutdown below.
                                // The intent of saving on shutdown isn't to retry a failed save, rather as a best effort attempt to save once during graceful teardown.
                                unsavedNewState = null;

                                newState.WriteTime = DateTimeOffset.UtcNow;
                                _logger.LogTrace("Checkpointing for shard '{shardId}': {newState}", node.ShardId, newState);
                                await _shardStateClient.PutAsync(newState, cancellation.GracefulToken);

                                checkpointLagTimer.Restart();
                                checkpointLagCounter = 0;
                            }
                            else
                            {
                                unsavedNewState = newState;
                            }

                            // Need to update shardState for next iteration even if we didn't persist it yet...
                            shardState = newState;
                        }

                        if (error)
                        {
                            delayTicks = _options.RetryCallbackSnoozeTime.Ticks;
                        }
                        else
                        {
                            if (string.IsNullOrEmpty(resp.NextShardIterator))
                            {
                                _logger.LogInformation("Shard is finished: {shardId}", node.ShardId);
                                return;
                            }

                            if (node.Children.Length > 0)
                            {
                                // This shard must be closed (since it has children), so it is safe to continue iterating as quickly as possible so we get past it...
                                delayTicks = 0;
                            }
                            else
                            {
                                if (resp.Records.Count == 0)
                                {
                                    // TODO: Maybe we aren't done on this shard, how do we know?
                                    // Delaying _options.SnoozeTime between calls could lead to very long delays until we are synced up again...
                                    //
                                    // See: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetRecords.html:
                                    //    > If there are no stream records available in the portion of the shard that the iterator points to,
                                    //    > GetRecords returns an empty list. Note that it might take multiple calls
                                    //    > to get to a portion of the shard that contains stream records.
                                    delayTicks = _options.SnoozeTime.Ticks;
                                }
                                else
                                {
                                    delayTicks = 0;
                                }
                            }
                        }
                    }
                    catch (OperationCanceledException) when (cancellation.StoppingToken.IsCancellationRequested)
                    {
                        // Graceful shutdown...
                        throw;
                    }
                    catch (Exception ex)
                    {
                        delayTicks = _options.RetryCallbackSnoozeTime.Ticks;
                        _logger.LogError(ex, $"{nameof(ProcessSingleNodeAsync)} iteration failed");
                    }

                    if (delayTicks > 0)
                    {
                        await Task.Delay(TimeSpan.FromTicks(delayTicks), cancellation.StoppingToken);
                    }
                }
                finally
                {
                    if (cancellation.StoppingToken.IsCancellationRequested)
                    {
                        if (unsavedNewState != null)
                        {
                            unsavedNewState.WriteTime = DateTimeOffset.UtcNow;
                            _logger.LogDebug("Checkpointing shard '{shardId}' on teardown: {newState}", node.ShardId, unsavedNewState);
                            await _shardStateClient.PutAsync(unsavedNewState, cancellation.GracefulToken);
                        }

                        cancellation.StoppingToken.ThrowIfCancellationRequested();
                        throw new InvalidOperationException("Unreachable code");
                    }
                }
            }
        }

        /// <summary>
        /// Returns null if the shard had already been processed to completion
        /// </summary>
        /// <param name="node"></param>
        /// <param name="cancellation"></param>
        /// <returns>
        /// Null if the shard had already been processed to completion.
        /// </returns>
        private async Task<(GetRecordsResponse Response, string CurrentIterator)?> GetBatch(ShardNode node, ShardStateDto stateOrNull, CancellationToken cancellation)
        {
            string iterator;
            if (stateOrNull == null)
            {
                _logger.LogDebug("Shard iterator isn't tracked yet for shard '{shardId}'", node.ShardId);
                iterator = await GetIteratorOrTrimHorizon(
                    shardId: node.ShardId,
                    iteratorType: ShardIteratorType.AT_SEQUENCE_NUMBER,
                    sequenceNumber: node.Shard.SequenceNumberRange.StartingSequenceNumber,
                    cancellation);
            }
            else if (string.IsNullOrEmpty(stateOrNull.NextIterator))
            {
                // Shard is completed!
                _logger.LogDebug("Shard '{shardId}' is already complete", node.ShardId);
                return null;
            }
            else
            {
                iterator = stateOrNull.NextIterator;
                _logger.LogTrace("Resuming shard '{shardId}' from iterator {iterator}", node.ShardId, iterator);
            }

            try
            {
                var result = await _streamsClient.GetRecordsAsync(
                    new GetRecordsRequest()
                    {
                        ShardIterator = iterator,
                        Limit = _options.BatchSize,
                    },
                    cancellation);
                _logger.Log(
                    result.Records.Count > 0 ? LogLevel.Debug : LogLevel.Trace,
                    "Got {numRecords} records from shard {shardId}",
                    result.Records.Count,
                    node.ShardId);
                return (result, iterator);
            }
            catch (ExpiredIteratorException)
            {
                var lastSequence = stateOrNull?.LastSequenceNumber;
                if (string.IsNullOrEmpty(lastSequence))
                {
                    throw new InvalidOperationException($"Encountered {nameof(ExpiredIteratorException)} while accessing an iterator we had just created for shard '{node.ShardId}'. Aborting current execution, starting over should help.");
                }

                _logger.LogInformation($"Encountered {nameof(ExpiredIteratorException)} for shard '{{shardId}}'. Retrying with lastSequence '{{lastSequence}}'", node.ShardId, lastSequence);

                iterator = await GetIteratorOrTrimHorizon(
                    shardId: node.ShardId,
                    iteratorType: ShardIteratorType.AFTER_SEQUENCE_NUMBER,
                    sequenceNumber: lastSequence,
                    cancellation);

                var result = await _streamsClient.GetRecordsAsync(
                    new GetRecordsRequest()
                    {
                        ShardIterator = iterator,
                        Limit = _options.BatchSize
                    },
                    cancellation);
                _logger.LogDebug("Found {numRecords} in shard {shardId} (after iterator expired)", result.Records.Count, node.ShardId);
                return (result, iterator);
            }
        }

        private async Task<string> GetIteratorOrTrimHorizon(string shardId, ShardIteratorType iteratorType, string sequenceNumber, CancellationToken cancellation)
        {
            Debug.Assert(iteratorType == ShardIteratorType.AT_SEQUENCE_NUMBER || iteratorType == ShardIteratorType.AFTER_SEQUENCE_NUMBER);
            try
            {
                var iterResp = await _streamsClient.GetShardIteratorAsync(
                    new GetShardIteratorRequest()
                    {
                        ShardId = shardId,
                        StreamArn = _options.StreamArn,
                        ShardIteratorType = iteratorType,
                        SequenceNumber = sequenceNumber,
                    },
                    cancellation);
                return iterResp.ShardIterator;
            }
            catch (TrimmedDataAccessException ex)
            {
                _logger.LogDebug(ex, $"Encountered {nameof(TrimmedDataAccessException)} while trying to get iterator {{iteratorType}} for shard '{{shardId}}' at {{sequenceNumber}}", iteratorType.Value, shardId, sequenceNumber);
                var iterResp = await _streamsClient.GetShardIteratorAsync(
                    new GetShardIteratorRequest()
                    {
                        ShardId = shardId,
                        StreamArn = _options.StreamArn,
                        ShardIteratorType = ShardIteratorType.TRIM_HORIZON,
                    },
                    cancellation);
                return iterResp.ShardIterator;
            }
        }
    }

    internal class ShardTreeNodeProcessorOptions
    {
        public string AppName { get; set; }
        public string StreamArn { get; set; }
        public Func<Record, CancellationToken, Task> Action { get; set; }

        /// <summary>
        /// Invoked when <see cref="ShardNodeProcessor"/> has reached a leaf node and is about to start processing it.
        /// This is useful for the consumer to determine when it is getting in sync with the subscribed stream.
        /// While reaching the leaf doesn't mean we are fully in sync, it is at least a prereq to being in sync.
        /// </summary>
        public Action<ShardNode> OnReachedLeaf { get; set; }

        /// <summary>
        /// Invoked when a non-recoverable failure happens while processing a node. This causes the entire processing chain to shutdown and start over.
        /// </summary>
        public Action<ShardNode, Exception> OnFailure { get; set; }

        public TimeSpan SnoozeTime { get; set; } = TimeSpan.FromSeconds(3);
        public TimeSpan RetryCallbackSnoozeTime { get; set; } = TimeSpan.FromSeconds(15);
        public int BatchSize { get; set; } = 100;

        /// <summary>
        /// See <see cref="StreamSubscriberOptions.MaxCheckpointLagInterval"/>.
        /// </summary>
        public TimeSpan MaxCheckpointLagInterval { get; set; }

        /// <summary>
        /// See <see cref="StreamSubscriberOptions.MaxCheckpointLagRecords"/>.
        /// </summary>
        public int MaxCheckpointLagRecords { get; set; }
    }
}
