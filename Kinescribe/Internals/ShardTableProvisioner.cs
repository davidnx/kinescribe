using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kinescribe.Internals
{
    internal class ShardTableProvisioner : IShardTableProvisioner
    {
        private readonly IAmazonDynamoDB _client;
        private readonly StreamSubscriberOptions _options;
        private readonly ILogger _logger;

        public ShardTableProvisioner(
            IAmazonDynamoDB dynamoClient,
            IOptions<StreamSubscriberOptions> options,
            ILogger<ShardTableProvisioner> logger)
        {
            _client = dynamoClient ?? throw new ArgumentNullException(nameof(dynamoClient));
            _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task ProvisionAsync(CancellationToken cancellation)
        {
            try
            {
                try
                {
                    var table = await _client.DescribeTableAsync(_options.ShardTableName, cancellation);
                    if (table.Table.TableStatus == TableStatus.ACTIVE)
                    {
                        _logger.LogInformation($"Shard table {_options.ShardTableName} already exists, all good");
                        return;
                    }
                    else if (table.Table.TableStatus == TableStatus.CREATING)
                    {
                        await WaitForTableActivation(cancellation);
                    }
                }
                catch (ResourceNotFoundException)
                {
                    await CreateTable(cancellation);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Error creating lock table", ex);
            }
        }

        private async Task CreateTable(CancellationToken cancellation)
        {
            _logger.LogInformation($"Creating shards table {_options.ShardTableName}");
            var createRequest = new CreateTableRequest(_options.ShardTableName, new List<KeySchemaElement>()
            {
                new KeySchemaElement("id", KeyType.HASH)
            })
            {
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition("id", ScalarAttributeType.S)
                },
            };

            if (_options.TableBillingMode == CursorTableBillingMode.PayPerRequest)
            {
                createRequest.BillingMode = BillingMode.PAY_PER_REQUEST;
            }
            else if (_options.TableBillingMode == CursorTableBillingMode.MinimalProvisionedThroughput)
            {
                createRequest.ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 1,
                    WriteCapacityUnits = 1,
                };
            }
            else
            {
                throw new InvalidOperationException($"Unknown {nameof(_options.TableBillingMode)}: {_options.TableBillingMode}");
            }

            await _client.CreateTableAsync(createRequest, cancellation);
            await WaitForTableActivation(cancellation);
        }

        private async Task WaitForTableActivation(CancellationToken cancellation)
        {
            _logger.LogInformation($"Waiting for Active state for shards table {_options.ShardTableName}");

            const int NumAttempts = 20;
            for (int i = 0; i < NumAttempts; i++)
            {
                try
                {
                    await Task.Delay(1000, cancellation);
                    var poll = await _client.DescribeTableAsync(_options.ShardTableName, cancellation);
                    _logger.LogInformation($"Status for shard table {_options.ShardTableName}: {poll.Table.TableStatus}");
                    if (poll.Table.TableStatus == TableStatus.ACTIVE)
                    {
                        return;
                    }
                }
                catch (ResourceNotFoundException)
                {
                    _logger.LogInformation($"Shards table {_options.ShardTableName} doesn't exist yet...");
                }
            }

            throw new InvalidOperationException($"Creation of shards table {_options.ShardTableName} still not complete after {NumAttempts} polling attempts");
        }
    }
}
