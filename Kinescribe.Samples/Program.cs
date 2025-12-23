using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.Runtime;
using DynamoLock;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBStreams;

namespace Kinescribe.Samples
{
    class Program
    {
        static async Task Main(string[] args)
        {
            using var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddConsole();
                builder.SetMinimumLevel(LogLevel.Debug);
            });

            var logger = loggerFactory.CreateLogger<Program>();
            logger.LogInformation("Starting...");

            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (object sender, ConsoleCancelEventArgs e) =>
            {
                logger.LogInformation("Stopping...");
                e.Cancel = true;
                cts.Cancel();
            };

            var credentials = new EnvironmentVariablesAWSCredentials();
            var dynamoClient = new AmazonDynamoDBClient(credentials, RegionEndpoint.SAEast1);
            var streamsClient = new AmazonDynamoDBStreamsClient(credentials, RegionEndpoint.SAEast1);
            var options = Options.Create(
                new StreamSubscriberOptions
                {
                    ShardTableName = "kinescribe-sample-shards",
                    LockOptions = new DynamoDbLockOptions
                    {
                        TableName = "kinescribe-sample-lock",
                    },
                });
            var subscriber = new StreamSubscriber(dynamoClient, streamsClient, options, loggerFactory);

            var task = subscriber.ExecuteAsync("my-app", tableName: "dummy1", (record, _) =>
            {
                Console.WriteLine($"Got event {record.Dynamodb.SequenceNumber} - {record.EventName.Value}: {JsonSerializer.Serialize(record.Dynamodb.NewImage)}");
                return Task.CompletedTask;
            }, cts.Token);

            await Publish(dynamoClient, cts.Token);
            await task;
        }

        static async Task Publish(IAmazonDynamoDB client, CancellationToken cancellation)
        {
            Console.WriteLine("Writing item...");
            await client.PutItemAsync(
                new PutItemRequest
                {
                    TableName = "dummy1",
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { "id", new AttributeValue { S=Guid.NewGuid().ToString() } },
                    },
                },
                cancellation);
        }
    }
}
