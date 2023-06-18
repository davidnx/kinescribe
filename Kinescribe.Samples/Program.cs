using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using DynamoLock;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kinescribe.Samples
{
    class Program
    {
        static async Task Main(string[] args)
        {
            using var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddConsole();
            });

            var logger = loggerFactory.CreateLogger<Program>();
            logger.LogInformation("Starting...");

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

            var task = subscriber.ExecuteAsync("my-app", tableName: "dummy1", record =>
            {
                Console.WriteLine($"Got event {record.Dynamodb.SequenceNumber} - {record.EventName.Value}: {Document.FromAttributeMap(record.Dynamodb.NewImage).ToJson()}");
            }, CancellationToken.None);

            await Publish(dynamoClient);
            await task;
        }

        static async Task Publish(IAmazonDynamoDB client)
        {
            Console.WriteLine("Writing item...");
            await client.PutItemAsync(
                new PutItemRequest
                {
                    TableName = "dummy1",
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { "id", new AttributeValue(Guid.NewGuid().ToString()) },
                    },
                });
        }
    }
}
