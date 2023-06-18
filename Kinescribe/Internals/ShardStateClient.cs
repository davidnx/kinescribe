using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace Kinescribe.Internals
{
    internal class ShardStateClient : IShardStateClient
    {
        private readonly IAmazonDynamoDB _client;
        private readonly string _tableName;

        public ShardStateClient(IAmazonDynamoDB client, string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentException(nameof(tableName));
            }

            _client = client ?? throw new ArgumentNullException(nameof(client));
            _tableName = tableName;
        }

        public async Task<ShardStateDto> GetAsync(string app, string streamArn, string shardId, CancellationToken cancellation)
        {
            var response = await _client.GetItemAsync(
                new GetItemRequest()
                {
                    TableName = _tableName,
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { "id", new AttributeValue(ComputeId(app, streamArn, shardId)) },
                    },
                },
                cancellation);

            var item = response.Item;
            if (item == null || item.Count == 0)
            {
                return null;
            }

            return new ShardStateDto
            {
                App = app,
                StreamArn = streamArn,
                ShardId = shardId,
                NextIterator = item.TryGetValue("nextIterator", out var nextIterator) ? nextIterator.S : null,
                LastSequenceNumber = item.TryGetValue("lastSequence", out var lastSequenceNumber) ? lastSequenceNumber.S : null,
                WriteTime = item.TryGetValue("writeTime", out var writeTime) ? DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(writeTime.N)) : DateTimeOffset.MinValue,
            };
        }

        public async Task PutAsync(ShardStateDto state, CancellationToken cancellation)
        {
            _ = state ?? throw new ArgumentNullException(nameof(state));

            var item = new Dictionary<string, AttributeValue>
            {
                { "id", new AttributeValue(ComputeId(state.App, state.StreamArn, state.ShardId)) },
                { "writeTime", new AttributeValue { N = state.WriteTime.ToUnixTimeMilliseconds().ToString() } },
            };

            if (!string.IsNullOrEmpty(state.NextIterator))
            {
                item.Add("nextIterator", new AttributeValue(state.NextIterator));
            }

            if (!string.IsNullOrEmpty(state.LastSequenceNumber))
            {
                item.Add("lastSequence", new AttributeValue(state.LastSequenceNumber));
            }

            await _client.PutItemAsync(
                new PutItemRequest()
                {
                    TableName = _tableName,
                    Item = item,
                },
                cancellation);
        }

        private static string ComputeId(string app, string streamArn, string shardId) => $"{Uri.EscapeDataString(app)}/{Uri.EscapeDataString(streamArn)}/{Uri.EscapeDataString(shardId)}";
    }
}
