using System.Collections.Generic;
using System.Threading;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using FakeItEasy;
using FluentAssertions;
using Kinescribe.Internals;
using Xunit;

namespace Kinescribe.Internals.Tests
{
#if false
    public class ShardStateClientTests
    {
        private ShardTracker _subject;
        private IAmazonDynamoDB _dynamoClient;
        private string _tableName = "locks";

        public ShardStateClientTests()
        {
            _dynamoClient = A.Fake<IAmazonDynamoDB>();
            _subject = new ShardTracker(_dynamoClient, _tableName);
        }

        [Fact]
        public async Task should_update_shard_iterator()
        {
            await _subject.IncrementShardIterator("app", "stream", "shard", "iter", CancellationToken.None);

            A.CallTo(() => _dynamoClient.UpdateItemAsync(A<UpdateItemRequest>.That.Matches(x =>
                    x.TableName == _tableName &&
                    x.Key["id"].S == "app/stream/shard" &&
                    x.UpdateExpression == "SET next_iterator = :n" &&
                    x.ExpressionAttributeValues[":n"].S == "iter"), A<CancellationToken>.Ignored))
                .MustHaveHappened();
        }

        [Fact]
        public async Task should_update_shard_iteratorand_sequence()
        {
            await _subject.IncrementShardIteratorAndSequence("app", "stream", "shard", "iter", "seq", CancellationToken.None);

            A.CallTo(() => _dynamoClient.PutItemAsync(A<PutItemRequest>.That.Matches(x =>
                    x.TableName == _tableName &&
                    x.Item["id"].S == "app/stream/shard" &&
                    x.Item["next_iterator"].S == "iter" &&
                    x.Item["last_sequence"].S == "seq"), A<CancellationToken>.Ignored))
                .MustHaveHappened();
        }

        [Fact]
        public async Task should_get_shard_iterator()
        {
            A.CallTo(() => _dynamoClient.GetItemAsync(A<GetItemRequest>.That.Matches(x =>
                    x.TableName == _tableName &&
                    x.Key["id"].S == "app/stream/shard"), A<CancellationToken>.Ignored))
                .Returns(new GetItemResponse()
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { "next_iterator", new AttributeValue("iter") }
                    }
                });

            var result = await _subject.GetNextShardIterator("app", "stream", "shard", CancellationToken.None);

            A.CallTo(() => _dynamoClient.GetItemAsync(A<GetItemRequest>.That.Matches(x =>
                    x.TableName == _tableName &&
                    x.Key["id"].S == "app/stream/shard"), A<CancellationToken>.Ignored))
                .MustHaveHappened();
            result.Should().Be("iter");
        }
    }
#endif
}
