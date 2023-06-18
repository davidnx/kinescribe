using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoLock;
using FakeItEasy;
using FluentAssertions;
using Kinescribe.Internals;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace Kinescribe.Tests
{
    public class StreamSubscriberTests
    {
        private const string TestTableName = "table1";
        private const string TestStreamArn = "arn:aws:dynamodb:xx-xxxx-x:xxxxxxxxxxxx:table/table1/stream/2023-06-15T12:01:02.345";

        private StreamSubscriber _subject;
        private IShardStateClient _tracker;
        private IDistributedLockManager _lockManager;
        private IAmazonDynamoDB _dynamoClient;
        private IAmazonDynamoDBStreams _streamsClient;
        private TimeSpan _waitTime = TimeSpan.FromSeconds(5);

        public StreamSubscriberTests()
        {
            _dynamoClient = A.Fake<IAmazonDynamoDB>();
            A.CallTo(() => _dynamoClient.DescribeTableAsync(TestTableName, A<CancellationToken>.Ignored))
                .Returns(new DescribeTableResponse
                {
                    Table = new TableDescription
                    {
                        StreamSpecification = new StreamSpecification { StreamEnabled = true, },
                        LatestStreamArn = TestStreamArn,
                    },
                });


            _streamsClient = A.Fake<IAmazonDynamoDBStreams>();
            _tracker = A.Fake<IShardStateClient>();

            _lockManager = A.Fake<IDistributedLockManager>();
            A.CallTo(() => _lockManager.ExecuteAsync(A<CancellationToken>.Ignored))
                .ReturnsLazily((CancellationToken cancellation) =>
                {
                    var tcs = new TaskCompletionSource();
                    cancellation.Register(() => tcs.TrySetResult());
                    return tcs.Task;
                });

            var options = Options.Create(new StreamSubscriberOptions());
            var provisioner = A.Fake<IShardTableProvisioner>();
            A.CallTo(() => provisioner.ProvisionAsync(A<CancellationToken>.Ignored))
                .Returns(Task.CompletedTask);
            _subject = new StreamSubscriber(_dynamoClient, _streamsClient, options, NullLoggerFactory.Instance, provisioner, _tracker, _lockManager);
        }

        [Fact]
        public async Task should_get_records()
        {
            //arrange
            var iterator = Guid.NewGuid().ToString();
            var nextIterator = Guid.NewGuid().ToString();
            var receivedSequenceNumber = string.Empty;
            var shardId = Guid.NewGuid().ToString();
            var record = new Amazon.DynamoDBv2.Model.Record
            {
                Dynamodb = new StreamRecord
                {
                    SequenceNumber = Guid.NewGuid().ToString(),
                },
            };

            A.CallTo(() => _streamsClient.DescribeStreamAsync(A<DescribeStreamRequest>.That.Matches(x => x.StreamArn == TestStreamArn), A<CancellationToken>.Ignored))
                .Returns(new DescribeStreamResponse()
                {
                    StreamDescription = new StreamDescription
                    {
                        Shards = new List<Shard>()
                        {
                            new Shard()
                            {
                                ShardId = shardId
                            }
                        }
                    },
                });

            A.CallTo(() => _streamsClient.GetRecordsAsync(A<GetRecordsRequest>.That.Matches(x => x.ShardIterator == iterator), A<CancellationToken>.Ignored))
                .Returns(new GetRecordsResponse()
                {
                    NextShardIterator = nextIterator,
                    Records = new List<Amazon.DynamoDBv2.Model.Record>()
                    {
                        record
                    }
                });

            A.CallTo(() => _tracker.GetAsync("app", TestStreamArn, shardId, A<CancellationToken>.Ignored))
                .ReturnsNextFromSequence(
                    new ShardStateDto
                    {
                        NextIterator = iterator,
                    },
                    new ShardStateDto
                    {
                        NextIterator = nextIterator,
                    });

            var releasedLock = false;
            A.CallTo(() => _lockManager.AcquireLockAsync(A<string>.Ignored, A<CancellationToken>.Ignored))
                .Returns(DistributedLockAcquisition.CreateAcquired(new CancellationTokenSource(), cancellation => { releasedLock = true; return Task.CompletedTask; }));

            //act
            var cts = new CancellationTokenSource(_waitTime);
            await _subject.ExecuteAsync("app", TestTableName, (x, _) => { receivedSequenceNumber = x.Dynamodb.SequenceNumber; return Task.CompletedTask; }, cts.Token);

            //assert
            A.CallTo(() => _streamsClient.GetRecordsAsync(A<GetRecordsRequest>.That.Matches(x => x.ShardIterator == iterator), A<CancellationToken>.Ignored))
                .MustHaveHappened();

            receivedSequenceNumber.Should().Be(record.Dynamodb.SequenceNumber);
            A.CallTo(() => _tracker.PutAsync(A<ShardStateDto>.That.Matches(s => s.App == "app" && s.StreamArn == TestStreamArn && s.ShardId == shardId && s.NextIterator == nextIterator && s.LastSequenceNumber == record.Dynamodb.SequenceNumber), A<CancellationToken>.Ignored)).MustHaveHappened();
            A.CallTo(() => _lockManager.AcquireLockAsync(A<string>.Ignored, A<CancellationToken>.Ignored)).MustHaveHappened();
            releasedLock.Should().BeTrue();
        }

        [Fact]
        public async Task should_update_shard_iterator()
        {
            //arrange
            var iterator = Guid.NewGuid().ToString();
            var nextIterator = Guid.NewGuid().ToString();
            var shardId = Guid.NewGuid().ToString();

            A.CallTo(() => _streamsClient.DescribeStreamAsync(A<DescribeStreamRequest>.That.Matches(x => x.StreamArn == TestStreamArn), A<CancellationToken>.Ignored))
                .Returns(new DescribeStreamResponse()
                {
                    StreamDescription = new StreamDescription
                    {
                        Shards = new List<Shard>()
                        {
                            new Shard()
                            {
                                ShardId = shardId
                            }
                        }
                    },
                });

            A.CallTo(() => _streamsClient.GetRecordsAsync(A<GetRecordsRequest>.That.Matches(x => x.ShardIterator == iterator), A<CancellationToken>.Ignored))
                .Returns(new GetRecordsResponse()
                {
                    NextShardIterator = nextIterator,
                    Records = new List<Amazon.DynamoDBv2.Model.Record>()
                });

            A.CallTo(() => _tracker.GetAsync("app", TestStreamArn, shardId, A<CancellationToken>.Ignored))
                .Returns(new ShardStateDto
                {
                    NextIterator = iterator,
                });

            A.CallTo(() => _lockManager.AcquireLockAsync(A<string>.Ignored, A<CancellationToken>.Ignored))
                .Returns(DistributedLockAcquisition.CreateAcquired(new CancellationTokenSource(), cancellation => Task.CompletedTask));

            //act
            var cts = new CancellationTokenSource(_waitTime);
            await _subject.ExecuteAsync("app", TestTableName, (x, _) => Task.CompletedTask, cts.Token);

            //assert
            A.CallTo(() => _tracker.PutAsync(A<ShardStateDto>.That.Matches(s => s.App == "app" && s.StreamArn == TestStreamArn && s.ShardId == shardId && s.NextIterator == nextIterator), A<CancellationToken>.Ignored)).MustHaveHappened();
        }

        [Fact]
        public async Task should_get_starting_shard_iterator_when_not_yet_tracked()
        {
            //arrange
            var startIterator = Guid.NewGuid().ToString();
            var nextIterator = Guid.NewGuid().ToString();
            var shardId = Guid.NewGuid().ToString();
            var startSeqNum = Guid.NewGuid().ToString();

            A.CallTo(() => _streamsClient.DescribeStreamAsync(A<DescribeStreamRequest>.That.Matches(x => x.StreamArn == TestStreamArn), A<CancellationToken>.Ignored))
                .Returns(new DescribeStreamResponse()
                {
                    StreamDescription = new StreamDescription
                    {
                        Shards = new List<Shard>()
                        {
                            new Shard()
                            {
                                ShardId = shardId,
                                SequenceNumberRange = new SequenceNumberRange()
                                {
                                    StartingSequenceNumber = startSeqNum
                                },
                            }
                        }
                    },
                });

            A.CallTo(() => _streamsClient.GetRecordsAsync(A<GetRecordsRequest>.Ignored, A<CancellationToken>.Ignored))
                .Returns(new GetRecordsResponse()
                {
                    NextShardIterator = nextIterator,
                    Records = new List<Amazon.DynamoDBv2.Model.Record>()
                });

            A.CallTo(() => _tracker.GetAsync("app", TestStreamArn, shardId, A<CancellationToken>.Ignored))
                .Returns((ShardStateDto)null);

            A.CallTo(() => _lockManager.AcquireLockAsync(A<string>.Ignored, A<CancellationToken>.Ignored))
                .Returns(DistributedLockAcquisition.CreateAcquired(new CancellationTokenSource(), cancellation => Task.CompletedTask));

            A.CallTo(() => _streamsClient.GetShardIteratorAsync(A<GetShardIteratorRequest>.That.Matches(x =>
                    x.ShardId == shardId &&
                    x.StreamArn == TestStreamArn &&
                    x.SequenceNumber == startSeqNum &&
                    x.ShardIteratorType == ShardIteratorType.AT_SEQUENCE_NUMBER
                ), A<CancellationToken>.Ignored))
                .Returns(new GetShardIteratorResponse()
                {
                    ShardIterator = startIterator
                });

            //act
            var cts = new CancellationTokenSource(_waitTime);
            await _subject.ExecuteAsync("app", TestTableName, (x, _) => Task.CompletedTask, cts.Token);

            //assert
            A.CallTo(() => _streamsClient.GetShardIteratorAsync(A<GetShardIteratorRequest>.That.Matches(x =>
                    x.ShardId == shardId &&
                    x.StreamArn == TestStreamArn &&
                    x.SequenceNumber == startSeqNum &&
                    x.ShardIteratorType == ShardIteratorType.AT_SEQUENCE_NUMBER
                ), A<CancellationToken>.Ignored))
            .MustHaveHappened();

            A.CallTo(() => _streamsClient.GetRecordsAsync(A<GetRecordsRequest>.That.Matches(x => x.ShardIterator == startIterator), A<CancellationToken>.Ignored))
                .MustHaveHappened();
        }

        [Fact]
        public async Task should_lock_shard_id()
        {
            //arrange
            var iterator = Guid.NewGuid().ToString();
            var nextIterator = Guid.NewGuid().ToString();
            var shardId = Guid.NewGuid().ToString();

            A.CallTo(() => _streamsClient.DescribeStreamAsync(A<DescribeStreamRequest>.That.Matches(x => x.StreamArn == TestStreamArn), A<CancellationToken>.Ignored))
                .Returns(new DescribeStreamResponse()
                {
                    StreamDescription = new StreamDescription
                    {
                        Shards = new List<Shard>()
                        {
                            new Shard()
                            {
                                ShardId = shardId
                            }
                        }
                    },
                });

            A.CallTo(() => _streamsClient.GetRecordsAsync(A<GetRecordsRequest>.That.Matches(x => x.ShardIterator == iterator), A<CancellationToken>.Ignored))
                .Returns(new GetRecordsResponse()
                {
                    NextShardIterator = nextIterator,
                    Records = new List<Amazon.DynamoDBv2.Model.Record>()
                });

            A.CallTo(() => _tracker.GetAsync("app", TestStreamArn, shardId, A<CancellationToken>.Ignored))
                .Returns(new ShardStateDto { NextIterator = iterator });

            var releasedLock = false;
            A.CallTo(() => _lockManager.AcquireLockAsync(A<string>.Ignored, A<CancellationToken>.Ignored))
                .Returns(DistributedLockAcquisition.CreateAcquired(new CancellationTokenSource(), cancellation => { releasedLock = true; return Task.CompletedTask; }));

            //act
            var cts = new CancellationTokenSource(_waitTime);
            await _subject.ExecuteAsync("app", TestTableName, (x, _) => Task.CompletedTask, cts.Token);

            //assert
            A.CallTo(() => _lockManager.AcquireLockAsync($"stream/{Uri.EscapeDataString(TestStreamArn)}/app/app/global", A<CancellationToken>.Ignored)).MustHaveHappenedOnceExactly();
            releasedLock.Should().BeTrue();
        }

        [Fact]
        public async Task should_get_new_iterator_when_expired()
        {
            //arrange
            var expiredIterator = "iterator1: expired";
            var newIterator = "iterator2: new";
            var nextIterator = "iterator3: next";
            var shardId = Guid.NewGuid().ToString();
            var startSeqNum = Guid.NewGuid().ToString();
            var lastSeqNum = Guid.NewGuid().ToString();

            A.CallTo(() => _streamsClient.DescribeStreamAsync(A<DescribeStreamRequest>.That.Matches(x => x.StreamArn == TestStreamArn), A<CancellationToken>.Ignored))
                .Returns(new DescribeStreamResponse()
                {
                    StreamDescription = new StreamDescription
                    {
                        Shards = new List<Shard>()
                        {
                            new Shard()
                            {
                                ShardId = shardId,
                                SequenceNumberRange = new SequenceNumberRange()
                                {
                                    StartingSequenceNumber = startSeqNum
                                },
                            }
                        }
                    },
                });

            A.CallTo(() => _streamsClient.GetRecordsAsync(A<GetRecordsRequest>.That.Matches(x =>
                    x.ShardIterator == expiredIterator), A<CancellationToken>.Ignored))
                .Throws(new ExpiredIteratorException(string.Empty));

            A.CallTo(() => _streamsClient.GetRecordsAsync(A<GetRecordsRequest>.That.Matches(x =>
                    x.ShardIterator == newIterator), A<CancellationToken>.Ignored))
                .Returns(new GetRecordsResponse()
                {
                    NextShardIterator = nextIterator,
                    Records = new List<Amazon.DynamoDBv2.Model.Record>()
                });

            A.CallTo(() => _tracker.GetAsync("app", TestStreamArn, shardId, A<CancellationToken>.Ignored))
                .ReturnsNextFromSequence(
                    new ShardStateDto { NextIterator = expiredIterator, LastSequenceNumber = lastSeqNum, },
                    new ShardStateDto { NextIterator = newIterator, }
                );

            A.CallTo(() => _lockManager.AcquireLockAsync(A<string>.Ignored, A<CancellationToken>.Ignored))
                .Returns(DistributedLockAcquisition.CreateAcquired(new CancellationTokenSource(), cancellation => Task.CompletedTask));

            A.CallTo(() => _streamsClient.GetShardIteratorAsync(A<GetShardIteratorRequest>.That.Matches(x =>
                    x.ShardId == shardId &&
                    x.StreamArn == TestStreamArn &&
                    x.SequenceNumber == lastSeqNum &&
                    x.ShardIteratorType == ShardIteratorType.AFTER_SEQUENCE_NUMBER
                ), A<CancellationToken>.Ignored))
                .Returns(new GetShardIteratorResponse()
                {
                    ShardIterator = newIterator
                });

            //act
            var cts = new CancellationTokenSource(_waitTime);
            await _subject.ExecuteAsync("app", TestTableName, (x, _) => Task.CompletedTask, cts.Token);

            //assert
            A.CallTo(() => _streamsClient.GetShardIteratorAsync(A<GetShardIteratorRequest>.That.Matches(x =>
                    x.ShardId == shardId &&
                    x.StreamArn == TestStreamArn &&
                    x.SequenceNumber == lastSeqNum &&
                    x.ShardIteratorType == ShardIteratorType.AFTER_SEQUENCE_NUMBER
                ), A<CancellationToken>.Ignored))
            .MustHaveHappened();

            A.CallTo(() => _streamsClient.GetRecordsAsync(A<GetRecordsRequest>.That.Matches(x => x.ShardIterator == newIterator), A<CancellationToken>.Ignored))
                .MustHaveHappened();
        }
    }
}
