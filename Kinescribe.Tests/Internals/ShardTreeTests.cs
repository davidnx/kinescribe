using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using FluentAssertions;
using Kinescribe.Internals;
using Xunit;

namespace Kinescribe.Internal.Tests
{
    public class ShardTreeTests
    {
        [Fact]
        public void Empty_Works()
        {
            // Arrange
            var shards = new List<Shard>();

            // Act
            var tree = new ShardTree(shards);

            // Assert
            tree.Roots.Length.Should().Be(0);
        }

        [Fact]
        public void SingleShard_Works()
        {
            // Arrange
            var shards = new List<Shard>
            {
                new Shard
                {
                    ShardId = "1",
                }
            };

            // Act
            var tree = new ShardTree(shards);

            // Assert
            tree.Roots.Length.Should().Be(1);
            tree.Roots[0].Shard.Should().BeSameAs(shards[0]);
            tree.Roots[0].Parent.Should().BeNull();
            tree.Roots[0].Children.Should().BeEmpty();
        }

        [Fact]
        public void SingleShard_MissingParentId_Works()
        {
            // Arrange
            var shards = new List<Shard>
            {
                new Shard
                {
                    ShardId = "1",
                    ParentShardId = "does not exist",
                }
            };

            // Act
            var tree = new ShardTree(shards);

            // Assert
            tree.Roots.Length.Should().Be(1);
            tree.Roots[0].Shard.Should().BeSameAs(shards[0]);
            tree.Roots[0].Parent.Should().BeNull();
            tree.Roots[0].Children.Should().BeEmpty();
        }

        [Fact]
        public void SingleChain_Works()
        {
            // Arrange
            // shard1 --> shard2 --> shard3
            var shard1 = new Shard
            {
                ShardId = "1",
                ParentShardId = null,
            };
            var shard2 = new Shard
            {
                ShardId = "2",
                ParentShardId = "1",
            };
            var shard3 = new Shard
            {
                ShardId = "3",
                ParentShardId = "2",
            };
            var shards = new List<Shard> { shard2, shard3, shard1 };

            // Act
            var tree = new ShardTree(shards);

            // Assert
            tree.Roots.Length.Should().Be(1);
            var node1 = tree.Roots[0];
            node1.Shard.Should().BeSameAs(shard1);
            node1.Parent.Should().BeNull();
            node1.Children.Length.Should().Be(1);

            var node2 = node1.Children[0];
            node2.Shard.Should().Be(shard2);
            node2.Parent.Should().BeSameAs(node1);
            node2.Children.Length.Should().Be(1);

            var node3 = node2.Children[0];
            node3.Shard.Should().Be(shard3);
            node3.Parent.Should().BeSameAs(node2);
            node3.Children.Should().BeEmpty();
        }

        [Fact]
        public void SplitChain_Works()
        {
            // Arrange
            //  - shard1 --> shard2
            //
            //  - shard3 --> shard4a
            //          \
            //           \--> shard4b
            //
            var shard1 = new Shard
            {
                ShardId = "1",
                ParentShardId = null,
            };
            var shard2 = new Shard
            {
                ShardId = "2",
                ParentShardId = "1",
            };
            var shard3 = new Shard
            {
                ShardId = "3",
                ParentShardId = null,
            };
            var shard4a = new Shard
            {
                ShardId = "4a",
                ParentShardId = "3",
            };
            var shard4b = new Shard
            {
                ShardId = "4b",
                ParentShardId = "3",
            };
            var shards = new List<Shard> { shard2, shard3, shard1, shard4b, shard4a };

            // Act
            var tree = new ShardTree(shards);

            // Assert
            tree.Roots.Length.Should().Be(2);
            var node1 = tree.Roots[0];
            node1.Shard.Should().BeSameAs(shard1);
            node1.Parent.Should().BeNull();
            node1.Children.Length.Should().Be(1);

            var node2 = node1.Children[0];
            node2.Shard.Should().Be(shard2);
            node2.Parent.Should().BeSameAs(node1);
            node2.Children.Should().BeEmpty();

            var node3 = tree.Roots[1];
            node3.Shard.Should().BeSameAs(shard3);
            node3.Parent.Should().BeNull();
            node3.Children.Length.Should().Be(2);

            var node4a = node3.Children[0];
            node4a.Shard.Should().BeSameAs(shard4a);
            node4a.Parent.Should().BeSameAs(node3);
            node4a.Children.Should().BeEmpty();

            var node4b = node3.Children[1];
            node4b.Shard.Should().BeSameAs(shard4b);
            node4b.Parent.Should().BeSameAs(node3);
            node4b.Children.Should().BeEmpty();
        }
    }
}
