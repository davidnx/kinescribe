using System;
using Amazon.DynamoDBv2.Model;

namespace Kinescribe.Internals
{
    internal class ShardNode
    {
        public ShardNode(Shard shard)
        {
            Shard = shard ?? throw new ArgumentNullException(nameof(shard));
        }

        public Shard Shard { get; }
        public ShardNode Parent { get; set; }
        public ShardNode[] Children { get; set; }

        public string ShardId => Shard.ShardId;
    }
}
