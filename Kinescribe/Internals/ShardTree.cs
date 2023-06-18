using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Amazon.DynamoDBv2.Model;

namespace Kinescribe.Internals
{
    internal class ShardTree
    {
        public ShardTree(ICollection<Shard> shards)
        {
            var nodesById = shards.ToDictionary(shard => shard.ShardId, shard => new ShardNode(shard));
            var nodesByParentId = nodesById.Values
                .Where(node => node.Shard.ParentShardId != null)
                .GroupBy(node => node.Shard.ParentShardId)
                .ToDictionary(
                    group => group.Key,
                    group => group
                        .OrderBy(node => node.ShardId)
                        .ToArray());

            foreach (var node in nodesById.Values)
            {
                if (node.Shard.ParentShardId != null &&
                    nodesById.TryGetValue(node.Shard.ParentShardId, out var parent))
                {
                    node.Parent = parent;
                }

                if (nodesByParentId.TryGetValue(node.ShardId, out var children))
                {
                    node.Children = children;
                }
                else
                {
                    node.Children = Array.Empty<ShardNode>();
                }
            }

            Roots = nodesById.Values
                .Where(n => n.Parent == null)
                .OrderBy(n => n.ShardId)
                .ToArray();
            NumLeaves = nodesById.Count(n => n.Value.Children.Length == 0);
        }

        public ShardNode[] Roots { get; }
        public int NumLeaves { get; }

        public override string ToString()
        {
            var builder = new StringBuilder();
            foreach (var root in Roots)
            {
                Append(builder, 0, root);
            }

            return builder.ToString();

            static void Append(StringBuilder builder, int indent, ShardNode node)
            {
                builder.Append(' ', indent);
                builder.AppendLine(node.ShardId);

                foreach (var child in node.Children)
                {
                    Append(builder, indent + 2, child);
                }
            }
        }
    }
}
