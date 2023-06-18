using System.Threading;
using System.Threading.Tasks;
using Kinescribe.Internals;

namespace Kinescribe
{
    public interface IShardStateClient
    {
        Task<ShardStateDto> GetAsync(string app, string streamArn, string shardId, CancellationToken cancellation);
        Task PutAsync(ShardStateDto item, CancellationToken cancellation);
    }
}