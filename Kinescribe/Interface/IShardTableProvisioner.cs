using System.Threading;
using System.Threading.Tasks;

namespace Kinescribe
{
    public interface IShardTableProvisioner
    {
        Task ProvisionAsync(CancellationToken cancellation);
    }
}