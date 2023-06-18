using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;

namespace Kinescribe
{
    public interface IStreamSubscriber
    {
        Task ExecuteAsync(string appName, string streamArn, Func<Record, CancellationToken, Task> action, CancellationToken cancellation, int batchSize = 100);
    }
}