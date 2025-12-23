using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBStreams.Model;

namespace Kinescribe
{
    public interface IStreamSubscriber
    {
        Task ExecuteAsync(string appName, string streamArn, Func<Record, CancellationToken, Task> action, CancellationToken cancellation, int batchSize = 100);
    }
}