using System;
using DynamoLock;

namespace Kinescribe
{
    public class StreamSubscriberOptions
    {
        public DynamoDbLockOptions LockOptions { get; set; } = new DynamoDbLockOptions
        {
            TableName = "kinescribe_locks",
        };

        public string ShardTableName { get; set; } = "kinescribe_shards";

        // How frequently we should try to acquire the global distribute lock. Each instance tries to acquire the lock this often...
        public TimeSpan LockAcquisitionInterval { get; set; } = TimeSpan.FromSeconds(5);

        public TimeSpan SnoozeTime { get; set; } = TimeSpan.FromSeconds(3);

        /// <summary>
        /// How long to wait before trying the same shard again after a failureoccurred when the subscribed action failed.
        /// The subscribed action will continue to be called periodically with approximately this interval until it succeeds.
        /// </summary>
        public TimeSpan RetryCallbackSnoozeTime { get; set; } = TimeSpan.FromSeconds(15);

        public CursorTableBillingMode TableBillingMode { get; set; } = CursorTableBillingMode.PayPerRequest;

        public void Validate()
        {
            if (LockOptions == null)
            {
                throw new InvalidOperationException($"{nameof(LockOptions)} must be non-null");
            }

            LockOptions.Validate();

            if (string.IsNullOrEmpty(ShardTableName))
            {
                throw new InvalidOperationException($"{nameof(ShardTableName)} must be non-empty");
            }

            if (SnoozeTime <= TimeSpan.Zero)
            {
                throw new InvalidOperationException($"{nameof(SnoozeTime)} must be positive");
            }

            if (ShardTableName == LockOptions.TableName)
            {
                throw new InvalidOperationException($"Shards and Locks table should not be the same");
            }

            if (RetryCallbackSnoozeTime < SnoozeTime)
            {
                throw new InvalidOperationException($"{nameof(RetryCallbackSnoozeTime)} must be greater than or equal to {nameof(SnoozeTime)} ({SnoozeTime}), found {RetryCallbackSnoozeTime}");
            }
        }
    }

    public enum CursorTableBillingMode
    {
        PayPerRequest,
        MinimalProvisionedThroughput,
    }
}
