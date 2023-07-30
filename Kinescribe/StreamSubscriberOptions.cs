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
        /// Extra time afforded for teardown operations after the cancellation token passed to <see cref="StreamSubscriber.ExecuteAsync"/> is signaled.
        /// This helps us attempt to leave things in a clean state (e.g. release the distributed lock, checkpoint current state, etc.).
        /// </summary>
        public TimeSpan TeardownTimeout { get; set;} = TimeSpan.FromSeconds(5);

        /// <summary>
        /// How long to wait before trying the same shard again after a failureoccurred when the subscribed action failed.
        /// The subscribed action will continue to be called periodically with approximately this interval until it succeeds.
        /// </summary>
        public TimeSpan RetryCallbackSnoozeTime { get; set; } = TimeSpan.FromSeconds(15);

        /// <summary>
        /// Works along with <see cref="MaxCheckpointLagRecords"/> to reduce the number of PUT calls to store stream iterators.
        /// <see cref="StreamSubscriber"/> offers "at-least-once" processing guarantees, and these options help tune the trade-off
        /// between (1) the likelihood of work duplications; vs. (2) the number of PUT calls to persist updated stream iterators.
        /// Higher numbers can help reduce cost of table operations to persist iterators.
        /// </summary>
        public TimeSpan MaxCheckpointLagInterval { get; set; } = TimeSpan.FromMinutes(2);

        /// <summary>
        /// Works along with <see cref="MaxCheckpointLagInterval"/> to reduce the number of PUT calls to store stream iterators.
        /// <see cref="StreamSubscriber"/> offers "at-least-once" processing guarantees, and these options help tune the trade-off
        /// between (1) the likelihood of work duplications; vs. (2) the number of PUT calls to persist updated stream iterators.
        /// Higher numbers can help reduce cost of table operations to persist iterators.
        /// </summary>
        public int MaxCheckpointLagRecords { get; set; } = 1;

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

            if (LockAcquisitionInterval <= TimeSpan.Zero)
            {
                throw new InvalidOperationException($"{nameof(LockAcquisitionInterval)} must be positive");
            }

            if (SnoozeTime <= TimeSpan.Zero)
            {
                throw new InvalidOperationException($"{nameof(SnoozeTime)} must be positive");
            }

            if (TeardownTimeout <= TimeSpan.Zero)
            {
                throw new InvalidOperationException($"{nameof(TeardownTimeout)} must be positive");
            }

            if (ShardTableName == LockOptions.TableName)
            {
                throw new InvalidOperationException($"Shards and Locks table should not be the same");
            }

            if (RetryCallbackSnoozeTime < SnoozeTime)
            {
                throw new InvalidOperationException($"{nameof(RetryCallbackSnoozeTime)} must be greater than or equal to {nameof(SnoozeTime)} ({SnoozeTime}), found {RetryCallbackSnoozeTime}");
            }

            if (MaxCheckpointLagInterval < TimeSpan.Zero)
            {
                throw new InvalidOperationException($"{nameof(MaxCheckpointLagInterval)} must be non-negative");
            }

            if (MaxCheckpointLagRecords < 0)
            {
                throw new InvalidOperationException($"{nameof(MaxCheckpointLagRecords)} must be non-negative");
            }
        }
    }

    public enum CursorTableBillingMode
    {
        PayPerRequest,
        MinimalProvisionedThroughput,
    }
}
