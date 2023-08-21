using System;

namespace Kinescribe.Internals
{
    /// <summary>
    /// Represents a benign condition that requires <see cref="StreamSubscriber"/> to start over.
    /// A common example is when one of the shard processors reaches the end of a leaf node.
    /// At that point, with the current implementation, we have to start over and enumerate all nodes again.
    /// </summary>
    internal class BenignStartOverException : Exception
    {
        public BenignStartOverException(string message) : base(message)
        {
        }

        public BenignStartOverException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
