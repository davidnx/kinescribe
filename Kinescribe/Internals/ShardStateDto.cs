using System;

namespace Kinescribe.Internals
{
    public class ShardStateDto
    {
        public string App { get; set; }
        public string StreamArn { get; set; }
        public string ShardId { get; set; }
        public string NextIterator { get; set; }
        public string LastSequenceNumber { get; set; }
        public DateTimeOffset WriteTime { get; set; }

        public override string ToString()
        {
            return $"(App={App}, StreamArn={StreamArn}, ShardId={ShardId}, NextIterator={NextIterator}, LastSequenceNumber={LastSequenceNumber}, WriteTime={WriteTime:O})";
        }
    }
}
