using System;
namespace com.alicloud.openservices.tablestore.core.protocol
{
    public class PlainBufferSequenceInfo
    {
        private uint epoch = 0;
        private long timestamp = 0;
        private uint rowIndex = 0;
        private bool hasSeq = false;

        public uint GetEpoch()
        {
            return epoch;
        }

        public void SetEpoch(uint epoch)
        {
            this.epoch = epoch;
            this.hasSeq = true;
        }
        public long GetTimestamp()
        {
            return timestamp;
        }

        public void SetTimestamp(long timestamp)
        {
            this.timestamp = timestamp;
            this.hasSeq = true;
        }

        public uint GetRowIndex()
        {
            return rowIndex;
        }

        public void SetRowIndex(uint rowIndex)
        {
            this.rowIndex = rowIndex;
            this.hasSeq = true;
        }

        public bool GetHasSeq()
        {
            return hasSeq;
        }

        public override String ToString()
        {
            return "Epoch: " + epoch + ", Timestamp: " + timestamp + ", RowIndex: " + rowIndex;
        }
    }
}
