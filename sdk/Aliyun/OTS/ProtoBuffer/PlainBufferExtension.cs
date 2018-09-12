using System;
namespace com.alicloud.openservices.tablestore.core.protocol
{
    public class PlainBufferExtension
    {
        private PlainBufferSequenceInfo sequenceInfo;


        public PlainBufferExtension()
        {
            this.sequenceInfo = new PlainBufferSequenceInfo();
        }
        public void setSequenceInfo(PlainBufferSequenceInfo sequenceInfo)
        {
            this.sequenceInfo = sequenceInfo;
        }

        public PlainBufferSequenceInfo GetSequenceInfo()
        {
            return sequenceInfo;
        }

        public bool HasSeq()
        {
            return sequenceInfo.GetHasSeq();
        }

        public override String ToString()
        {

            String str = "";
            if (HasSeq())
            {
                str += " SequenceInfo: {" + GetSequenceInfo() + "}";
            }

            return str;
        }
    }
}
