using Aliyun.OTS.DataModel;
using System;
using System.Collections.Generic;

namespace Aliyun.OTS.Response
{
    public class ParallelScanResponse : OTSResponse
    {
        public List<Row> Rows { get; set; }

        public byte[] NextToken { get; set; }

        public long BodyBytes;
    }
}
