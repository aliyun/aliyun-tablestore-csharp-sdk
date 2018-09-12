using System;
namespace Aliyun.OTS.DataModel
{
    public class StreamDetails
    {
        /// <summary>
        /// 该表是否打开stream
        /// </summary>
        public bool EnableStream { private set; get; }

        /// <summary>
        /// 该表的stream的id
        /// </summary>
        public string StreamId { set; get; }

        /// <summary>
        /// 该表的stream的过期时间
        /// </summary>
        public int ExpirationTime { set; get; }

        /// <summary>
        /// 该stream的打开的时间
        /// </summary>
        public Int64 LastEnableTime { set; get; }


        public StreamDetails(bool enableStream)
        {
            EnableStream = enableStream;
        }
    }
}
