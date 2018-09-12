using System;
using System.Text;

namespace Aliyun.OTS.Util
{
    public static class OtsUtils
    {
        public static string DetermineSystemArchitecture()
        {
            return (IntPtr.Size == 8) ? "x86_64" : "x86";
        }

        public static string DetermineOsVersion()
        {
            try
            {
                var os = Environment.OSVersion;
                return "windows " + os.Version.Major + "." + os.Version.Minor;
            }
            catch (InvalidOperationException)
            {
                return "Unknown OSVersion";
            }
        }

        public static string FormatDateTimeStr(DateTime dt)
        {
            return dt.ToString("yyyy-MM-ddTHH:mm:ss.000Z");
        }

        /// <summary>
        /// 计算字符串的大小(按照UTF-8编码)
        /// </summary>
        /// <returns>返回字符串的字节数</returns>
        /// <param name="str">String.</param>
        public static int CalcStringSizeInBytes(string str)
        {
            return String2Bytes(str).Length;
        }

        public static byte[] String2Bytes(string str)
        {
            return Encoding.UTF8.GetBytes(str);
        }

        public static int CompareByteArrayInLexOrder(byte[] buffer1, int offset1, int length1,
                                             byte[] buffer2, int offset2, int length2)
        {
            // Short circuit equal case
            if (buffer1 == buffer2 &&
                    offset1 == offset2 &&
                    length1 == length2)
            {
                return 0;
            }
            // Bring WritableComparator code local
            int end1 = offset1 + length1;
            int end2 = offset2 + length2;
            for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++)
            {
                int a = (buffer1[i] & 0xff);
                int b = (buffer2[j] & 0xff);
                if (a != b)
                {
                    return a - b;
                }
            }
            return length1 - length2;
        }

        public static string Bytes2UTF8String(byte[] buffer)
        {
            return Encoding.UTF8.GetString(buffer);
        }
    }
}
