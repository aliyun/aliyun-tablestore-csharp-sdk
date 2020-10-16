using System;
using System.Collections;
namespace Aliyun.OTS.DataModel
{
    public class NameTimestampComparator: IComparer
    {
        public int Compare(Object obj1, Object obj2)
        {
            var c1 = obj1 as Column;
            var c2 = obj2 as Column;
            int ret = string.Compare(c1.Name, c2.Name, StringComparison.Ordinal);

            if (ret != 0)
            {
                return ret;
            }

            long t1 = c1.Timestamp.Value;
            long t2 = c2.Timestamp.Value;
            return t1 == t2 ? 0 : (t1 < t2 ? 1 : -1);
        }
    }
}
