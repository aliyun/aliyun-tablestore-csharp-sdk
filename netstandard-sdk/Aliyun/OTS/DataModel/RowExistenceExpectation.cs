namespace Aliyun.OTS.DataModel
{
    public enum RowExistenceExpectation
    {
        /// <summary>
        /// 表示写操作（包括PutRow, DeleteRow, UpdateRow, BatchWriteRow）中的IGNORE条件。
        /// 即不论该行是否存在均执行操作。
        /// </summary>
        IGNORE,

        /// <summary>
        /// 表示写操作（包括PutRow, DeleteRow, UpdateRow, BatchWriteRow）EXPECT_EXIST条件。
        /// 即仅在该行存在的情况下执行操作；否则操作出错。
        /// </summary>
        EXPECT_EXIST,

        /// <summary>
        /// 表示写操作PutRow和BatchWriteRow中的put操作的EXPECT_NOT_EXIST条件。
        /// 即仅在该行不存在的情况下执行操作；否则操作出错。
        /// </summary>
        EXPECT_NOT_EXIST
    }
}
