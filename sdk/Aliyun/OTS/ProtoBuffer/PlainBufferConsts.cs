namespace com.alicloud.openservices.tablestore.core.protocol
{
    public static class PlainBufferConsts
    {
        public const int HEADER = 0x75;

        // tag type
        public const byte TAG_ROW_PK = 0x1;
        public const byte TAG_ROW_DATA = 0x2;
        public const byte TAG_CELL = 0x3;
        public const byte TAG_CELL_NAME = 0x4;
        public const byte TAG_CELL_VALUE = 0x5;
        public const byte TAG_CELL_TYPE = 0x6;
        public const byte TAG_CELL_TIMESTAMP = 0x7;
        public const byte TAG_DELETE_ROW_MARKER = 0x8;
        public const byte TAG_ROW_CHECKSUM = 0x9;
        public const byte TAG_CELL_CHECKSUM = 0x0A;
        public const byte TAG_EXTENSION = 0x0B;
        public const byte TAG_SEQ_INFO = 0x0C;
        public const byte TAG_SEQ_INFO_EPOCH = 0x0D;
        public const byte TAG_SEQ_INFO_TS = 0x0E;
        public const byte TAG_SEQ_INFO_ROW_INDEX = 0x0F;

        // cell op type
        public const byte DELETE_ALL_VERSION = 0x1;
        public const byte DELETE_ONE_VERSION = 0x3;

        // variant type
        public const byte VT_INTEGER = 0x0;
        public const byte VT_DOUBLE = 0x1;
        public const byte VT_BOOLEAN = 0x2;
        public const byte VT_STRING = 0x3;
        //public const byte VT_NULL = 0x6;
        public const byte VT_BLOB = 0x7;
        public const byte VT_INF_MIN = 0x9;
        public const byte VT_INF_MAX = 0xa;
        public const byte VT_AUTO_INCREMENT = 0xb;

        public static string PrintTag(uint tag)
        {
            switch (tag)
            {
                case TAG_ROW_PK:
                    return "TAG_ROW_PK";
                case TAG_ROW_DATA:
                    return "TAG_ROW_DATA";
                case TAG_CELL:
                    return "TAG_CELL";
                case TAG_CELL_NAME:
                    return "TAG_CELL_NAME";
                case TAG_CELL_VALUE:
                    return "TAG_CELL_VALUE";
                case TAG_CELL_TYPE:
                    return "TAG_CELL_TYPE";
                case TAG_CELL_TIMESTAMP:
                    return "TAG_CELL_TIMESTAMP";
                case TAG_DELETE_ROW_MARKER:
                    return "TAG_DELETE_ROW_MARKER";
                case TAG_ROW_CHECKSUM:
                    return "TAG_ROW_CHECKSUM";
                case TAG_CELL_CHECKSUM:
                    return "TAG_CELL_CHECKSUM";
                case TAG_SEQ_INFO:
                    return "TAG_SEQ_INFO";
                case TAG_SEQ_INFO_EPOCH:
                    return "TAG_SEQ_INFO_EPOCH";
                case TAG_SEQ_INFO_TS:
                    return "TAG_SEQ_INFO_TS";
                case TAG_SEQ_INFO_ROW_INDEX:
                    return "TAG_SEQ_INFO_ROW_INDEX";
                case TAG_EXTENSION:
                    return "TAG_EXTENSION";
                default:
                    return "UNKNOWN_TAG(" + tag + ")";
            }
        }

        public static bool IsUnknownTag(uint tag)
        {
            switch (tag)
            {
                case TAG_ROW_PK:
                case TAG_ROW_DATA:
                case TAG_CELL:
                case TAG_CELL_NAME:
                case TAG_CELL_VALUE:
                case TAG_CELL_TYPE:
                case TAG_CELL_TIMESTAMP:
                case TAG_DELETE_ROW_MARKER:
                case TAG_ROW_CHECKSUM:
                case TAG_CELL_CHECKSUM:
                case TAG_SEQ_INFO:
                case TAG_SEQ_INFO_EPOCH:
                case TAG_SEQ_INFO_TS:
                case TAG_SEQ_INFO_ROW_INDEX:
                case TAG_EXTENSION:
                    return false;
                default:
                    return true;
            }
        }

        public static bool IsTagInExtension(uint tag)
        {
           return tag == TAG_SEQ_INFO || IsUnknownTag(tag);
        }
    }
}
