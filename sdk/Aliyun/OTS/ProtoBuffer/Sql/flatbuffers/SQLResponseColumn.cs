// <auto-generated>
//  automatically generated by the FlatBuffers compiler, do not modify
// </auto-generated>

namespace com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers
{

    using global::System;
    using Google.FlatBuffers;

    public struct SQLResponseColumn : IFlatbufferObject
    {
        private Table __p;
        public ByteBuffer ByteBuffer { get { return __p.bb; } }
        public static void ValidateVersion() { FlatBufferConstants.FLATBUFFERS_1_12_0(); }
        public static SQLResponseColumn GetRootAsSQLResponseColumn(ByteBuffer _bb) { return GetRootAsSQLResponseColumn(_bb, new SQLResponseColumn()); }
        public static SQLResponseColumn GetRootAsSQLResponseColumn(ByteBuffer _bb, SQLResponseColumn obj) { return (obj.__assign(_bb.GetInt(_bb.Position) + _bb.Position, _bb)); }
        public void __init(int _i, ByteBuffer _bb) { __p = new Table(_i, _bb); }
        public SQLResponseColumn __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

        public string ColumnName { get { int o = __p.__offset(4); return o != 0 ? __p.__string(o + __p.bb_pos) : null; } }
#if ENABLE_SPAN_T
  public Span<byte> GetColumnNameBytes() { return __p.__vector_as_span<byte>(4, 1); }
#else
        public ArraySegment<byte>? GetColumnNameBytes() { return __p.__vector_as_arraysegment(4); }
#endif
        public byte[] GetColumnNameArray() { return __p.__vector_as_array<byte>(4); }
        public com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.DataType ColumnType { get { int o = __p.__offset(6); return o != 0 ? (com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.DataType)__p.bb.GetSbyte(o + __p.bb_pos) : com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.DataType.NONE; } }
        public com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.ColumnValues? ColumnValue { get { int o = __p.__offset(8); return o != 0 ? (com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.ColumnValues?)(new com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.ColumnValues()).__assign(__p.__indirect(o + __p.bb_pos), __p.bb) : null; } }

        public static Offset<com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.SQLResponseColumn> CreateSQLResponseColumn(FlatBufferBuilder builder,
            StringOffset column_nameOffset = default(StringOffset),
            com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.DataType column_type = com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.DataType.NONE,
            Offset<com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.ColumnValues> column_valueOffset = default(Offset<com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.ColumnValues>))
        {
            builder.StartTable(3);
            SQLResponseColumn.AddColumnValue(builder, column_valueOffset);
            SQLResponseColumn.AddColumnName(builder, column_nameOffset);
            SQLResponseColumn.AddColumnType(builder, column_type);
            return SQLResponseColumn.EndSQLResponseColumn(builder);
        }

        public static void StartSQLResponseColumn(FlatBufferBuilder builder) { builder.StartTable(3); }
        public static void AddColumnName(FlatBufferBuilder builder, StringOffset columnNameOffset) { builder.AddOffset(0, columnNameOffset.Value, 0); }
        public static void AddColumnType(FlatBufferBuilder builder, com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.DataType columnType) { builder.AddSbyte(1, (sbyte)columnType, 0); }
        public static void AddColumnValue(FlatBufferBuilder builder, Offset<com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.ColumnValues> columnValueOffset) { builder.AddOffset(2, columnValueOffset.Value, 0); }
        public static Offset<com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.SQLResponseColumn> EndSQLResponseColumn(FlatBufferBuilder builder)
        {
            int o = builder.EndTable();
            return new Offset<com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.SQLResponseColumn>(o);
        }
    };
}