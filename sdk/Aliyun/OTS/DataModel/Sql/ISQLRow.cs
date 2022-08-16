using System.IO;

namespace Aliyun.OTS.DataModel.SQL
{
    /// <summary>
    /// 表示SQL行的数据结构
    /// </summary>
    public interface ISQLRow
    {
        /// <summary>
        /// 根据列游标获取数据对象
        /// </summary>
        /// <param name="columnIndex"></param>
        /// <returns><see cref="object"/></returns>
        object Get(int columnIndex);
        /// <summary>
        /// 根据列名获取数据对象
        /// </summary>
        /// <param name="name"></param>
        /// <returns><see cref="object"/></returns>
        object Get(string name);
        /// <summary>
        /// 根据列游标获取字符串类型的值。
        /// <p>当且仅当数据类型为<see cref="ColumnValueType.String"/>才可获取值</p>
        /// </summary>
        /// <param name="columnIndex"></param>
        /// <returns><see cref="string"/></returns>
        string GetString(int columnIndex);
        /// <summary>
        /// 根据列名获取字符串类型的值。
        /// <p>当且仅当数据类型为<see cref="ColumnValueType.String"/>才可获取值</p>
        /// </summary>
        /// <param name="name"></param>
        /// <returns><see cref="string"/></returns>
        string GetString(string name);
        /// <summary>
        /// 根据列游标获取整型数值。
        /// <p>当且仅当数据类型为<see cref="ColumnValueType.Integer"/>才可获取值</p>
        /// </summary>
        /// <param name="columnIndex"></param>
        /// <returns><see cref="long"/></returns>
        long? GetLong(int columnIndex);
        /// <summary>
        /// 根据列名获取整型数值。
        /// <p>当且仅当数据类型为<see cref="ColumnValueType.Integer"/>才可获取值</p>
        /// </summary>
        /// <param name="name"></param>
        /// <returns><see cref="long"/></returns>
        long? GetLong(string name);
        /// <summary>
        /// 根据列游标获取布尔类型值。
        /// <p>当且仅当数据类型为<see cref="ColumnValueType.Boolean"/>才可获取值</p>
        /// </summary>
        /// <param name="columnIndex"></param>
        /// <returns><see cref="bool"/></returns>
        bool? GetBoolean(int columnIndex);
        /// <summary>
        /// 根据列名获取布尔类型值。
        /// <p>当且仅当数据类型为<see cref="ColumnValueType.Boolean"/>才可获取值</p>
        /// </summary>
        /// <param name="name"></param>
        /// <returns><see cref="bool"/></returns>
        bool? GetBoolean(string name);
        /// <summary>
        /// 根据列游标获取浮点数类型值。
        /// <p>当且仅当数据类型为<see cref="ColumnValueType.Double"/>才可获取值</p>
        /// </summary>
        /// <param name="columnIndex"></param>
        /// <returns><see cref="double"/></returns>
        double? GetDouble(int columnIndex);
        /// <summary>
        /// 根据列名获取浮点数类型值。
        /// <p>当且仅当数据类型为<see cref="ColumnValueType.Double"/>才可获取值</p>
        /// </summary>
        /// <param name="name"></param>
        /// <returns><see cref="double"/></returns>
        double? GetDouble(string name);
        /// <summary>
        /// 根据列游标获取Binary类型值。
        /// <p>当且仅当数据类型为<see cref="ColumnValueType.Binary"/>才可获取值</p>
        /// </summary>
        /// <param name="columnIndex"></param>
        /// <returns><see cref="MemoryStream"/></returns>
        MemoryStream GetBinary(int columnIndex);
        /// <summary>
        /// 根据列名获取Binary类型值。
        /// <p>当且仅当数据类型为<see cref="ColumnValueType.Binary"/>才可获取值</p>
        /// </summary>
        /// <param name="name"></param>
        /// <returns><see cref="MemoryStream"/></returns>
        MemoryStream GetBinary(string name);
        /// <summary>
        /// 建议只用于Debug和测试使用。
        /// 格式化输出该行数据，按照"columnA: valueA, columnB: valueB"的格式
        /// </summary>
        /// <returns><see cref="string"/></returns>
        string ToDebugString();
    }
}
