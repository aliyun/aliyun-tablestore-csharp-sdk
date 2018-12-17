using System;
using System.Collections.Generic;

namespace Aliyun.OTS.DataModel
{
    public class DefinedColumnSchema : List<Tuple<string, DefinedColumnType>>
    {
        /// <summary>
        /// 添加一个预定义列的设计
        /// </summary>
        /// <param name="name"></param>
        /// <param name="type"></param>
        public void Add(string name, DefinedColumnType type)
        {
            var tuple = new Tuple<string, DefinedColumnType>(name, type);

            Add(tuple);
        }
    }
}
