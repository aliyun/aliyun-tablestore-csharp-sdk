/*
 *  Trade secret of Alibaba Group R&D.
 *  Copyright (c) 2015 Alibaba Group R&D. 
 *
 *  All rights reserved.  This notice is intended as a precaution against
 *  inadvertent publication and does not imply publication or any waiver
 *  of confidentiality.  The year included in the foregoing notice is the
 *  year of creation of the work.
 *
 */

using System.Collections.Generic;

namespace Aliyun.OTS.Response
{
    /// <summary>
    /// 表示ListTable的返回。
    /// </summary>
    public class ListTableResponse : OTSResponse
    {
        /// <summary>
        /// ListTable返回的表名
        /// </summary>
        public IList<string> TableNames {get; set;}
        public ListTableResponse() {}
    }
}
