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

using System;

namespace Aliyun.OTS
{
    /// <summary>
    /// OTS错误类型的基类，它有两个子类<see cref="OTSClientException"/>和<see cref="OTSServerException"/>。
    /// </summary>
    public class OTSException : Exception
    {
        public OTSException() { }

        public OTSException(string message)
            : base(message)
        {
        }
    }
}
