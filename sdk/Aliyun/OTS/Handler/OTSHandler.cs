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


using Aliyun.OTS.ProtoBuffer;

namespace Aliyun.OTS.Handler
{
    public class OTSHandler : PipelineHandler
    {
        public OTSHandler()
        {
            PipelineHandler inner;
            inner = new HttpHandler();
            inner = new HttpHeaderHandler(inner);
            inner = new ErrorHandler(inner);
            inner = new ProtocolBufferDecoder(inner);
            inner = new ProtocolBufferEncoder(inner);
            inner = new RetryHandler(inner);
            InnerHandler = inner;
        }


        public override void HandleBefore(Context context)
        {
            InnerHandler.HandleBefore(context);
        }

        public override void HandleAfter(Context context)
        {
            InnerHandler.HandleAfter(context);
        }
    }
}
