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


namespace Aliyun.OTS.Handler
{
    public abstract class PipelineHandler
    {
        protected PipelineHandler InnerHandler { get; set; }

        protected PipelineHandler()
        {
            InnerHandler = null;
        }

        protected PipelineHandler(PipelineHandler innerHandler)
        {
            InnerHandler = innerHandler;
        }

        public abstract void HandleBefore(Context context);
        public abstract void HandleAfter(Context context);
    }
}
