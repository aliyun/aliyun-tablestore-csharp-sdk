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

using Aliyun.OTS.Handler;

namespace Aliyun.OTS.Retry
{
    public abstract class RetryPolicy
    {
        public static RetryPolicy DefaultRetryPolicy = new DefaultRetryPolicy();
        public static RetryPolicy RetryPolicyNoDelay = new RetryPolicyNoDelay();
        public static RetryPolicy NoRetryPolicy = new NoRetryPolicy();
        
        public abstract bool MaxRetryTimeReached(Context context, OTSException exception);
        public abstract bool CanRetry(Context context, OTSException exception);
        public abstract int DelayBeforeNextRetry(Context context, OTSException exception);
    }
}
