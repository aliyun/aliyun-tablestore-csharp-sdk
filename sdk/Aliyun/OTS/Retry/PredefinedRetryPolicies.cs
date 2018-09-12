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

using Aliyun.OTS.Handler;

namespace Aliyun.OTS.Retry
{
    public class DefaultRetryPolicy : RetryPolicy
    {
        public static int MaxRetryTimes = 3;
        
        private int MaxDelay = 2000;
        
        private int ScaleFactor = 2;
        
        private int ServerThrottlingExceptionDelayFactor = 500;
        
        private int StabilityExceptionDelayFactor = 200;

        private readonly Random RandomGenerator;

        public DefaultRetryPolicy()
        {
            RandomGenerator = new Random();
        }
        
        public DefaultRetryPolicy(int maxRetryTimes, int maxRetryDelay)
        {
            MaxRetryTimes = maxRetryTimes;
            if (maxRetryTimes < 0) {
                throw new OTSClientException("maxRetryTimes must be >= 0.");
            }

            MaxDelay = maxRetryDelay;
            if (maxRetryDelay < 0) {
                throw new OTSClientException("maxRetryDelay must be >= 0.");
            }

            RandomGenerator = new Random();
        }
        
        public override bool MaxRetryTimeReached(Context context, OTSException exception)
        {
            return context.RetryTimes >= MaxRetryTimes;
        }
        
        public override bool CanRetry(Context context, OTSException exception)
        {
            if (exception == null)
            {
                // No exception ocurred
                return false;
            }
            
            if (RetryUtil.ShouldRetryNoMatterWhichAPI(exception))
            {
                return true;
            }
            
            if (RetryUtil.IsRepeatableAPI(context.APIName) &&
                RetryUtil.ShouldRetryWhenAPIRepeatable(exception))
            {
                return true;
            }
            
            return false;
        }
        
        public override int DelayBeforeNextRetry(Context context, OTSException exception)
        {
            int delayFactor;
            
            if (RetryUtil.IsServerThrottlingException(exception)) {
                delayFactor = ServerThrottlingExceptionDelayFactor;
            } else {
                delayFactor = StabilityExceptionDelayFactor;
            }
            
            int delayLimit = delayFactor * (int)Math.Pow(ScaleFactor, context.RetryTimes);
            
            if (delayLimit >= MaxDelay) {
                delayLimit = MaxDelay;
            }
            
            int realDelay = RandomGenerator.Next(delayLimit / 2, delayLimit);
            
            return realDelay;
        }
    }
    
    public class RetryPolicyNoDelay : DefaultRetryPolicy
    {
        public override int DelayBeforeNextRetry(Context context, OTSException exception)
        {
            return 0;
        }
    }
    
    public class NoRetryPolicy : DefaultRetryPolicy
    {
        public override bool CanRetry(Context context, OTSException exception)
        {
            return false;
        }
    }
}
