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

using System.Linq;
using System.Threading;

using Aliyun.OTS.Retry;

namespace Aliyun.OTS.Handler
{
    public class RetryHandler : PipelineHandler
    {
        public RetryHandler(PipelineHandler innerHandler) : base(innerHandler) { }

        public override void HandleBefore(Context context) 
        {
            InnerHandler.HandleBefore(context);
        }
        
        private bool ShouldRetry(RetryPolicy retryPolicy, Context context, OTSException exception)
        {
            if (retryPolicy.MaxRetryTimeReached(context, exception))
            {
                return false;
            }
            
            if (retryPolicy.CanRetry(context, exception))
            {
                return true;
            }
            
            return false;
        }
        
        private void RestRequestForRetry(Context context)
        {
        }
        
        private void ResetRetry(Context context)
        {
            InnerHandler.HandleBefore(context);
            context.HttpTask.Wait();
        }
        
        public override void HandleAfter(Context context) 
        {
            var retryPolicy = context.ClientConfig.RetryPolicy;
            
            while (true) 
            { 
                OTSException exceptionForRetry = null;
                
                try {
                    InnerHandler.HandleAfter(context);
                } catch (OTSClientException exception) {
                    exceptionForRetry = exception;
                } catch (OTSServerException exception) {
                    exceptionForRetry = exception;
                }
                
                if (OTSClientTestHelper.RetryTimesAndBackOffRecordSwith) {
                    if (OTSClientTestHelper.RetryExceptions.Count() > OTSClientTestHelper.RetryTimes) {
                        exceptionForRetry = OTSClientTestHelper.RetryExceptions[OTSClientTestHelper.RetryTimes];
                    }
                }
                
                if (ShouldRetry(retryPolicy, context, exceptionForRetry)) {
                    RestRequestForRetry(context);
                    int retryDelay = retryPolicy.DelayBeforeNextRetry(context, exceptionForRetry);
                    Thread.Sleep(retryDelay);
                    ResetRetry(context);
                    context.RetryTimes += 1;
                    
                    if (OTSClientTestHelper.RetryTimesAndBackOffRecordSwith) {
                        OTSClientTestHelper.RetryTimes += 1;
                        OTSClientTestHelper.RetryDelays.Add(retryDelay);
                    }
                    
                    continue;
                }
                
                if (exceptionForRetry != null) {
                    throw exceptionForRetry;
                }
                
                // TODO handle retry in BatchWriteRow & BatchGetRow
                return;
            }

        }
    }
}
