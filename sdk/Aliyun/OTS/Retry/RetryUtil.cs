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

namespace Aliyun.OTS.Retry
{
    public static class RetryUtil
    {
        public static bool ShouldRetryNoMatterWhichAPI(OTSException exception)
        {
            var e = exception as OTSServerException;

            if (e != null)
            {
                if (e.ErrorCode == "OTSRowOperationConflict" ||
                    e.ErrorCode == "OTSNotEnoughCapacityUnit" ||
                    e.ErrorCode == "OTSTableNotReady" ||
                    e.ErrorCode == "OTSPartitionUnavailable" ||
                    e.ErrorCode == "OTSServerBusy")
                {
                    return true;
                }

                if (e.ErrorCode == "OTSQuotaExhausted" &&
                    e.ErrorMessage == "Too frequent table operations.")
                {
                    return true;
                }
            }

            return false;
        }
        
        public static bool IsRepeatableAPI(string apiName)
        {
            if (apiName == "/ListTable" ||
                apiName == "/DescribeTable" ||
                apiName == "/GetRow" ||
                apiName == "/BatchGetRow" ||
                apiName == "/GetRange" ||
                apiName == "/SQLQuery" ||
                apiName == "/Search" )
            {
                return true;
            }
            
            return false;
        }
        
        public static bool ShouldRetryWhenAPIRepeatable(OTSException exception)
        {
            var e = exception as OTSServerException;

            if (e != null)
            {
                if (e.ErrorCode == "OTSTimeout" ||
                    e.ErrorCode == "OTSInternalServerError" ||
                    e.ErrorCode == "OTSServerUnavailable")
                {
                    return true;
                }

                int code = (int)e.HttpStatusCode;
                if (code == 500 || code == 502 || code == 503)
                {
                    return true;
                }

                // TODO handle network error & timeout
            }

            return false;
        }
        
        public static bool IsServerThrottlingException(OTSException exception)
        {
            var e = exception as OTSServerException;

            if (e != null)
            {
                if (e.ErrorCode == "OTSServerBusy" ||
                    e.ErrorCode == "OTSNotEnoughCapacityUnit" ||
                    (e.ErrorCode == "OTSQuotaExhausted" && e.ErrorMessage == "Too frequent table operations."))
                {
                    return true;
                }
            }

            return false;
        }
    }
}
