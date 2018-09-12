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


using PB = com.alicloud.openservices.tablestore.core.protocol;

namespace Aliyun.OTS.Handler
{
    public class ErrorHandler : PipelineHandler
    {
        public ErrorHandler(PipelineHandler innerHandler) : base(innerHandler) { }

        public override void HandleBefore(Context context) 
        {
            InnerHandler.HandleBefore(context);
        }
        
        private void throwOTSServerException(Context context)
        {
            var exception = new OTSServerException(context.APIName, context.HttpResponseStatusCode);
            if (context.ClientConfig.OTSErrorLogHandler != null) {
                context.ClientConfig.OTSErrorLogHandler(exception.ToString() + "\n");
            }
            throw exception;
        }

        public override void HandleAfter(Context context) 
        {
         
            OTSServerException exception;
            InnerHandler.HandleAfter(context);
            
            var statusCode = context.HttpResponseStatusCode;
            
            if ((int)statusCode >= 200 && (int)statusCode < 300)
            {
                return;
            }
            
            var builder = PB.Error.CreateBuilder();
            string errorCode = null, errorMessage = null;
            
            try 
            {
                builder.MergeFrom(context.HttpResponseBody);
                var message = builder.Build();
                errorCode = message.Code;
                errorMessage = message.Message;
            } catch (Google.ProtocolBuffers.InvalidProtocolBufferException) {
                throwOTSServerException(context);
            } catch (Google.ProtocolBuffers.UninitializedMessageException) {
                throwOTSServerException(context);
            }
            
            string requestID;
            if (context.HttpResponseHeaders.ContainsKey("x-ots-requestid")) 
            {
                requestID = context.HttpResponseHeaders["x-ots-requestid"];
            }
            else
            {
                requestID = null;
            }
            
            exception = new OTSServerException(
                context.APIName,
                statusCode,
                errorCode,
                errorMessage,
                requestID
            );

            if(context.ClientConfig.OTSErrorLogHandler != null)
            {
                context.ClientConfig.OTSErrorLogHandler.Invoke(exception.ToString());
            }

            throw exception;
        }
    }
}
