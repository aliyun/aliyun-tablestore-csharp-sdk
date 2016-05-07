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
using System.Net.Http;
using System.Threading;
using System.Collections.Concurrent;

namespace Aliyun.OTS
{
    /// <summary>
    /// OTS SDK的连接池实现。使用 ConcurrentBag 作为连接容器实现。
    /// </summary>
    public class OTSConnectionPool
    {
        private ConcurrentQueue<HttpClient> Pool;
        private string EndPoint;

        /// <summary>
        /// OTSConnectionPool的构造函数。
        /// </summary>
        /// <param name="endPoint">OTS服务地址</param>
        /// <param name="connectionLimit">最大连接数</param>
        public OTSConnectionPool(string endPoint, int connectionLimit)
        {
            Pool = new ConcurrentQueue<HttpClient>();
            EndPoint = endPoint;

            for (int i = 0; i < connectionLimit; i ++)
            {
                var client = new HttpClient();
                client.BaseAddress = new Uri(EndPoint);
                Pool.Enqueue(client);
            }
        }

        /// <summary>
        /// 从连接池中取出一个连接；若连接池为空（此时连接池已经被用满）则等待。
        /// </summary>
        /// <returns>得到的连接。</returns>
        public HttpClient TakeHttpClient()
        {
            HttpClient client;
            while (!Pool.TryDequeue(out client)) {
                Thread.Sleep(10);
            }
            return client;
        }

        /// <summary>
        /// 将一个连接归还到连接池中，以便其他请求可以重用这个连接。
        /// </summary>
        /// <param name="httpClient"></param>
        public void ReturnHttpClient(HttpClient httpClient)
        {
            Pool.Enqueue(httpClient);
        }
        
    }
}
