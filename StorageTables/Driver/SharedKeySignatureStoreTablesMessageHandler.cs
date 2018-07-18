using EastFive.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EastFive.Azure.StorageTables.Driver
{
    public class SharedKeySignatureStoreTablesMessageHandler : DelegatingHandler
    {
        private byte[] key;
        private string azureStorageAccount;

        public SharedKeySignatureStoreTablesMessageHandler(string azureStorageAccount, string azureStorageAccountSharedKey, HttpMessageHandler innerHandler = default(HttpMessageHandler))
            : base()
        {
            this.azureStorageAccount = azureStorageAccount;
            this.key = Convert.FromBase64String(azureStorageAccountSharedKey);
            
            if (innerHandler.IsDefaultOrNull())
                innerHandler = new HttpClientHandler();
            this.InnerHandler = innerHandler;
        }

        protected byte [] HMACSHA256(string signatureString)
        {
            // Have to create this every time because it is not thread safe
            using (var hmac = new HMACSHA256(key))
            {
                var signatureBytes = Encoding.UTF8.GetBytes(signatureString);
                var hashValue = hmac.ComputeHash(signatureBytes);
                return hashValue;
            }
        }

        protected async override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var verb = request.Method.Method.ToUpper();
            var contentMd5 = Convert.ToBase64String(request.Content.Headers.ContentMD5);
            var conentType = request.Content.Headers.ContentType.ToString();
            var date = request.Headers.Date.HasValue ?
                request.Headers.Date.Value.ToString()
                :
                "";
            var canonicalizedResource = CanonicalizedResource(request);
            var stringToSign = verb + "\n" +
               contentMd5 + "\n" +
               conentType + "\n" +
               date + "\n" +
               canonicalizedResource;

            var signature = Convert.ToBase64String(HMACSHA256(stringToSign));
            request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue($"SharedKey {this.azureStorageAccount}:{signature}");

            return await base.SendAsync(request, cancellationToken);
        }

        /// <summary>
        /// Beginning with an empty string (""), append a forward slash (/), followed by the name of the account that owns the resource being accessed.
        /// 
        /// Append the resource's encoded URI path. If the request URI addresses a component of the resource, append the appropriate query string. 
        /// The query string should include the question mark and the comp parameter (for example, ?comp=metadata).
        /// No other parameters should be included on the query string.
        /// </summary>
        /// <returns></returns>
        protected static string CanonicalizedResource(HttpRequestMessage request)
        {
            var sections = request.RequestUri.PathAndQuery.Split(new char[] { '?' });
            var path = sections[0];
            return request.RequestUri.ParseQueryParameter(
                    (string comp) => comp,
                    comp => comp,
                    (comp) => $"{path}?comp={comp}",
                    () => path);
        }
    }
}
