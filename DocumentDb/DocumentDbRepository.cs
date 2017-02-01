using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using BlackBarLabs.Core.Extensions;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace BlackBarLabs.Persistence.Azure.DocumentDb
{
    public class DocumentDbRepository
    {
        private readonly DocumentClient client;
        private readonly string databaseName;

        public DocumentDbRepository(string endpointUri, string primaryKey, string databaseName)
        {
            this.databaseName = databaseName;
            this.client = new DocumentClient(new Uri(endpointUri), primaryKey);
        }

        private async Task<TResult> CreateDatabaseIfNotExists<TResult>(
            Func<TResult> success,
            Func<string, TResult> failure)
        {
            try
            {
                await this.client.CreateDatabaseAsync(new Database { Id = databaseName });
                return success();
            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode == HttpStatusCode.Conflict)
                {
                    //Already exists, return success
                    return success();
                }
                return failure(ex.Message);
            }
        }

        private async Task<TResult> CreateDocumentCollectionIfNotExists<TResult>(string collectionName,
            Func<TResult> success,
            Func<string, TResult> failure )
        {
            try
            {
                var collectionInfo = new DocumentCollection
                {
                    Id = collectionName,
                    IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) {Precision = -1})
                };
                await this.client.CreateDocumentCollectionAsync(
                    UriFactory.CreateDatabaseUri(databaseName),
                    collectionInfo,
                    new RequestOptions {OfferThroughput = 400});
                return success();
            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    return await await CreateDatabaseIfNotExists(
                        async () =>
                        {
                            return await CreateDocumentCollectionIfNotExists(collectionName, success, failure);
                        },
                        (message) =>
                        {
                            return failure(message).ToTask();
                        });
                }

                if (ex.StatusCode == HttpStatusCode.Conflict)
                {
                    // Already exists, return success
                    return success();
                }
                return failure(ex.Message);
            }
        }

        public async Task<TResult> CreateDocumentIfNotExists<TDoc, TResult>(IDocumentDbDocument document,
            Func<TResult> success,
            Func<string, TResult> failure)
        {
            try
            {
                await this.client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(databaseName, typeof(TDoc).Name), document);
                return success();
            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    return await await CreateDocumentCollectionIfNotExists(typeof(TDoc).Name,
                        async () =>
                        {
                            return await CreateDocumentIfNotExists<TDoc, TResult>(document, success, failure);
                        },
                        (message) =>
                        {
                            return failure(message).ToTask();
                        });
                }
                return failure(ex.Message);
            }
        }


        public TDoc Query<TDoc>(string collectionName, 
            Func<IQueryable<TDoc>, TDoc> result)
        {
            var queryOptions = new FeedOptions { MaxItemCount = -1 };
            var queryable = this.client.CreateDocumentQuery<TDoc>(
                UriFactory.CreateDocumentCollectionUri(databaseName, collectionName), queryOptions);
            return result(queryable);
        }
    }
}
