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
                await ExecuteWithRetries(()=> this.client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(databaseName, typeof(TDoc).Name), document));
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

        //From https://blogs.msdn.microsoft.com/bigdatasupport/2015/09/02/dealing-with-requestratetoolarge-errors-in-azure-documentdb-and-testing-performance/
        private async Task<V> ExecuteWithRetries<V>(Func<Task<V>> function)
        {
            TimeSpan sleepTime = TimeSpan.Zero;

            while (true)
            {
                try
                {
                    return await function();
                }
                catch (DocumentClientException de)
                {
                    if (de.StatusCode == HttpStatusCode.NotFound)
                    {

                    }
                    if ((int)de.StatusCode != 429)
                    {
                        throw;
                    }
                    sleepTime = de.RetryAfter;
                }
                catch (AggregateException ae)
                {
                    if (!(ae.InnerException is DocumentClientException))
                    {
                        throw;
                    }

                    DocumentClientException de = (DocumentClientException)ae.InnerException;
                    if ((int)de.StatusCode != 429)
                    {
                        throw;
                    }
                    sleepTime = de.RetryAfter;
                }

                await Task.Delay(sleepTime);
            }
        }

        public TResult GetCollection<TDoc, TResult>(
            Func<IQueryable<TDoc>, TResult> result)
        {
            var collectionName = typeof(TDoc).Name;
            var queryOptions = new FeedOptions { EnableCrossPartitionQuery = true };
            var queryable = this.client.CreateDocumentQuery<TDoc>(
                UriFactory.CreateDocumentCollectionUri(databaseName, collectionName), queryOptions);
            return result(queryable);
        }

        public async Task<TResult> DeleteAsync<TDoc, TResult>(Guid id,
            Func<TResult> success,
            Func<TResult> notFound,
            Func<string, TResult> failure)
        {
            var queryable = client.CreateDocumentQuery(UriFactory.CreateDocumentCollectionUri(databaseName, typeof(TDoc).Name));
            var queryDoc = queryable.Where(document => document.Id == id.ToString())
                .AsEnumerable();
            var doc = queryDoc.FirstOrDefault();
            if (default(Document) == doc)
                return notFound();

            try
            {
                await client.DeleteDocumentAsync(doc.SelfLink);
                return success();
            }
            catch (Exception ex)
            {
                return failure(ex.Message);
            }
        }

        public async Task<TResult> DeleteAsync<TDoc, TResult>(Guid id,
            Func<TResult> success)
        {
            var sql = $"SELECT VALUE c._self FROM c WHERE c.id = '{id}'";

            var documentLinks = client.CreateDocumentQuery<string>(UriFactory.CreateDocumentCollectionUri(databaseName, typeof(TDoc).Name), sql).ToList();

            Console.WriteLine("Found {0} documents to be deleted", documentLinks.Count);

            foreach (var documentLink in documentLinks)
            {
                await ExecuteWithRetries(() => client.DeleteDocumentAsync(documentLink));
                //await client.DeleteDocumentAsync(documentLink);
            }
            return success();
        }


    }
}