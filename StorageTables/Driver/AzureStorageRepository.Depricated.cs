using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using BlackBarLabs.Linq.Async;
using BlackBarLabs.Persistence.Azure.StorageTables.RelationshipDocuments;
using EastFive.Azure.StorageTables.Driver;
using EastFive.Extensions;
using EastFive.Linq.Async;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace BlackBarLabs.Persistence.Azure.StorageTables
{
    public partial class AzureStorageRepository
    {
        
        [Obsolete("Use FindByIdAsync")]
        public Task<TEntity> FindById<TEntity>(Guid rowId)
            where TEntity : class,ITableEntity
        {
            return FindById<TEntity>(rowId.AsRowKey());
        }

        [Obsolete("Use FindByIdAsync")]
        public async Task<TEntity> FindById<TEntity>(string rowKey)
                   where TEntity : class, ITableEntity
        {
            TEntity entity = null;
            if(!await TryFindByIdAsync<TEntity>(rowKey, (retries, data) =>
            {
                entity = data;
                if (retries > 0)
                    Console.WriteLine($"{retries} retries where made to query {typeof (TEntity).Name} table.");
            }))
                throw new Exception("Unable to query Azure.");
            return entity;
        }
        
        private delegate void QueryDelegate<in TData>(int retries, TData data);
        [Obsolete("Use FindByIdAsync")]
        private async Task<bool> TryFindByIdAsync<TData>(string rowKey, QueryDelegate<TData> callback) where TData : class, ITableEntity
        {
            var retriesAttempted = 0;
            bool shouldRetry;
            StorageException ex;
            var table = GetTable<TData>();
            var operation = TableOperation.Retrieve<TData>(rowKey.GeneratePartitionKey(), rowKey);
            do
            {
                try
                {
                    var result = await table.ExecuteAsync(operation);
                    callback(retriesAttempted, (TData)result.Result);
                    return true;
                }
                catch (StorageException se)
                {
                    if (retriesAttempted == 0)
                    {
                        if (!await table.ExistsAsync())
                        {
                            callback(0, default(TData));
                            return true;
                        }
                    }
                    TimeSpan retryDelay;
                    shouldRetry = retryPolicy.ShouldRetry(retriesAttempted++, se.RequestInformation.HttpStatusCode, se, out retryDelay, null);
                    ex = se;
                    if (shouldRetry) await Task.Delay(retryDelay);
                }
            } while (shouldRetry);
            Console.WriteLine($"{ex.Message} {typeof(TData).Name} could not be queried after {retriesAttempted - 1} retries.");
            return false;
        }

        [Obsolete("Use FindAllAsync")]
        public async Task<IEnumerable<TData>> FindByQueryAsync<TData>(TableQuery<TData> query, int numberOfTimesToRetry = DefaultNumberOfTimesToRetry)
            where TData : class, ITableEntity, new()
        {
            var table = GetTable<TData>();
            while (true)
            {
                try
                {
                    // The ToList is needed so that evaluation is immediate rather than returning
                    // a lazy object and avoiding our try/catch here.
                    TableContinuationToken token = null;
                    var results = new TData[] { };
                    do
                    {
                        var segment = await table.ExecuteQuerySegmentedAsync(query, token);
                        token = segment.ContinuationToken;
                        results = results.Concat(segment.Results).ToArray();
                    } while (token != null);
                    return results;
                }
                catch (AggregateException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    if (!table.Exists()) return new TData[] { };
                    if (ex is StorageException except && except.IsProblemTimeout())
                    {
                        if (--numberOfTimesToRetry > 0)
                        {
                            await Task.Delay(DefaultBackoffForRetry);
                            continue;
                        }
                    }
                    throw;
                }
            }
        }

        public IEnumerableAsync<TData> FindAll<TData>(int numberOfTimesToRetry = DefaultNumberOfTimesToRetry)
            where TData : class, ITableEntity, new()
        {
            var query = new TableQuery<TData>();
            return FindAllAsync(query);
        }

        public IEnumerableAsync<TData> FindAllAsync<TData>(TableQuery<TData> query, int numberOfTimesToRetry = DefaultNumberOfTimesToRetry)
            where TData : class, ITableEntity, new()
        {
            var table = GetTable<TData>();
            var token = default(TableContinuationToken);
            var segment = default(TableQuerySegment<TData>);
            var resultsIndex = 0;
            return EnumerableAsync.Yield<TData>(
                async (yieldReturn, yieldBreak) =>
                {
                    if(segment.IsDefaultOrNull() || segment.Results.Count <= resultsIndex)
                    {
                        resultsIndex = 0;
                        while (true)
                        {
                            try
                            {
                                if ((!segment.IsDefaultOrNull()) && token.IsDefaultOrNull())
                                    return yieldBreak;

                                segment = await table.ExecuteQuerySegmentedAsync(query, token);
                                token = segment.ContinuationToken;
                                if (!segment.Results.Any())
                                    continue;
                                break;
                            }
                            catch (AggregateException)
                            {
                                throw;
                            }
                            catch (Exception ex)
                            {
                                if (!table.Exists())
                                    return yieldBreak;
                                if (ex is StorageException except && except.IsProblemTimeout())
                                {
                                    if (--numberOfTimesToRetry > 0)
                                    {
                                        await Task.Delay(DefaultBackoffForRetry);
                                        continue;
                                    }
                                }
                                throw;
                            }
                        }
                    }

                    var result = segment.Results[resultsIndex];
                    resultsIndex++;
                    return yieldReturn(result);
                });
        }

        public async Task<IEnumerable<TData>> FindByQueryAsync<TData>(string filter)
            where TData : class, ITableEntity, new()
        {
            var resultsAllPartitions = await Enumerable
                .Range(-13, 27)
                .Select(
                    partitionIndex =>
                    {
                        var query = new TableQuery<TData>().Where(
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition(
                                "PartitionKey",
                                QueryComparisons.Equal,
                                partitionIndex.ToString()),
                            TableOperators.And,
                            filter));

                        var foundDocs = this.FindAllAsync(query);
                        return foundDocs;
                    })
                .SelectMany()
                .Async();
            return resultsAllPartitions;
        }
        
    }
}
