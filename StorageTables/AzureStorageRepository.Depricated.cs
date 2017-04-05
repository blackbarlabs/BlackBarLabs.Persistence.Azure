using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using BlackBarLabs.Persistence.Azure.StorageTables.RelationshipDocuments;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;
using BlackBarLabs.Linq.Async;

namespace BlackBarLabs.Persistence.Azure.StorageTables
{
    public partial class AzureStorageRepository
    {

        [Obsolete("Please use Delete<TDocument, TResult> instead.")]
        public async Task<bool> DeleteAsync<TData>(TData data, Func<TData, TData, bool> deleteIssueCallback, int numberOfTimesToRetry = int.MaxValue)
            where TData : class, ITableEntity
        {
            while (true)
            {
                try
                {
                    var table = GetTable<TData>();
                    if (string.IsNullOrEmpty(data.ETag)) data.ETag = "*";
                    var delete = TableOperation.Delete(data);
                    await table.ExecuteAsync(delete);
                    return true;
                }
                catch (StorageException ex)
                {
                    if (ex.IsProblemPreconditionFailed())
                    {
                        var mostRecentData = await FindById<TData>(data.RowKey);
                        var deleteMostRecent = deleteIssueCallback.Invoke(data, mostRecentData);
                        if (!deleteMostRecent)
                            return false;
                        data = mostRecentData;
                        continue;
                    }
                }
                numberOfTimesToRetry--;
                if (numberOfTimesToRetry <= 0)
                    throw new Exception("Tries exceeded");
            }
        }

        public delegate TResult DeleteDelegate<TResult, TData>(TData storedDocument, Action delete);
        [Obsolete("Please use DeleteIf<TDocument, TResult> instead.")]
        public async Task<TResult> DeleteAsync<TResult, TData>(Guid documentId,
            DeleteDelegate<TResult, TData> success,
            NotFoundDelegate<TResult> onNotFound,
            RetryDelegate onTimeout = default(RetryDelegate))
            where TData : class, ITableEntity
        {
            if (default(RetryDelegate) == onTimeout)
                onTimeout = GetRetryDelegate();

            return await await this.FindByIdAsync<TData, Task<TResult>>(documentId,
                async (data) =>
                {
                    var table = GetTable<TData>();
                    if (default(CloudTable) == table)
                        return onNotFound();

                    bool needTodelete = false;
                    var result = success(data, () => needTodelete = true);

                    if (needTodelete)
                    {
                        return await DeleteAsync(data,
                            () => result,
                            () => onNotFound(),
                            onTimeout);
                    }
                    return result;
                },
                () => Task.FromResult(onNotFound()));
        }

        [Obsolete]
        public async Task<TData> CreateAsync<TData>(TData data)
            where TData : class, ITableEntity
        {
            // todo, accept convert func here also
            var table = GetTable<TData>();
            TableResult tableResult = null;
            var insert = TableOperation.Insert(data);
            try
            {
                tableResult = await table.ExecuteAsync(insert);
                //tableResult = table.Execute(insert);

            }
            catch (StorageException storageEx)
            {
                if (!storageEx.IsProblemTableDoesNotExist())
                    throw;

                Console.WriteLine("{0} Possible reason: {1} might not be created yet. Retrying...", storageEx.Message, typeof(TData).Name);
            }
            catch (Exception ex)
            {
                Console.WriteLine("{0} Possible reason: {1} might not be created yet. Retrying...", ex.Message, typeof(TData).Name);
            }
            if (tableResult != null) return (TData) tableResult.Result;
            // Try to create the table when creating a row fails
            var retriesAttempted = await TryCreateTableAsync(table);
            tableResult = await table.ExecuteAsync(insert);
            Console.WriteLine("{0} retries were made to create {1} table.", retriesAttempted, typeof(TData).Name);
            return (TData)tableResult.Result;
        }

        public async Task<TData> Create<TData>(TData data)
           where TData : class, ITableEntity
        {
            // todo, accept convert func here also
            var table = GetTable<TData>();
            TableResult tableResult = null;
            var insert = TableOperation.Insert(data);
            try
            {
                tableResult = table.Execute(insert);
            }
            catch (StorageException storageEx)
            {
                if (!storageEx.IsProblemTableDoesNotExist())
                    throw;

                Console.WriteLine("{0} Possible reason: {1} might not be created yet. Retrying...", storageEx.Message, typeof(TData).Name);
            }
            catch (Exception ex)
            {
                Console.WriteLine("{0} Possible reason: {1} might not be created yet. Retrying...", ex.Message, typeof(TData).Name);
            }
            if (tableResult != null) return (TData)tableResult.Result;
            // Try to create the table when creating a row fails
            var retriesAttempted = await TryCreateTableAsync(table);
            tableResult = await table.ExecuteAsync(insert);
            Console.WriteLine("{0} retries were made to create {1} table.", retriesAttempted, typeof(TData).Name);
            return (TData)tableResult.Result;
        }

        public async Task<TResult> GetFirstAsync<TData, TResult>(TableQuery<TData> query, Func<TData, TResult> convertFunc) where TData : class, ITableEntity, new()
        {
            var table = GetTable<TData>();
            try
            {
                // The FirstOrDefault is needed so that evaluation is immediate rather than returning
                // a lazy object and avoiding our try/catch here.
                var segment = await table.ExecuteQuerySegmentedAsync(query, null);
                var result = segment.Results.FirstOrDefault();
                return result == null ? default(TResult) : convertFunc(result);
            }
            catch (Exception)
            {
                if (!table.Exists()) return default(TResult);
                throw;
            }
        }

        internal Task<TData> GetAsync<TData>(Guid id) where TData : class, ITableEntity, new()
        {
            return GetAsync<TData, TData>(id.AsRowKey(), doc => doc);
        }

        public Task<IEnumerable<TResult>> GetListAsync<TData, TResult>(Func<TData, TResult> convertFunc) where TData : class, ITableEntity, new()
        {
            return GetListAsync(new TableQuery<TData>(), convertFunc);
        }

        internal async Task<IEnumerable<TResult>> GetListAsync<TData, TResult>(TableQuery<TData> query, Func<TData, TResult> convertFunc)
            where TData : class, ITableEntity, new()
        {
            var table = GetTable<TData>();
            try
            {
                TableContinuationToken token = null;
                var results = new List<TResult>();
                do
                {
                    var segment = await table.ExecuteQuerySegmentedAsync(query, token);
                    token = segment.ContinuationToken;
                    results.AddRange(segment.Results.ToList().Select(convertFunc));
                } while (token != null);
                return results;
            }
            catch (AggregateException)
            {
                throw;
            }
            catch (Exception)
            {
                if (!table.Exists()) return (IEnumerable<TResult>)new TResult[] { };
                throw;
            }
        }

        internal Task<TResult> GetAsync<TData, TResult>(Guid id, Func<TData, TResult> convertFunc) where TData : class, ITableEntity, new()
        {
            return GetAsync(id.AsRowKey(), convertFunc);
        }
        
        internal Task<TResult> GetAsync<TData, TResult>(string rowKey, Func<TData, TResult> convertFunc) where TData : class, ITableEntity, new()
        {           
            var partitionKey = rowKey.GeneratePartitionKey();
            var query = new TableQuery<TData>().Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey),
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey)
                    ));

            var result = GetFirstAsync(query, convertFunc);
            return result;
        }

        [Obsolete]
        public async Task<TData> CreateAndAssociateAsync<TData>(TData data, Guid parentKey, Guid associatedPageId, Func<ChildDocument, Task<bool>> assignSharedDocumentFunc) where TData : TableEntity
        {
            var table = GetTable<TData>();
            TableResult tableResult = null;
            var insert = TableOperation.Insert(data);
            try
            {
                tableResult = await table.ExecuteAsync(insert);
            }
            catch (Exception ex)
            {
                Console.WriteLine("{0} Possible reason: {1} might not be created yet. Retrying...", ex.Message, typeof(TData).Name);
            }
            if (tableResult == null)
            {
                // Try to create the table when creating a row fails
                var retriesAttempted = await TryCreateTableAsync(table);
                tableResult = await table.ExecuteAsync(insert);
                Console.WriteLine("{0} retries were made to create {1} table.", retriesAttempted, typeof(TData).Name);
            }

            if (tableResult != null)
            {
                await AssociateAsync(parentKey, associatedPageId, Guid.Parse(data.RowKey), assignSharedDocumentFunc);
            }

            return (TData)tableResult?.Result;
        }

        private async Task<int> TryCreateTableAsync(CloudTable table)
        {
            // Use retry logic to create the table
            var retriesAttempted = 0;
            TimeSpan retryDelay;
            while (retryPolicy.ShouldRetry(retriesAttempted++, retryHttpStatus, retryException, out retryDelay, null))
            {
                await Task.Delay(retryDelay);
                var justCreated = false;
                try
                {
                    justCreated = await table.CreateIfNotExistsAsync();
                }
                catch (StorageException storageException)
                {
                    if (storageException.RequestInformation.HttpStatusCode != 409) throw;  // retry all 409 Conflicts
                }
                if (justCreated || await table.ExistsAsync()) break;
            }
            return retriesAttempted;
        }

        #region Relationships

        [Obsolete]
        internal Task<bool> AssociateListAsync(Guid parentKey, Guid associatedPageId, IList<Guid> itemsToAssociate,
            Func<ChildDocument, Task<bool>> assignSharedDocumentFunc)
        {
            return InternalAssociateAsync(associatedPageId, assignSharedDocumentFunc,
                list =>
                {
                    if (itemsToAssociate.Contains(Guid.Empty)) return false;
                    if (itemsToAssociate.Distinct().Count() != itemsToAssociate.Count) return false;
                    if (list.Intersect(itemsToAssociate).Any()) return false;
                    list.AddRange(itemsToAssociate);
                    return true;
                });
        }

        [Obsolete]
        internal Task<bool> AssociateAsync(Guid parentKey, Guid associatedPageId, Guid itemToAssociate,
            Func<ChildDocument, Task<bool>> assignSharedDocumentFunc)
        {
            return InternalAssociateAsync(associatedPageId, assignSharedDocumentFunc,
                list =>
                {
                    if (itemToAssociate == Guid.Empty || list.Contains(itemToAssociate)) return false;
                    list.Add(itemToAssociate);
                    return true;
                });
        }

        [Obsolete]
        private async Task<bool> InternalAssociateAsync(Guid associatedPageId, 
            Func<ChildDocument, Task<bool>> assignSharedDocumentFunc, Func<List<Guid>, bool> addToListFunc)
        {
            //Get the row which contains the Shared Document GUIDs
            ChildDocument childDocument = null;
            if(associatedPageId != Guid.Empty)
                childDocument = await GetAsync<ChildDocument, ChildDocument>(associatedPageId, shareDocument => shareDocument);

            //If there is no shared Document, we have to create one
            if (childDocument == null)
            {
                var newId = Guid.NewGuid();
                var newDocument = new ChildDocument(newId)
                {
                    OrderedListOfSharedEntities = string.Empty
                };

                childDocument = await CreateAsync(newDocument);
                if (!await assignSharedDocumentFunc.Invoke(newDocument)) return false;
            }

            //Convert the list back to a list of GUIDs
            var currentList = childDocument.OrderedListOfSharedEntities.GetGuidStorageString();

            //Add new items
            if (!addToListFunc(currentList)) return false;

            //ReEncode the Items
            childDocument.OrderedListOfSharedEntities = currentList.SetGuidStorageString();

            //Update the Document
            return (await UpdateIfNotModifiedAsync(childDocument)) != null;
        }

        [Obsolete]
        public async Task<bool> DisassociateAsync(Guid associatedPageId, Guid itemToDisassociate)
        {
            //Get the row which contains the Shared Document GUIDs
            var sharedDocument = await GetAsync<ChildDocument, ChildDocument>(associatedPageId, shareDocument => shareDocument);

            //If there is no shared Document, we have to create one
            if (sharedDocument == null)
            {
                return false;
            }

            //Convert the list back to a list of GUIDs
            var currentList = sharedDocument.OrderedListOfSharedEntities.GetGuidStorageString();

            //Add the new GUID
            if (!currentList.Remove(itemToDisassociate)) return false;

            //ReEncode the Items
            sharedDocument.OrderedListOfSharedEntities = currentList.SetGuidStorageString();

            //Update the Document
            return (await UpdateIfNotModifiedAsync(sharedDocument)) != null;
        }

        public async Task<TResult> GetAssociatedAsync<TDocument, TResult>(Guid associatedPageId, Guid itemToLocate, Func<TDocument, TResult> convertFunc) where TDocument : class, ITableEntity, new()
        {
            //If there is no associated linkage betweeen the Page and the Parent, fail fast
            var sharedDocument = await GetAsync<ChildDocument, ChildDocument>(associatedPageId, document => document);
            if (sharedDocument == null)
            {
                return default(TResult);
            }

            if (sharedDocument.OrderedListOfSharedEntities.Contains(itemToLocate.ToString()))
            {
                return await GetAsync(itemToLocate, convertFunc);
            }
            return default(TResult);
        }

        [Obsolete]
        public async Task<IEnumerable<TResult>> GetAssociatedListAsync<TData, TResult>(Guid associatedPageId,
           Func<TData, TResult> convertFunc) where TData : class, ITableEntity, new() where TResult : class
        {
            if (default(Guid) == associatedPageId)
            {
                return new List<TResult>();
            }

            var sharedDocument = await GetAsync<ChildDocument, ChildDocument>(associatedPageId, document => document);

            if (sharedDocument == null)
            {
                return new List<TResult>();
            }

            //Convert the list back to a list of GUIDs
            var currentList = sharedDocument.OrderedListOfSharedEntities.GetGuidStorageString();

            var associatedList = new List<TResult>();

            var removeList = new List<Guid>();
            foreach (var sharedEntity in currentList)
            {
                var associated = await GetAsync(sharedEntity, convertFunc);
                if (associated == null)
                {
                    removeList.Add(sharedEntity);
                    continue;
                }
                associatedList.Add(associated);
            }
            if (removeList.Any())
            {
                foreach (var removeEntity in removeList)
                    currentList.Remove(removeEntity);
                sharedDocument.OrderedListOfSharedEntities = currentList.SetGuidStorageString();
                await UpdateIfNotModifiedAsync(sharedDocument);
            }
            return associatedList;
        }
        
        public async Task<IEnumerable<Guid>> GetAssociatedGuidListAsync(Guid associatedPageId)
        {
            var sharedDocument = await GetAsync<ChildDocument, ChildDocument>(associatedPageId, document => document);
            if (sharedDocument == null)
            {
                return new Guid[] {};
            }

            //Convert the list back to a list of GUIDs
            return sharedDocument.OrderedListOfSharedEntities.GetGuidStorageString();
        }

        #endregion

        #region Update
        [Obsolete("UpdateAtomicAsync is deprecated, please use UpdateAsync with UpdateDelegate<TData, Task<TResult>> instead.")]
        public async Task<bool> UpdateAtomicAsync<TData>(Guid id, Func<TData, TData> atomicModifier, int numberOfTimesToRetry = int.MaxValue)
                  where TData : class, ITableEntity
        {
            return await UpdateAtomicAsync(id.AsRowKey(), atomicModifier, numberOfTimesToRetry);
        }

        [Obsolete("UpdateAtomicAsync is deprecated, please use UpdateAsync with UpdateDelegate<TData, Task<TResult>> instead.")]
        public async Task<bool> UpdateAtomicAsync<TData>(string id, Func<TData, TData> atomicModifier, int numberOfTimesToRetry = int.MaxValue)
            where TData : class, ITableEntity
        {
            var document = await FindById<TData>(id);
            
            while (true)
            {
                document = atomicModifier.Invoke(document);
                if (default(TData) == document)
                    return false;
                
                try
                {
                    await UpdateIfNotModifiedAsync(document);
                    return true;
                }
                catch (StorageException ex)
                {
                    if (
                        !ex.IsProblemPreconditionFailed() &&
                        !ex.IsProblemTimeout())
                    {
                        throw;
                    }
                }

                numberOfTimesToRetry--;

                if (numberOfTimesToRetry <= 0)
                    throw new Exception("Process has exceeded maximum allowable attempts");

                document = await FindById<TData>(id);
            }
        }

        [Obsolete("UpdateAtomicAsync is deprecated, please use UpdateAsync with UpdateDelegate<TData, Task<TResult>> instead.")]
        public async Task<bool> UpdateAtomicAsync<TData>(Guid id, Func<TData, Task<TData>> atomicModifier, int numberOfTimesToRetry = int.MaxValue)
            where TData : class, ITableEntity => await UpdateAtomicAsync(id.AsRowKey(), atomicModifier, numberOfTimesToRetry);

        [Obsolete("UpdateAtomicAsync is deprecated, please use UpdateAsync with UpdateDelegate<TData, Task<TResult>> instead.")]
        public async Task<bool> UpdateAtomicAsync<TData>(string id, Func<TData, Task<TData>> atomicModifier, int numberOfTimesToRetry = int.MaxValue)
            where TData : class, ITableEntity
        {
            var document = await FindById<TData>(id);
            while (true)
            {
                if(default(TData) == document)
                    throw new RecordNotFoundException<TData>();

                document = await atomicModifier.Invoke(document);
                if (default(TData) == document)
                    return false;

                try
                {
                    await UpdateIfNotModifiedAsync(document);
                    return true;
                }
                catch (StorageException ex)
                {
                    if (
                        !ex.IsProblemPreconditionFailed() &&
                        !ex.IsProblemTimeout())
                    {
                        throw;
                    }
                }

                numberOfTimesToRetry--;

                if (numberOfTimesToRetry <= 0)
                    throw new Exception("Process has exceeded maximum allowable attempts");

                document = await FindById<TData>(id);
            }
        }

        #endregion

        #region Create

        [Obsolete]
        public Task<TData> CreateOrGetLatestAsync<TData>(Guid id, int numberOfTimesToRetry = int.MaxValue) where TData : class, ITableEntity => CreateOrGetLatestAsync<TData>(id.AsRowKey());

        [Obsolete]
        public async Task<TData> CreateOrGetLatestAsync<TData>(string id, int numberOfTimesToRetry = int.MaxValue) where TData : class, ITableEntity
        {

            var document = Activator.CreateInstance<TData>();
            document.SetId(id);

            while (true)
            {
                try
                {
                    await CreateAsync(document);
                    return document;
                }
                catch (StorageException ex)
                {
                    if (ex.IsProblemResourceAlreadyExists())
                    {
                        return await FindById<TData>(id);
                    }
                    if (ex.IsProblemTimeout())
                        continue;
                }
                numberOfTimesToRetry--; if (numberOfTimesToRetry <= 0)
                    throw new Exception("Process has exceeded maximum allowable attempts");
            }

        }
        
        [Obsolete]
        public async Task<bool> CreateOrUpdateAtomicAsync<TData>(Guid id, Func<TData, TData> atomicModifier,
            int numberOfTimesToRetry = int.MaxValue)
            where TData : class, ITableEntity
        {
            return await CreateOrUpdateAtomicAsync<TData>(id.AsRowKey(),
                (data) =>
                {
                    return Task.FromResult(atomicModifier(data));
                });
        }

        private TDocument CreateStorableDocument<TDocument>(Guid id)
            where TDocument : class, ITableEntity
        {
            var document = Activator.CreateInstance<TDocument>();
            document.SetId(id);
            return document;
        }



        [Obsolete("Create is deprecated, please use CreateAsync instead.")]
        public async Task<TResult> Create<TResult, TData>(Guid id, TData document,
            CreateSuccessDelegate<TResult> onSuccess,
            AlreadyExitsDelegate<TResult> onAlreadyExists,
            RetryDelegate onTimeout = default(RetryDelegate))
            where TData : class, ITableEntity
        {
            if (default(RetryDelegate) == onTimeout)
                onTimeout = GetRetryDelegate();

            document.SetId(id);

            while (true)
            {
                try
                {
                    await Create(document);
                    return onSuccess();
                }
                catch (StorageException ex)
                {
                    if (ex.IsProblemResourceAlreadyExists())
                        return onAlreadyExists();

                    if (ex.IsProblemTimeout())
                    {
                        TResult result = default(TResult);
                        await onTimeout(ex.RequestInformation.HttpStatusCode, ex,
                            async () =>
                            {
                                result = await CreateAsync(id, document, onSuccess, onAlreadyExists, onTimeout);
                            });
                        return result;
                    }

                    throw;
                }
                catch (Exception general_ex)
                {
                    var message = general_ex;
                }

            }
        }

        [Obsolete]
        public Task<bool> CreateOrUpdateAtomicAsync<TData>(Guid id, Func<TData, Task<TData>> atomicModifier,
            int numberOfTimesToRetry = int.MaxValue)
            where TData : class, ITableEntity => CreateOrUpdateAtomicAsync(id.AsRowKey(), atomicModifier);

        [Obsolete]
        public async Task<bool> CreateOrUpdateAtomicAsync<TData>(string id, Func<TData, Task<TData>> atomicModifier, int numberOfTimesToRetry = int.MaxValue)
            where TData : class, ITableEntity
        {
            var document = await FindById<TData>(id);
            if (default(TData) == document)
            {
                document = Activator.CreateInstance<TData>();
                document.SetId(id);
                document = await atomicModifier.Invoke(document);
                while (true)
                {
                    try
                    {
                        await CreateAsync(document);
                        return true;
                    }
                    catch (StorageException ex)
                    {
                        if (ex.IsProblemResourceAlreadyExists())
                        {
                            document = await FindById<TData>(id);
                            break;
                        }
                        if (ex.IsProblemTimeout())
                            continue;
                        throw;
                    }
                }
            }

            //Update code
            while (true)
            {
                document = await atomicModifier.Invoke(document);
                if (default(TData) == document)
                    return false;

                try
                {
                    await UpdateIfNotModifiedAsync(document);
                    return true;
                }
                catch (StorageException ex)
                {
                    if (
                        !ex.IsProblemPreconditionFailed() &&
                        !ex.IsProblemTimeout())
                    {
                        throw;
                    }
                }

                numberOfTimesToRetry--;

                if (numberOfTimesToRetry <= 0)
                    throw new Exception("Process has exceeded maximum allowable attempts");

                document = await FindById<TData>(id);
            }
        }


        [Obsolete]
        public Task<bool> CreateAtomicAsync<TData>(Guid id, Func<Task<TData>> atomicModifier,
            int numberOfTimesToRetry = int.MaxValue)
            where TData : class, ITableEntity => CreateAtomicAsync(id.AsRowKey(), atomicModifier);

        [Obsolete]
        public async Task<bool> CreateAtomicAsync<TData>(string id, Func<Task<TData>> atomicModifier, int numberOfTimesToRetry = int.MaxValue)
            where TData : class, ITableEntity
        {
            //Update code
            while (true)
            {
                var document = await atomicModifier.Invoke();
                if (default(TData) == document)
                    return false;

                try
                {
                    await CreateAsync(document);
                    return true;
                }
                catch (StorageException ex)
                {
                    if (ex.IsProblemResourceAlreadyExists())
                        return false;

                    if (!ex.IsProblemTimeout())
                        throw;
                }

                numberOfTimesToRetry--;

                if (numberOfTimesToRetry <= 0)
                    throw new Exception("Process has exceeded maximum allowable attempts");
            }
        }

        #endregion

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

        public async Task<IEnumerable<TData>> FindByQueryAsync<TData>(TableQuery<TData> query)
            where TData : class, ITableEntity, new()
        {
            var table = GetTable<TData>();
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
            catch (Exception)
            {
                if (!table.Exists()) return new TData[] { };
                throw;
            }
        }

        public async Task<IEnumerable<TData>> FindByQueryAsync<TData>(string filter)
            where TData : class, ITableEntity, new()
        {
            var resultsAllPartitions = await Enumerable.Range(-13, 27).Select(async partitionIndex =>
            {
                var query = new TableQuery<TData>().Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition(
                            "PartitionKey",
                            QueryComparisons.Equal,
                            partitionIndex.ToString()),
                        TableOperators.And,
                        filter));

                var foundDocs = (await this.FindByQueryAsync(query)).ToList();
                return foundDocs.ToArray();
            })
             .WhenAllAsync()
             .SelectManyAsync()
             .ToArrayAsync();
            return resultsAllPartitions;
        }

        #region Locked update old

        [Obsolete]
        public Task<bool> LockedUpdateAsync<TDocument>(Guid id,
                Expression<Func<TDocument, bool>> lockedPropertyExpression,
                Func<TDocument, Task<bool>> whileLockedFunc,
                Action<TDocument> mutateEntityToSaveAction)
            where TDocument : TableEntity
        {
            return LockedUpdateAsync<TDocument>(id.AsRowKey(), (doc) => true,
                lockedPropertyExpression, whileLockedFunc, mutateEntityToSaveAction);
        }

        [Obsolete]
        public Task<bool> LockedCreateOrUpdateAsync<TDocument>(Guid id,
                Expression<Func<TDocument, bool>> lockedPropertyExpression,
                Func<TDocument, Task<bool>> whileLockedFunc,
                Action<TDocument> mutateEntityToSaveAction)
            where TDocument : TableEntity
        {
            return LockedCreateOrUpdateAsync<TDocument>(id.AsRowKey(), (doc) => true,
                lockedPropertyExpression, whileLockedFunc, mutateEntityToSaveAction);
        }

        [Obsolete]
        public Task<bool> LockedUpdateAsync<TDocument>(Guid id,
                Func<TDocument, bool> conditionForLocking,
                Expression<Func<TDocument, bool>> lockedPropertyExpression,
                Func<TDocument, Task<bool>> whileLockedFunc,
                Action<TDocument> mutateEntityToSaveAction)
            where TDocument : TableEntity
        {
            return LockedUpdateAsync<TDocument>(id.AsRowKey(), conditionForLocking,
                lockedPropertyExpression, whileLockedFunc, mutateEntityToSaveAction);
        }

        [Obsolete]
        public Task<bool> LockedUpdateAsync<TDocument>(string id,
                Expression<Func<TDocument, bool>> lockedPropertyExpression,
                Func<TDocument, Task<bool>> whileLockedFunc,
                Action<TDocument> mutateEntityToSaveAction)
            where TDocument : TableEntity
        {
            return LockedUpdateAsync<TDocument>(id, (doc) => true,
                lockedPropertyExpression, whileLockedFunc, mutateEntityToSaveAction);
        }

        /// <summary>
        /// Perform operation while property is locked.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="lockedPropertyExpression">Property to lock on</param>
        /// <param name="whileLockedFunc">non-idpotent operation to perform while locked</param>
        /// <param name="mutateEntityToSaveAction">idempotent mutation of entity to be saved</param>
        /// /// <param name="conditionForLocking">idempotent mutation of entity to be saved</param>
        /// <returns></returns>
        [Obsolete]
        public async Task<bool> LockedUpdateAsync<TDocument>(string id,
                Func<TDocument, bool> conditionForLocking,
                Expression<Func<TDocument, bool>> lockedPropertyExpression,
                Func<TDocument, Task<bool>> whileLockedFunc,
                Action<TDocument> mutateEntityToSaveAction)
            where TDocument : TableEntity
        {
            return await LockedUpdateAsync<TDocument>(
                id,
                conditionForLocking,
                lockedPropertyExpression,
                whileLockedFunc,
                mutateEntityToSaveAction,
                async (callback) => await UpdateAtomicAsync<TDocument>(id, callback));
        }

        /// <summary>
        /// Perform operation while property is locked.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="lockedPropertyExpression">Property to lock on</param>
        /// <param name="whileLockedFunc">non-idpotent operation to perform while locked</param>
        /// <param name="mutateEntityToSaveAction">idempotent mutation of entity to be saved</param>
        /// /// <param name="conditionForLocking">idempotent mutation of entity to be saved</param>
        /// <returns></returns>
        [Obsolete]
        public async Task<bool> LockedCreateOrUpdateAsync<TDocument>(string id,
                Func<TDocument, bool> conditionForLocking,
                Expression<Func<TDocument, bool>> lockedPropertyExpression,
                Func<TDocument, Task<bool>> whileLockedFunc,
                Action<TDocument> mutateEntityToSaveAction)
            where TDocument : TableEntity
        {
            return await LockedUpdateAsync<TDocument>(
                id,
                conditionForLocking,
                lockedPropertyExpression,
                whileLockedFunc,
                mutateEntityToSaveAction,
                async (callback) => await CreateOrUpdateAtomicAsync<TDocument>(Guid.Parse(id), callback));
        }

        [Obsolete]
        public async Task<bool> LockedUpdateAsync<TDocument>(string id,
                Func<TDocument, bool> conditionForLocking,
                Expression<Func<TDocument, bool>> lockedPropertyExpression,
                Func<TDocument, Task<bool>> whileLockedFunc,
                Action<TDocument> mutateEntityToSaveAction,
                Func<Func<TDocument, TDocument>, Task<bool>> lookupMethod)
            where TDocument : TableEntity
        {
            // decompile lock property expression to propertyInfo for easy use later
            var lockedPropertyMember = ((MemberExpression)lockedPropertyExpression.Body).Member;
            var fieldInfo = lockedPropertyMember as FieldInfo;
            var propertyInfo = lockedPropertyMember as PropertyInfo;
            // Do Idempotent locking
            var lockedDocument = default(TDocument);
            var retriesAttempted = 0;
            TimeSpan retryDelay;
            var lockSucceeded = false;
            while (retryPolicy.ShouldRetry(retriesAttempted++, retryHttpStatus, retryException, out retryDelay, null))
            {
                lockSucceeded = await lookupMethod((document) =>
                {
                    if (default(TDocument) == document)
                        throw new RecordNotFoundException();

                    if (!conditionForLocking(document))
                        return default(TDocument);

                    var locked = (bool)(fieldInfo != null ? fieldInfo.GetValue(document) : propertyInfo.GetValue(document));
                    if (locked)
                        return default(TDocument);

                    if (fieldInfo != null) fieldInfo.SetValue(document, true);
                    else propertyInfo.SetValue(document, true);

                    lockedDocument = document;
                    return document;
                });

                if (lockSucceeded)
                    break;

                await Task.Delay(retryDelay);
            }
            if (!lockSucceeded)
                return false;

            try
            {
                var opSucceeded = await whileLockedFunc.Invoke(lockedDocument);
                await Unlock<TDocument>(id, mutateEntityToSaveAction, fieldInfo, propertyInfo);
                return opSucceeded;
            }
            catch (Exception ex)
            {
                await Unlock<TDocument>(id, (document) => { }, fieldInfo, propertyInfo);
                throw ex;
            }
        }

        [Obsolete]
        private async Task Unlock<TDocument>(string id, Action<TDocument> mutateEntityToSaveAction, FieldInfo fieldInfo, PropertyInfo propertyInfo)
            where TDocument : TableEntity
        {
            // do idempotent unlocking
            var retriesAttempted = 0;
            TimeSpan retryDelay;
            while (retryPolicy.ShouldRetry(retriesAttempted++, retryHttpStatus, retryException, out retryDelay, null))
            {
                var unlockSucceeded = await UpdateAtomicAsync<TDocument>(id,
                    lockedEntityAtomic =>
                    {
                        if (fieldInfo != null) fieldInfo.SetValue(lockedEntityAtomic, false);
                        else propertyInfo?.SetValue(lockedEntityAtomic, false);

                        mutateEntityToSaveAction.Invoke(lockedEntityAtomic);
                        return lockedEntityAtomic;
                    });
                if (unlockSucceeded) break;
                await Task.Delay(retryDelay);
            }
        }

        #endregion
        
        #region Pages

        internal delegate IEnumerable<TData> PageDelegate<out TData>();
        
        internal IEnumerable<PageDelegate<TData>> GetPages<TData>(int itemsPerPage)
            where TData : class, ITableEntity, new()
        {
            var iter = new RowEnumerator<TData>(TableClient);
            while (true)
            {
                yield return () => GetPage(iter, itemsPerPage);
                try { var current = iter.Current; }
                catch (InvalidOperationException) { yield break; }
            }
        }

        private IEnumerable<TData> GetPage<TData>(IEnumerator<TData> iter, int itemsPerPage)
        {
            var count = 0;
            while (count < itemsPerPage)
            {
                if (!iter.MoveNext()) break;
                count++;
                yield return iter.Current;
            }
        }

        // todo: make this asynchronous
        private class RowEnumerator<TData> : IEnumerator<TData> where TData : class, ITableEntity, new()
        {
            private readonly TableQuery<TData> query;
            private readonly Queue<TData> data = new Queue<TData>();
            private readonly CloudTable table;

            private bool initialized;
            private TableContinuationToken continuation;
            private TData current;

            internal RowEnumerator(CloudTableClient client)
            {
                table = client.GetTableReference(typeof(TData).Name.ToLower());
                query = new TableQuery<TData>();
            }

            public bool MoveNext()
            {
                if (!initialized)
                {
                    FetchData();
                    initialized = true;
                }
                while (true)
                {
                    if (data.Any())
                    {
                        current = data.Dequeue();
                        return true;
                    }

                    if (!data.Any() && continuation != null)
                    {
                        FetchData();
                        continue;
                    }
                    current = default(TData);
                    return false;
                }
            }

            public TData Current
            {
                get
                {
                    if (default(TData) == current) throw new InvalidOperationException();
                    return current;
                }
            }

            object IEnumerator.Current => Current;

            public void Reset()
            {
                continuation = null;
                data.Clear();
                current = null;
                initialized = false;
            }

            public void Dispose() { }

            private void FetchData()
            {
                try
                {
                    var lastSegment = table.ExecuteQuerySegmented(query, continuation);
                    continuation = lastSegment.ContinuationToken;
                    foreach (var item in lastSegment.Results) data.Enqueue(item);
                }
                catch (Exception)
                {
                    if (table.Exists()) throw;
                }
            }

            #endregion
        }
    }
}
