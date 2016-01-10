using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace BlackBarLabs.Persistence.Azure.StorageTables
{
    internal abstract class AtomicEntity<TKey, TDocument> : IDisposable
        where TDocument : TableEntity, IDocument, new()
    {
        public TDocument document;
        private Task<TDocument> createDocumentTask;
        private readonly Task<TDocument> fetchDocumentTask;

        private readonly IDictionary<Type, Queue<Task>> associatedTasks = new Dictionary<Type, Queue<Task>>();

        protected void AddTask<TObject>(Task<TObject> task)
        {
            if (!associatedTasks.ContainsKey(typeof(TObject)))
            {
                lock (associatedTasks)
                    associatedTasks.Add(typeof(TObject), new Queue<Task>());
            }
            var queue = associatedTasks[typeof(TObject)];
            lock (queue)
                queue.Enqueue(task);
        }

        protected void ExecuteAnyPendingTasks<TObject>()
        {
            if (!associatedTasks.ContainsKey(typeof(TObject))) return;
            var queue = associatedTasks[typeof(TObject)];
            lock (queue)
            {
                while (queue.Any())
                {
                    var task = queue.Dequeue();
                    if (!task.IsCompleted)
                        task.Wait();
                }
            }
        }

        protected AtomicEntity(DataStores dataStores, Guid id, Task<TDocument> fetchDocumentTask = null)
        {
            if (dataStores == null)
                throw new ArgumentNullException(nameof(dataStores));
            if (EqualityComparer<Guid>.Default.Equals(id, default(Guid)))
                throw new ArgumentException("Argument cannot be null or the default value.", nameof(id));
            Id = id;
            _dateStores = dataStores;
            this.fetchDocumentTask = fetchDocumentTask ?? dataStores.AzureStorageRepository.GetAsync<TDocument,TDocument>(id, document => document);
        }

        protected AtomicEntity(DataStores dataStores, TDocument document)
        {
            if (dataStores == null)
                throw new ArgumentNullException(nameof(dataStores));
            if (EqualityComparer<TDocument>.Default.Equals(document, default(TDocument)))
                throw new ArgumentException("Argument cannot be null or the default value.", nameof(document));
            _dateStores = dataStores;
            this.document = document;
            Id = document.RowKey.AsGuid();
        }

        protected AtomicEntity(DataStores dataStores, TDocument document, Task<TDocument> createDocumentTask)
        {
            if (dataStores == null)
                throw new ArgumentNullException(nameof(dataStores));
            if (EqualityComparer<TDocument>.Default.Equals(document, default(TDocument)))
                throw new ArgumentException("Argument cannot be null or the default value.", nameof(document));
            if (createDocumentTask == null)
                throw new ArgumentNullException(nameof(createDocumentTask));
            _dateStores = dataStores;
            this.document = document;
            Id = document.RowKey.AsGuid();
            this.createDocumentTask = createDocumentTask;
        }

        protected DataStores _dateStores { get; }

        protected async Task<TDocument> GetDocument()
        {
            if (!EqualityComparer<TDocument>.Default.Equals(document, default(TDocument)))
            {
                // if we are gettin' the data, let's make sure it is persisted also
                if (createDocumentTask != null)
                {
                    document = await createDocumentTask;
                    if (EqualityComparer<TDocument>.Default.Equals(document, default(TDocument)))
                    {
                        throw new Exception();
                    }
                    createDocumentTask = null;
                }
                return document;
            }
            if (fetchDocumentTask == null)
                return default(TDocument);
            document = await fetchDocumentTask;
            if (EqualityComparer<TDocument>.Default.Equals(document, default(TDocument)))
                throw new Exception();
            return document;
        }

        protected async Task<TDocument> GetStoredDocument()
        {
            if (createDocumentTask != null)
            {
                document = await createDocumentTask;
                if (EqualityComparer<TDocument>.Default.Equals(document, default(TDocument)))
                {
                    throw new Exception();
                }
                createDocumentTask = null;
            }
            if (!EqualityComparer<TDocument>.Default.Equals(document, default(TDocument)))
                return document;
            if (fetchDocumentTask == null)
                throw new Exception();
            document = await fetchDocumentTask;
            return document;
        }

        #region AsyncProp Class

        protected abstract class AsyncPropBase<TReturn>
        {
            protected AtomicEntity<TKey, TDocument> Parent { get; }
            private Task<TReturn> Task { get; set; }

            protected AsyncPropBase(AtomicEntity<TKey, TDocument> parent)
            {
                if (parent == null)
                    throw new ArgumentNullException(nameof(parent));
                Parent = parent;
            }

            public Task<TReturn> GetTask()
            {
                return Task ?? (Task = PrivateTask());
            }

            protected abstract Task<TReturn> PrivateTask();
        }

        protected sealed class PropTask<TReturn> : AsyncPropBase<TReturn>
        {
            private readonly Func<TDocument, TReturn> lookupFunc;
            private readonly MemberInfo expression;

            public PropTask(AtomicEntity<TKey, TDocument> parent, Func<TDocument, TReturn> lookupFunc, Expression<Func<TDocument, TKey>> propertyExpression = null)
                : base(parent)
            {
                if (lookupFunc == null)
                    throw new ArgumentNullException(nameof(lookupFunc));
                this.lookupFunc = lookupFunc;
                if (propertyExpression != null) expression = ((MemberExpression)propertyExpression.Body).Member;
            }

            protected override async Task<TReturn> PrivateTask()
            {
                var document = await Parent.GetDocument();
                if (EqualityComparer<TDocument>.Default.Equals(document, default(TDocument)))
                    return default(TReturn);
                if (expression == null)
                    return lookupFunc.Invoke(document);
                var fieldInfo = expression as FieldInfo;
                var propertyInfo = expression as PropertyInfo;
                var value = default(TKey);
                if (fieldInfo != null)
                    value = (TKey)fieldInfo.GetValue(document);
                else if (propertyInfo != null)
                    value = (TKey)propertyInfo.GetValue(document);
                return EqualityComparer<TKey>.Default.Equals(value, default(TKey)) ? default(TReturn) : lookupFunc.Invoke(document);
            }
        }

        protected sealed class PropTaskEnumerableAsync<TReturn>
        {
            //private Task<IEnumerable<TReturn>> task;
            //private Task<IList<TKey>> fetchIdsTask;

            private readonly AtomicEntity<TKey, TDocument> parent;
            private readonly Func<TDocument, IList<TKey>> fetchIdsFunc;
            private readonly Func<TKey, TReturn> fetchItemsFunc;

            public PropTaskEnumerableAsync(AtomicEntity<TKey, TDocument> parent,
                Func<TDocument, IList<TKey>> fetchIdsFunc, Func<TKey, TReturn> fetchItemsFunc)
            {
                if (fetchIdsFunc == null)
                    throw new ArgumentNullException(nameof(fetchIdsFunc));
                if (fetchItemsFunc == null)
                    throw new ArgumentNullException(nameof(fetchItemsFunc));
                this.parent = parent;
                this.fetchIdsFunc = fetchIdsFunc;
                this.fetchItemsFunc = fetchItemsFunc;
            }

        }

        protected sealed class PropTaskConditionAsync
        {
            //private Task<bool> result;
            //private Task<IList<TKey>> fetchIdsTask;

            private readonly AtomicEntity<TKey, TDocument> parent;
            private readonly Func<TDocument, IList<TKey>> fetchIdsFunc;
            private readonly Func<IEnumerable<TKey>, bool> testIdsFunc;

            public PropTaskConditionAsync(AtomicEntity<TKey, TDocument> parent,
                Func<TDocument, IList<TKey>> fetchIdsFunc, Func<IEnumerable<TKey>, bool> testIdsFunc)
            {
                if (fetchIdsFunc == null)
                    throw new ArgumentNullException(nameof(fetchIdsFunc));
                if (testIdsFunc == null)
                    throw new ArgumentNullException(nameof(testIdsFunc));
                this.parent = parent;
                this.fetchIdsFunc = fetchIdsFunc;
                this.testIdsFunc = testIdsFunc;
            }

        }

        #endregion

        internal protected async Task<bool> UpdateAtomicAsync(Func<TDocument, bool> atomicModifier, int numberOfTimesToRetry = int.MaxValue)
        {
            var currentDocument = await GetStoredDocument();
            
            var numberOfRetryAttempts = 0;
            
            while (true)
            {
                if (!atomicModifier.Invoke(currentDocument))
                    return false;
                
                try
                {
                    currentDocument = await UpdateIfNotModifiedAsync(currentDocument);
                    document = currentDocument;
                    return true;
                }
                catch (Microsoft.WindowsAzure.Storage.StorageException ex)
                {
                    if (
                        !ex.IsProblemPreconditionFailed() &&
                        !ex.IsProblemTimeout())
                    {
                        throw;
                    }
                }

                numberOfRetryAttempts++;

                await Task.Delay((int)Math.Round(Math.Pow(2, numberOfRetryAttempts - 1) * 250));

                if (numberOfRetryAttempts >= numberOfTimesToRetry)
                    throw new Exception("Process has exceeded maximum allowable attempts");

                currentDocument = await _dateStores.AzureStorageRepository.GetAsync<TDocument,TDocument>(currentDocument.RowKey, doc => doc);
            }
        }

        private async Task<TDocument> UpdateIfNotModifiedAsync(TDocument updatedDocument)
        {
            var response = await _dateStores.AzureStorageRepository.UpdateIfNotModifiedAsync(updatedDocument);
            updatedDocument.ETag = response.ETag;
            updatedDocument.Timestamp = response.Timestamp;
            return updatedDocument;
        }

        protected Task<bool> DeleteAsync()
        {
            return UpdateEntityStateAsync(StorageState.RetiredData);
        }

        public Guid Id { get; }

        public void Dispose()
        {
            createDocumentTask?.RunSynchronously();
        }

        private PropTask<StorageState> entityStateTask;
        public Task<StorageState> EntityStateAsync
        {
            get
            {
                if (entityStateTask == null)
                    entityStateTask = new PropTask<StorageState>(this, item => (StorageState)item.EntityState);
                return entityStateTask.GetTask();
            }
        }

        public virtual Task<bool> UpdateEntityStateAsync(StorageState storageState)
        {
            return UpdateAtomicAsync(documentToModify =>
            {
                document.EntityState = (int)storageState;
                entityStateTask = null;
                return true;
            });
        }
    }
}
