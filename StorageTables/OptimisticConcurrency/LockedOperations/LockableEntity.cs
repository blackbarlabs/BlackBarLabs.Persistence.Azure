using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;

namespace BlackBarLabs.Persistence.Azure.StorageTables
{
    internal class LockableEntity<TKey, TDocument>
        : AtomicEntity<TKey, TDocument>
            where TDocument : TableEntity, IDocument, new()
    {
        private readonly Exception retryException = new Exception();
        private const int retryHttpStatus = 200;
        private readonly ExponentialRetry retryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(4), 10);

        protected LockableEntity(DataStores dataStores, Guid id, Task<TDocument> fetchDocumentTask = null)
            : base(dataStores, id, fetchDocumentTask)
        {

        }

        protected LockableEntity(DataStores dataStores, TDocument document)
            : base( dataStores, document)
        {

        }

        protected LockableEntity(DataStores dataStores, TDocument document, Task<TDocument> createDocumentTask)
            : base(dataStores, document, createDocumentTask)
        {

        }

        /// <summary>
        /// Perform operation while property is locked.
        /// </summary>
        /// <param name="lockedPropertyExpression">Property to lock on</param>
        /// <param name="whileLockedFunc">non-idpotent operation to perform while locked</param>
        /// <param name="mutateEntityToSaveAction">idempotent mutation of entity to be saved</param>
        /// <returns></returns>
        internal async Task<bool> LockedUpdateAsync(
                Expression<Func<TDocument, bool>> lockedPropertyExpression,
                Func<TDocument, Task<bool>> whileLockedFunc,
                Action<TDocument> mutateEntityToSaveAction)
        {
            // decompile lock property expression to propertyInfo for easy use later
            var lockedPropertyMember = ((MemberExpression)lockedPropertyExpression.Body).Member;
            var fieldInfo = lockedPropertyMember as FieldInfo;
            var propertyInfo = lockedPropertyMember as PropertyInfo;
            // Do Idempotent locking
            var lockedDocument = default(TDocument);
            var retriesAttempted = 0;
            TimeSpan retryDelay;
            bool lockSucceeded = false;
            while (retryPolicy.ShouldRetry(retriesAttempted++, retryHttpStatus, retryException, out retryDelay, null))
            {
                lockSucceeded = await UpdateAtomicAsync(
                    document =>
                    {
                        var locked =
                            (bool) (fieldInfo != null ? fieldInfo.GetValue(document) : propertyInfo.GetValue(document));
                        if (locked)
                            return false;

                        if (fieldInfo != null) fieldInfo.SetValue(document, true);
                        else propertyInfo.SetValue(document, true);
                        lockedDocument = document;
                        return true;
                    });
                if (lockSucceeded) break;
                await Task.Delay(retryDelay);
            }
            if (!lockSucceeded)
                return false;

            bool opSucceeded = false;
            Exception opException = null;
            try
            {
                opSucceeded = await whileLockedFunc.Invoke(lockedDocument);
            }
            catch (Exception e)
            {
                opException = e;
            }

            // do idempotent unlocking
            retriesAttempted = 0;
            while (retryPolicy.ShouldRetry(retriesAttempted++, retryHttpStatus, retryException, out retryDelay, null))
            {
                bool unlockSucceeded = await UpdateAtomicAsync(
                    lockedEntityAtomic =>
                    {
                        if (fieldInfo != null) fieldInfo.SetValue(lockedEntityAtomic, false);
                        else propertyInfo.SetValue(lockedEntityAtomic, false);

                        mutateEntityToSaveAction.Invoke(lockedEntityAtomic);
                        return true;
                    });
                if (unlockSucceeded) break;
                await Task.Delay(retryDelay);
            }
            if (opException != null) throw opException;
            return opSucceeded;
        }
    }
}
