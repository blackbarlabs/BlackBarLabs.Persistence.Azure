using BlackBarLabs.Persistence.Azure;
using BlackBarLabs.Persistence.Azure.StorageTables;
using EastFive.Azure.StorageTables.Driver;
using EastFive.Collections.Generic;
using EastFive.Extensions;
using EastFive.Linq;
using EastFive.Linq.Expressions;
using EastFive.Reflection;
using EastFive.Serialization;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence.Azure.StorageTables.Driver
{
    public class AzureTableDriverDynamic
    {
        protected const int DefaultNumberOfTimesToRetry = 10;
        protected static readonly TimeSpan DefaultBackoffForRetry = TimeSpan.FromSeconds(4);
        protected readonly ExponentialRetry retryPolicy = new ExponentialRetry(DefaultBackoffForRetry, DefaultNumberOfTimesToRetry);

        public readonly CloudTableClient TableClient;
        public readonly CloudBlobClient BlobClient;
        private const int retryHttpStatus = 200;

        private readonly Exception retryException = new Exception();

        public AzureTableDriverDynamic(CloudStorageAccount storageAccount)
        {
            TableClient = storageAccount.CreateCloudTableClient();
            TableClient.DefaultRequestOptions.RetryPolicy = retryPolicy;

            BlobClient = storageAccount.CreateCloudBlobClient();
            BlobClient.DefaultRequestOptions.RetryPolicy = retryPolicy;
        }

        public static AzureTableDriverDynamic FromSettings(string settingKey = EastFive.Azure.Persistence.AppSettings.Storage)
        {
            return EastFive.Web.Configuration.Settings.GetString(settingKey,
                (storageString) => FromStorageString(storageString),
                (why) => throw new Exception(why));
        }

        public static AzureTableDriverDynamic FromStorageString(string storageSetting)
        {
            var cloudStorageAccount = CloudStorageAccount.Parse(storageSetting);
            var azureStorageRepository = new AzureTableDriverDynamic(cloudStorageAccount);
            return azureStorageRepository;
        }

        private CloudTable GetTable<T>()
        {
            var tableName = typeof(T).Name.ToLower();
            return TableClient.GetTableReference(tableName);
        }

        private class CreatableEntity : ITableEntity
        {
            private Type entityType;
            private object entity;

            public string RowKey
            {
                get
                {
                    var properties = entityType
                        .GetMembers()
                        .ToArray();
                    var attributesKvp = properties
                        .Where(propInfo => propInfo.ContainsCustomAttribute<StoragePropertyAttribute>())
                        .Select(propInfo => propInfo.PairWithKey(propInfo.GetCustomAttribute<StoragePropertyAttribute>()))
                        .ToArray();
                    var rowKeyProperty = attributesKvp
                        .First<KeyValuePair<StoragePropertyAttribute, MemberInfo>, MemberInfo>(
                            (attr, next) =>
                            {
                                if (attr.Key.IsRowKey)
                                    return attr.Value;
                                return next();
                            },
                            () => throw new Exception("Entity does not contain row key attribute"));

                    var rowKeyValue = rowKeyProperty.GetValue(entity);
                    var rowKeyString = ((Guid)rowKeyValue).AsRowKey();
                    return rowKeyString;
                }
                set
                {
                    var x = value.GetType();
                }
            }

            public string PartitionKey
            {
                get
                {
                    var partitionKey = RowKey.GeneratePartitionKey();// partitionKey;
                    return partitionKey;
                }
                set
                {
                    var x = value.GetType();

                }
            }

            public DateTimeOffset Timestamp { get; set; }

            public string ETag { get; set; }

            public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
            {
                throw new NotImplementedException();
            }

            private KeyValuePair<string, EntityProperty>[] ConvertValue(string propertyName, object value)
            {
                KeyValuePair<string, EntityProperty>[] ReturnProperty(EntityProperty property)
                {
                    var kvp = property.PairWithKey(propertyName);
                    return (kvp.AsArray());
                }

                if (value.IsDefaultOrNull())
                    return new KeyValuePair<string, EntityProperty>[] { };

                if (typeof(string).IsInstanceOfType(value))
                {
                    var stringValue = value as string;
                    var ep = new EntityProperty(stringValue);
                    return ReturnProperty(ep);
                }
                if (typeof(bool).IsInstanceOfType(value))
                {
                    var boolValue = (bool)value;
                    var ep = new EntityProperty(boolValue);
                    return ReturnProperty(ep);
                }
                if (typeof(Guid).IsInstanceOfType(value))
                {
                    var guidValue = (Guid)value;
                    var ep = new EntityProperty(guidValue);
                    return ReturnProperty(ep);
                }
                if (typeof(Guid[]).IsInstanceOfType(value))
                {
                    var guidsValue = value as Guid[];
                    var ep = new EntityProperty(guidsValue.ToByteArrayOfGuids());
                    return ReturnProperty(ep);
                }
                if (typeof(Guid[]).IsInstanceOfType(value))
                {
                    var guidsValue = value as Guid[];
                    var ep = new EntityProperty(guidsValue.ToByteArrayOfGuids());
                    return ReturnProperty(ep);
                }
                if (typeof(Type).IsInstanceOfType(value))
                {
                    var typeValue = (value as Type);
                    var typeString = typeValue.AssemblyQualifiedName;
                    var ep = new EntityProperty(typeString);
                    return ReturnProperty(ep);
                }
                if (typeof(IReferenceable).IsAssignableFrom(value.GetType()))
                {
                    var refValue = value as IReferenceable;
                    var guidValue = refValue.id;
                    var ep = new EntityProperty(guidValue);
                    return ReturnProperty(ep);
                }
                if (typeof(IReferences).IsAssignableFrom(value.GetType()))
                {
                    var refValue = value as IReferences;
                    var guidValues = refValue.ids;
                    var ep = new EntityProperty(guidValues.ToByteArrayOfGuids());
                    return ReturnProperty(ep);
                }
                if (value.GetType().IsSubClassOfGeneric(typeof(IDictionary<,>)))
                {
                    var valueType = value.GetType();
                    var keysType = valueType.GenericTypeArguments[0];
                    var valuesType = valueType.GenericTypeArguments[1];
                    var kvps = value
                        .DictionaryKeyValuePairs();
                    var keyValues = kvps
                        .SelectKeys()
                        .Cast(keysType);
                    var valueValues = kvps
                        .SelectValues()
                        .Cast(valuesType);
                    return ConvertValue($"{propertyName}__keys", keyValues)
                        .Concat(ConvertValue($"{propertyName}__values", valueValues))
                        .ToArray();
                }
                if (value.GetType().IsArray)
                {
                    var valueType = ((System.Collections.IEnumerable)value)
                        .Cast<object>()
                        .ToArray()
                        .Select(
                            item =>
                            {
                                var epValues = ConvertValue(".", item);
                                if (!epValues.Any())
                                    throw new Exception($"Cannot serialize array of `{item.GetType().FullName}`.");
                                var epValue = epValues.First().Value;
                                switch(epValue.PropertyType)
                                {
                                    case EdmType.Binary:
                                        return epValue.BinaryValue;
                                    case EdmType.Boolean:
                                        return epValue.BooleanValue.Value?
                                            new byte[] { 1 } : new byte[] { 0 };
                                    case EdmType.Double:
                                        return BitConverter.GetBytes(epValue.DoubleValue.Value);
                                    case EdmType.Guid:
                                        return epValue.GuidValue.Value.ToByteArray();
                                    case EdmType.Int32:
                                        return BitConverter.GetBytes(epValue.Int32Value.Value);
                                    case EdmType.Int64:
                                        return BitConverter.GetBytes(epValue.Int32Value.Value);
                                    case EdmType.String:
                                        return epValue.StringValue.GetBytes();
                                }
                                return new byte[] { };
                            })
                        .ToByteArray();
                    return new EntityProperty(valueType).PairWithKey(propertyName).AsArray();
                }

                return new KeyValuePair<string, EntityProperty>[] { };
            }

            public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
            {
                var valuesToStore = entityType
                    .GetMembers()
                    .Where(propInfo => propInfo.ContainsAttributeInterface<IPersistInAzureStorageTables>())
                    .SelectMany(
                        (propInfo) =>
                        {
                            var attrs = propInfo.GetAttributesInterface<IPersistInAzureStorageTables>();
                            if (attrs.Length > 1)
                            {
                                var propIdentifier = $"{propInfo.DeclaringType.FullName}__{propInfo.Name}";
                                var attributesInConflict = attrs.Select(a => a.GetType().FullName).Join(",");
                                throw new Exception($"{propIdentifier} has multiple IPersistInAzureStorageTables attributes:{attributesInConflict}.");
                            }
                            var attr = attrs.First() as IPersistInAzureStorageTables;
                            var value = propInfo.GetValue(this.entity);
                            return attr.ConvertValue(value, propInfo);
                        })
                    .ToDictionary();
                return valuesToStore;
            }

            internal static ITableEntity Create<TEntity>(TEntity entity)
            {
                var creatableEntity = new CreatableEntity();
                creatableEntity.entity = entity;
                creatableEntity.entityType = typeof(TEntity);
                return creatableEntity;
            }
        }


        public async Task<TResult> CreateAsync<TEntity, TResult>(TEntity entity,
            Func<Guid, TResult> onSuccess,
            Func<TResult> onAlreadyExists,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
           AzureStorageDriver.RetryDelegate onTimeout = default(AzureStorageDriver.RetryDelegate))
        {
            var tableEntity = CreatableEntity.Create(entity);
            while (true)
            {
                var tableName = typeof(TEntity).GetCustomAttribute<TableEntityAttribute, string>(
                    attr => attr.TableName,
                    () => typeof(TEntity).Name);
                var table = TableClient.GetTableReference(tableName);
                try
                {
                    var insert = TableOperation.Insert(tableEntity);
                    var tableResult = await table.ExecuteAsync(insert);
                    return onSuccess(Guid.Parse(tableEntity.RowKey));
                }
                catch (StorageException ex)
                {
                    if (ex.IsProblemTableDoesNotExist())
                    {
                        try
                        {
                            await table.CreateIfNotExistsAsync();
                        }
                        catch (StorageException createEx)
                        {
                            // Catch bug with azure storage table client library where
                            // if two resources attempt to create the table at the same
                            // time one gets a precondtion failed error.
                            System.Threading.Thread.Sleep(1000);
                            createEx.ToString();
                        }
                        continue;
                    }

                    if (ex.IsProblemResourceAlreadyExists())
                        return onAlreadyExists();

                    if (ex.IsProblemTimeout())
                    {
                        TResult result = default(TResult);
                        if (default(AzureStorageDriver.RetryDelegate) == onTimeout)
                            onTimeout = AzureStorageDriver.GetRetryDelegate();
                        await onTimeout(ex.RequestInformation.HttpStatusCode, ex,
                            async () =>
                            {
                                result = await CreateAsync(entity, onSuccess, onAlreadyExists, onFailure, onTimeout);
                            });
                        return result;
                    }

                    if (ex.InnerException is System.Net.WebException)
                    {
                        try
                        {
                            var innerException = ex.InnerException as System.Net.WebException;
                            var responseContentStream = innerException.Response.GetResponseStream();
                            var responseContentBytes = responseContentStream.ToBytes();
                            var responseString = responseContentBytes.ToText();
                            throw new Exception(responseString);
                        }
                        catch (Exception)
                        {
                        }
                        throw;
                    }
                    //if(ex.InnerException.Response)

                    throw;
                }
                catch (Exception general_ex)
                {
                    var message = general_ex;
                    throw;
                }

            }
        }


        public async Task<TResult> FindByIdAsync<TEntity, TResult>(
                string rowKey, string partitionKey,
            Func<TEntity, TResult> onSuccess,
            Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            AzureStorageDriver.RetryDelegate onTimeout =
                default(AzureStorageDriver.RetryDelegate))
        {
            var tableName = typeof(TEntity).GetCustomAttribute<TableEntityAttribute, string>(
                attr => attr.TableName,
                () => typeof(TEntity).Name);
            var propertyNames = typeof(TEntity)
                .GetProperties()
                .Where(propInfo => propInfo.ContainsCustomAttribute<StoragePropertyAttribute>())
                .Select(propInfo => propInfo.GetCustomAttribute<StoragePropertyAttribute>().Name)
                .Join(",");

            var table = TableClient.GetTableReference(tableName);
            var operation = TableOperation.Retrieve(partitionKey, rowKey,
                (string partitionKeyEntity, string rowKeyEntity, DateTimeOffset timestamp, IDictionary<string, EntityProperty> properties, string etag) =>
                {
                    var entityInitial = Activator.CreateInstance<TEntity>();
                    var entityPopulated = typeof(TEntity)
                        .GetMembers()
                        .Where(propInfo => propInfo.ContainsCustomAttribute<StoragePropertyAttribute>())
                        .Aggregate(entityInitial,
                            (entity, propInfo) =>
                            {
                                var attr = propInfo.GetCustomAttribute<StoragePropertyAttribute>();
                                var key = attr.Name;
                                if (!properties.ContainsKey(key))
                                    return entity;
                                var value = properties[key];
                                var type = propInfo.GetMemberType();
                                TEntity Select(object convertedValue)
                                {
                                    propInfo.SetValue(ref entity, convertedValue);
                                    return entity;
                                }

                                if (type.IsAssignableFrom(typeof(Guid)))
                                    return Select(value.GuidValue);

                                if (type.IsAssignableFrom(typeof(Guid[])))
                                {
                                    var guidsValue = value.BinaryValue.ToGuidsFromByteArray();
                                    return Select(guidsValue);
                                }

                                if (type.IsSubClassOfGeneric(typeof(IRef<>)))
                                {
                                    var guidValue = value.GuidValue;
                                    var resourceType = type.GenericTypeArguments.First();
                                    var instantiatableType = typeof(EastFive.Azure.Persistence.Ref<>).MakeGenericType(resourceType);
                                    var instance = Activator.CreateInstance(instantiatableType, new object[] { guidValue });
                                    return Select(instance);
                                }

                                if (type.IsAssignableFrom(typeof(string)))
                                    return Select(value.StringValue);

                                return entity;
                            });

                    return entityPopulated;
                });
            try
            {
                var result = await table.ExecuteAsync(operation);
                if (404 == result.HttpStatusCode)
                    return onNotFound();
                return onSuccess((TEntity)result.Result);
            }
            catch (StorageException se)
            {
                if (se.IsProblemTableDoesNotExist())
                    return onNotFound();
                if (se.IsProblemTimeout())
                {
                    TResult result = default(TResult);
                    if (default(AzureStorageDriver.RetryDelegate) == onTimeout)
                        onTimeout = AzureStorageDriver.GetRetryDelegate();
                    await onTimeout(se.RequestInformation.HttpStatusCode, se,
                        async () =>
                        {
                            result = await FindByIdAsync(rowKey, partitionKey, onSuccess, onNotFound, onFailure, onTimeout);
                        });
                    return result;
                }
                throw se;
            }
            
        }

        //public async Task<TResult> FindAllAsync<TEntity, TResult>(
        //        string filter,
        //    Func<TEntity[], TResult> onSuccess,
        //    Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
        //        default(Func<ExtendedErrorInformationCodes, string, TResult>),
        //    AzureStorageDriver.RetryDelegate onTimeout =
        //        default(AzureStorageDriver.RetryDelegate))
        //{
        //    var tableName = typeof(TEntity).GetCustomAttribute<TableEntityAttribute, string>(
        //        attr => attr.TableName,
        //        () => typeof(TEntity).Name);
        //    var propertyNames = typeof(TEntity)
        //        .GetProperties()
        //        .Where(propInfo => propInfo.ContainsCustomAttribute<StoragePropertyAttribute>())
        //        .Select(propInfo => propInfo.GetCustomAttribute<StoragePropertyAttribute>().Name)
        //        .Join(",");

        //    var http = new HttpClient(
        //        new SharedKeySignatureStoreTablesMessageHandler(this.accountName, this.accountKey))
        //    {
        //        Timeout = new TimeSpan(0, 5, 0)
        //    };

        //    var filterAndParameter = filter.IsNullOrWhiteSpace(
        //        () => string.Empty,
        //        queryExpression => $"$filter={queryExpression}&");
        //    var url = $"https://{accountName}.table.core.windows.net/{tableName}()?{filterAndParameter}$select=propertyNames";
        //    // https://myaccount.table.core.windows.net/mytable()?$filter=<query-expression>&$select=<comma-separated-property-names>
        //    var response = await http.GetAsync(url);

        //    return onSuccess(default(TEntity).AsArray());
        //}
        

        private class DeletableEntity : ITableEntity
        {
            private Guid rowKeyValue;

            public string RowKey
            {
                get
                {
                    var rowKeyString = ((Guid)rowKeyValue).AsRowKey();
                    return rowKeyString;
                }
                set
                {
                    var x = value.GetType();
                }
            }

            public string PartitionKey
            {
                get
                {
                    var partitionKey = RowKey.GeneratePartitionKey();// partitionKey;
                    return partitionKey;
                }
                set
                {
                    var x = value.GetType();

                }
            }

            public DateTimeOffset Timestamp { get; set; }

            public string ETag
            {
                get
                {
                    return "*";
                }
                set
                {
                }
            }

            public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
            {
                throw new NotImplementedException();
            }

            public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
            {
                throw new NotImplementedException();
            }

            internal static ITableEntity Delete(Guid rowKey)
            {
                var creatableEntity = new DeletableEntity();
                creatableEntity.rowKeyValue = rowKey;
                return creatableEntity;
            }
        }

        public async Task<TResult> DeleteByIdAsync<TData, TResult>(Guid documentId,
            Func<TResult> success,
            Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            AzureStorageDriver.RetryDelegate onTimeout = default(AzureStorageDriver.RetryDelegate))
        {
            var table = GetTable<TData>();
            if (default(CloudTable) == table)
                return onNotFound();

            var document = DeletableEntity.Delete(documentId);
            var delete = TableOperation.Delete(document);
            try
            {
                await table.ExecuteAsync(delete);
                return success();
            }
            catch (StorageException se)
            {
                return await se.ParseStorageException(
                    async (errorCode, errorMessage) =>
                    {
                        switch (errorCode)
                        {
                            case ExtendedErrorInformationCodes.Timeout:
                                {
                                    var timeoutResult = default(TResult);
                                    if (default(AzureStorageDriver.RetryDelegate) == onTimeout)
                                        onTimeout = AzureStorageDriver.GetRetryDelegate();
                                    await onTimeout(se.RequestInformation.HttpStatusCode, se,
                                        async () =>
                                        {
                                            timeoutResult = await DeleteByIdAsync<TData, TResult>(documentId, success, onNotFound, onFailure, onTimeout);
                                        });
                                    return timeoutResult;
                                }
                            case ExtendedErrorInformationCodes.TableNotFound:
                            case ExtendedErrorInformationCodes.TableBeingDeleted:
                                {
                                    return onNotFound();
                                }
                            default:
                                {
                                    if (se.IsProblemDoesNotExist())
                                        return onNotFound();
                                    if (onFailure.IsDefaultOrNull())
                                        throw se;
                                    return onFailure(errorCode, errorMessage);
                                }
                        }
                    },
                    () =>
                    {
                        throw se;
                    });
            }
        }


    }
}
