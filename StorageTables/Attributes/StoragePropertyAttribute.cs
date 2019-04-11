using BlackBarLabs.Persistence.Azure;
using EastFive.Collections.Generic;
using EastFive.Extensions;
using EastFive.Reflection;
using EastFive.Serialization;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using BlackBarLabs.Extensions;
using EastFive.Linq.Expressions;
using EastFive.Persistence.Azure.StorageTables;
using BlackBarLabs.Persistence.Azure.StorageTables;
using EastFive.Linq;

namespace EastFive.Persistence
{
    public interface IPersistInAzureStorageTables 
    {
        // string Name { get; }

        KeyValuePair<string, EntityProperty>[] ConvertValue(object value, MemberInfo memberInfo);
        object GetMemberValue(MemberInfo memberInfo, IDictionary<string, EntityProperty> values);

        string GetTablePropertyName(MemberInfo member);
    }

    public interface IPersistInEntityProperty
    {
        EntityProperty ConvertValue(object value);

        object GetMemberValue(EntityProperty value);
    }

    public interface IModifyAzureStorageTablePartitionKey
    {
        string GeneratePartitionKey(string rowKey, object value, MemberInfo memberInfo);
        EntityType ParsePartitionKey<EntityType>(EntityType entity, string value, MemberInfo memberInfo);
    }

    public interface IModifyAzureStorageTableRowKey
    {
        string GenerateRowKey(object value, MemberInfo memberInfo);

        EntityType ParseRowKey<EntityType>(EntityType entity, string value, MemberInfo memberInfo);
    }

    public interface IModifyAzureStorageTableLastModified
    {
        DateTimeOffset GenerateLastModified(object value, MemberInfo memberInfo);

        EntityType ParseLastModfied<EntityType>(EntityType entity, DateTimeOffset value, MemberInfo memberInfo);
    }

    public class StorageAttribute : Attribute,
        IPersistInAzureStorageTables
    {
        public string Name { get; set; }

        public string GetTablePropertyName(MemberInfo member)
        {
            var tablePropertyName = this.Name;
            if (tablePropertyName.IsNullOrWhiteSpace())
                return member.Name;
            return tablePropertyName;
        }

        public bool IsRowKey { get; set; }
        public Type ReferenceType { get; set; }
        public string ReferenceProperty { get; set; }

        public virtual KeyValuePair<string, EntityProperty>[] ConvertValue(object value, MemberInfo memberInfo)
        {
            var propertyName = this.GetTablePropertyName(memberInfo);

            var valueType = memberInfo.GetPropertyOrFieldType();
            return CastValue(valueType, value, propertyName);
        }

        public KeyValuePair<string, EntityProperty>[] CastValue(Type typeOfValue, object value, string propertyName)
        {
            if (value.IsDefaultOrNull())
                return new KeyValuePair<string, EntityProperty>[] { };

            if (IsMultiProperty(typeOfValue))
            {
                return CastEntityProperties(value, typeOfValue,
                   (propertyKvps) =>
                   {
                       var kvps = propertyKvps
                           .Select(
                               propertyKvp =>
                               {
                                   var nestedPropertyName = propertyKvp.Key;
                                   var compositePropertyName = $"{propertyName}__{nestedPropertyName}";
                                   return compositePropertyName.PairWithValue(propertyKvp.Value);
                               })
                           .ToArray();
                       return kvps;
                   },
                   () => new KeyValuePair<string, EntityProperty>[] { });
            }

            return value.CastEntityProperty(typeOfValue, 
                (property) =>
                {
                    var kvp = property.PairWithKey(propertyName);
                    return kvp.AsArray();
                },
                () => new KeyValuePair<string, EntityProperty>[] { });
        }

        /// <summary>
        /// Will this type be stored in a single EntityProperty or across multiple entity properties.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public virtual bool IsMultiProperty(Type type)
        {
            if (type.IsSubClassOfGeneric(typeof(IDictionary<,>)))
                return true;
            if (type.IsSubClassOfGeneric(typeof(KeyValuePair<,>)))
                return true;

            // Look for a custom type
            var storageMembers = type.GetPersistenceAttributes();
            if (storageMembers.Any())
                return true;

            if (type.IsArray)
            {
                var arrayType = type.GetElementType();
                if (arrayType.GetPersistenceAttributes().Any())
                    return true;
                if (arrayType.IsSubClassOfGeneric(typeof(KeyValuePair<,>)))
                    return true;
            }

            return false;
        }

        public virtual EntityProperty CastEntityPropertyEmpty(Type valueType)
        {
            if (valueType.IsAssignableFrom(typeof(string)))
                return new EntityProperty(default(string));
            if (valueType.IsAssignableFrom(typeof(bool)))
                return new EntityProperty(default(bool));
            if (valueType.IsAssignableFrom(typeof(Guid)))
                return new EntityProperty(default(Guid));
            if (valueType.IsAssignableFrom(typeof(Type)))
                return new EntityProperty(default(string));
            if (valueType.IsAssignableFrom(typeof(IReferenceable)))
                return new EntityProperty(default(Guid));
            if (valueType.IsAssignableFrom(typeof(IReferenceableOptional)))
                return new EntityProperty(default(Guid?));
            return new EntityProperty(default(byte[]));
        }

        public virtual object GetMemberValue(MemberInfo memberInfo, IDictionary<string, EntityProperty> values)
        {
            var propertyName = this.GetTablePropertyName(memberInfo);

            var type = memberInfo.GetPropertyOrFieldType();

            return GetMemberValue(type, propertyName, values,
                (convertedValue) => convertedValue,
                    () =>
                    {
                        var exceptionText = $"Could not deserialize value for {memberInfo.DeclaringType.FullName}..{memberInfo.Name}[{type.FullName}]" +
                            $"Please override StoragePropertyAttribute's BindEntityProperties for type:{type.FullName}";
                        throw new Exception(exceptionText);
                    });
        }

        public virtual TResult GetMemberValue<TResult>(Type type, string propertyName, IDictionary<string, EntityProperty> values,
            Func<object, TResult> onBound,
            Func<TResult> onFailureToBind)
        {
            if (IsMultiProperty(type))
                return BindEntityProperties(propertyName, type, values,
                    onBound,
                    onFailureToBind);

            if (!values.ContainsKey(propertyName))
                return BindEmptyEntityProperty(type,
                    onBound,
                    onFailureToBind);

            var value = values[propertyName];
            return value.Bind(type,
                onBound,
                onFailureToBind);

        }

        #region Single Value Serialization

        #endregion

        #region Array serialization

        public static TResult BindSingleEntityValueToArray<TResult>(Type arrayType, EntityProperty value,
            Func<object, TResult> onBound,
            Func<TResult> onFailedToBind)
        {
            #region Refs

            object ComposeFromBase<TBase>(Type composedType, Type genericCompositionType,
                Func<Type, TBase, object> instantiate)
            {
                return BindSingleEntityValueToArray(typeof(TBase), value,
                    objects =>
                    {
                        var guids = (TBase[])objects;
                        var resourceType = arrayType.GenericTypeArguments.First();
                        var instantiatableType = genericCompositionType.MakeGenericType(resourceType);

                        var refs = guids
                            .Select(
                                guidValue =>
                                {
                                    var instance = instantiate(instantiatableType, guidValue);
                                    // Activator.CreateInstance(instantiatableType, new object[] { guidValue });
                                    return instance;
                                })
                           .ToArray();
                        var typedRefs = refs.CastArray(arrayType);
                        return typedRefs;
                    },
                    () => throw new Exception("BindArray failed to bind to Guids?"));
            }

            if (arrayType.IsSubClassOfGeneric(typeof(IRef<>)))
            {
                var values = ComposeFromBase<Guid>(typeof(IRef<>), typeof(EastFive.Azure.Persistence.Ref<>),
                    (instantiatableType, guidValue) => Activator.CreateInstance(instantiatableType, new object[] { guidValue }));
                return onBound(values);
            }

            if (arrayType.IsSubClassOfGeneric(typeof(IRefObj<>)))
            {
                var values = ComposeFromBase<Guid>(typeof(IRefObj<>), typeof(EastFive.Azure.Persistence.RefObj<>),
                    (instantiatableType, guidValue) => Activator.CreateInstance(instantiatableType, new object[] { guidValue }));
                return onBound(values);
            }


            object ComposeOptionalFromBase<TBase>(Type composedType, Type genericCompositionType, Type optionalBaseType)
            {
                var values = ComposeFromBase<Guid?>(composedType, genericCompositionType,
                    (instantiatableType, guidValueMaybe) =>
                    {
                        if (!guidValueMaybe.HasValue)
                            return Activator.CreateInstance(instantiatableType, new object[] { });
                        var guidValue = guidValueMaybe.Value;
                        var resourceType = arrayType.GenericTypeArguments.First();
                        var instantiatableRefType = optionalBaseType.MakeGenericType(resourceType);
                        var refValue = Activator.CreateInstance(instantiatableRefType, new object[] { guidValue });
                        var result = Activator.CreateInstance(instantiatableType, new object[] { refValue });
                        return result;
                    });
                return values;
            }

            if (arrayType.IsSubClassOfGeneric(typeof(IRefOptional<>)))
            {
                var values = ComposeOptionalFromBase<Guid?>(typeof(IRefOptional<>),
                    typeof(EastFive.RefOptional<>), typeof(EastFive.Azure.Persistence.Ref<>));
                return onBound(values);
            }

            if (arrayType.IsSubClassOfGeneric(typeof(IRefObjOptional<>)))
            {
                var values = ComposeOptionalFromBase<Guid?>(typeof(IRefObjOptional<>),
                    typeof(EastFive.RefObjOptional<>), typeof(EastFive.Azure.Persistence.RefObj<>));
                return onBound(values);
            }

            #endregion

            if (arrayType.IsArray)
            {
                var arrayElementType = arrayType.GetElementType();
                var values = value.BinaryValue
                    .FromByteArray()
                    .Select(
                        bytes =>
                        {
                            var ep = new EntityProperty(bytes);
                            var arrayValues = ep.BindSingleValueToArray<object>(arrayElementType,
                                v => v,
                                () => Array.CreateInstance(arrayElementType, 0));
                            // var arrayValues = bytes.FromEdmTypedByteArray(arrayElementType);
                            return arrayValues;
                        })
                    .ToArray();
                //var values = value.BinaryValue.FromEdmTypedByteArray(arrayElementType);
                return onBound(values);
            }

            if (typeof(string) == arrayType)
            {
                var values = value.BinaryValue.ToStringNullOrEmptysFromUTF8ByteArray();
                return onBound(values);
            }
            return arrayType.IsNullable(
                nulledType =>
                {
                    if (typeof(Guid) == nulledType)
                    {
                        var values = value.BinaryValue.ToNullablesFromByteArray<Guid>(
                            (byteArray) =>
                            {
                                if (byteArray.Length == 16)
                                    return new Guid(byteArray);
                                return default(Guid);
                            });
                        return onBound(values);
                    }
                    if (typeof(DateTime) == nulledType)
                    {
                        var values = value.BinaryValue.ToNullableDateTimesFromByteArray();
                        return onBound(values);
                    }
                    var arrayOfObj = value.BinaryValue.FromEdmTypedByteArray(arrayType);
                    var arrayOfType = arrayOfObj.CastArray(arrayType);
                    return onBound(arrayOfType);

                    throw new Exception($"Cannot serialize a nullable array of `{nulledType.FullName}`.");
                },
                () =>
                {
                    if (typeof(Guid) == arrayType)
                    {
                        var values = value.BinaryValue.ToGuidsFromByteArray();
                        return onBound(values);
                    }
                    if (typeof(byte) == arrayType)
                    {
                        return onBound(value.BinaryValue);
                    }
                    if (typeof(bool) == arrayType)
                    {
                        var boolArray = value.BinaryValue
                            .Select(b => b != 0)
                            .ToArray();
                        return onBound(boolArray);
                    }
                    if (typeof(DateTime) == arrayType)
                    {
                        var values = value.BinaryValue.ToDateTimesFromByteArray();
                        return onBound(values);
                    }
                    if (typeof(double) == arrayType)
                    {
                        var values = value.BinaryValue.ToDoublesFromByteArray();
                        return onBound(values);
                    }
                    if (typeof(decimal) == arrayType)
                    {
                        var values = value.BinaryValue.ToDecimalsFromByteArray();
                        return onBound(values);
                    }
                    if (typeof(int) == arrayType)
                    {
                        var values = value.BinaryValue.ToIntsFromByteArray();
                        return onBound(values);
                    }
                    if (typeof(long) == arrayType)
                    {
                        var values = value.BinaryValue.ToLongsFromByteArray();
                        return onBound(values);
                    }
                    if (arrayType.IsEnum)
                    {
                        var values = value.BinaryValue.ToEnumsFromByteArray(arrayType);
                        return onBound(values);
                    }
                    if (typeof(object) == arrayType)
                    {
                        var values = value.BinaryValue.FromEdmTypedByteArray(arrayType);
                        return onBound(values);
                    }
                    throw new Exception($"Cannot serialize array of `{arrayType.FullName}`.");
                });

        }

        #endregion

        #region Multi-entity serialization

        protected virtual TResult BindEntityProperties<TResult>(string propertyName, Type type,
                IDictionary<string, EntityProperty> allValues,
            Func<object, TResult> onBound,
            Func<TResult> onFailedToBind)
        {
            if (type.IsSubClassOfGeneric(typeof(IDictionary<,>)))
            {
                // TODO: Actually map values
                var keyType = type.GenericTypeArguments[0];
                var valueType = type.GenericTypeArguments[1];
                var instantiatableType = typeof(Dictionary<,>)
                    .MakeGenericType(keyType, valueType);

                var keysPropertyName = $"{propertyName}__keys";
                var valuesPropertyName = $"{propertyName}__values";

                var refOpt = (IDictionary)Activator.CreateInstance(instantiatableType, new object[] { });

                bool ContainsKeys()
                {
                    if (!allValues.ContainsKey(keysPropertyName))
                        return false;
                    if (!allValues.ContainsKey(valuesPropertyName))
                        return false;
                    return true;
                }
                if (!ContainsKeys())
                {
                    // return empty set
                    return onBound(refOpt);
                }

                var keyArrayType = Array.CreateInstance(keyType, 0).GetType();
                var valueArrayType = Array.CreateInstance(valueType, 0).GetType();
                return allValues[keysPropertyName].Bind(keyArrayType,
                    (keyValues) => allValues[valuesPropertyName].Bind(valueArrayType,
                        (propertyValues) =>
                        {
                            var keyEnumerable = keyValues as System.Collections.IEnumerable;
                            var keyEnumerator = keyEnumerable.GetEnumerator();
                            var propertyEnumerable = propertyValues as System.Collections.IEnumerable;
                            var propertyEnumerator = propertyEnumerable.GetEnumerator();

                            //IDictionary<int, string> x;
                            //x.Add(1, "");
                            //var addMethod = typeof(Dictionary<,>)
                            //    .GetMethods()
                            //    .Where(method => method.Name == "Add")
                            //    .Where(method => method.GetParameters().Length == 2)
                            //    .Single();

                            while (keyEnumerator.MoveNext())
                            {
                                if (!propertyEnumerator.MoveNext())
                                    return onBound(refOpt);
                                var keyValue = keyEnumerator.Current;
                                var propertyValue = propertyEnumerator.Current;
                                refOpt.Add(keyValue, propertyValue);
                            }
                            return onBound(refOpt);

                        },
                        onFailedToBind),
                    onFailedToBind);
            }

            if (type.IsSubClassOfGeneric(typeof(KeyValuePair<,>)))
            {
                var keyType = type.GenericTypeArguments[0];
                var valueType = type.GenericTypeArguments[1];
                var instantiatableType = typeof(KeyValuePair<,>)
                    .MakeGenericType(keyType, valueType);

                var keyPropertyName = $"{propertyName}__key";
                var valuePropertyName = $"{propertyName}__value";

                bool ContainsKeys()
                {
                    if (!allValues.ContainsKey(keyPropertyName))
                        return false;
                    if (!allValues.ContainsKey(valuePropertyName))
                        return false;
                    return true;
                }
                if (!ContainsKeys())
                {
                    // return empty set
                    return onBound(instantiatableType.GetDefault());
                }

                return allValues[keyPropertyName].Bind(keyType,
                    (keyValue) =>
                    {
                        return allValues[valuePropertyName].Bind(keyType,
                            (valueValue) =>
                            {
                                var refOpt = Activator.CreateInstance(instantiatableType, new object[] { keyValue, valueValue });
                                return onBound(refOpt);
                            },
                            onFailedToBind);
                    },
                    onFailedToBind);
            }

            if (type.IsArray)
            {
                var arrayType = type.GetElementType();
                var storageMembersArray = arrayType.GetPersistenceAttributes();
                if (storageMembersArray.Any())
                {
                    var arrayEps = BindArrayEntityProperties(propertyName, arrayType,
                        storageMembersArray, allValues);
                    return onBound(arrayEps);
                }
                if(arrayType.IsSubClassOfGeneric(typeof(KeyValuePair<,>)))
                {
                    return BindArrayKvpEntityProperties(propertyName, arrayType, allValues,
                        onBound,
                        onFailedToBind);
                }
            }

            var storageMembers = type.GetPersistenceAttributes();
            if (storageMembers.Any())
            {
                var value = Activator.CreateInstance(type);
                foreach (var storageMemberKvp in storageMembers)
                {
                    var attr = storageMemberKvp.Value.First();
                    var member = storageMemberKvp.Key;
                    var objPropName = attr.GetTablePropertyName(member);
                    var propName = $"{propertyName}__{objPropName}";
                    if (!allValues.ContainsKey(propName))
                        continue;

                    var entityProperties = allValues[propName].PairWithKey(objPropName)
                        .AsArray()
                        .ToDictionary();
                    var propertyValue = attr.GetMemberValue(member, entityProperties);
                    member.SetValue(ref value, propertyValue);
                }

                return onBound(value);
            }

            return onFailedToBind();
        }

        public virtual TResult CastEntityProperties<TResult>(object value, Type valueType,
            Func<KeyValuePair<string, EntityProperty>[], TResult> onValues,
            Func<TResult> onNoCast)
        {
            if (valueType.IsSubClassOfGeneric(typeof(IDictionary<,>)))
            {
                var keysType = valueType.GenericTypeArguments[0];
                var valuesType = valueType.GenericTypeArguments[1];
                var kvps = value
                    .DictionaryKeyValuePairs();
                var keyValues = kvps
                    .SelectKeys()
                    .CastArray(keysType);
                var valueValues = kvps
                    .SelectValues()
                    .CastArray(valuesType);

                var keysArrayType = keysType.MakeArrayType();
                var keyEntityProperties = CastValue(keysArrayType, keyValues, "keys");
                var valuesArrayType = valuesType.MakeArrayType();
                var valueEntityProperties = CastValue(valuesArrayType, valueValues, "values");

                var entityProperties = keyEntityProperties.Concat(valueEntityProperties).ToArray();
                return onValues(entityProperties);
            }

            if (valueType.IsSubClassOfGeneric(typeof(KeyValuePair<,>)))
            {
                var kvpKeyType = valueType.GenericTypeArguments[0];
                var kvpValueType = valueType.GenericTypeArguments[1];
                var keyValue = valueType.GetProperty("Key").GetValue(value);
                var valueValue = valueType.GetProperty("Value").GetValue(value);

                var keyEntityProperties = CastValue(kvpKeyType, keyValue, "key");
                var valueEntityProperties = CastValue(kvpKeyType, keyValue, "value");

                var entityProperties = keyEntityProperties
                    .Concat(valueEntityProperties)
                    .ToArray();
                return onValues(entityProperties);
            }

            if (valueType.IsArray)
            {
                var arrayType = valueType.GetElementType();
                var peristenceAttrs = arrayType.GetPersistenceAttributes();
                if (peristenceAttrs.Any())
                {
                    var epsArray = CastArrayEntityProperties(value, peristenceAttrs);
                    return onValues(epsArray);
                }
                if (arrayType.IsSubClassOfGeneric(typeof(KeyValuePair<,>)))
                {
                    return CastArrayKvpEntityProperties(value, arrayType,
                        onValues,
                        onNoCast);
                }
            }

            var storageMembers = valueType.GetPersistenceAttributes();
            if (storageMembers.Any())
            {
                var storageArrays = storageMembers
                    .Select(
                        storageMemberKvp =>
                        {
                            var attr = storageMemberKvp.Value.First();
                            var member = storageMemberKvp.Key;
                            var propName = attr.GetTablePropertyName(member);
                            var memberType = member.GetPropertyOrFieldType();

                            var v = member.GetValue(value);
                            var epValue = v.CastEntityProperty(memberType,
                                ep => ep,
                                () => new EntityProperty(new byte[] { }));

                            return epValue.PairWithKey(propName);
                        })
                    .ToArray();
                return onValues(storageArrays);
            }

            return onNoCast();
        }

        protected TResult BindArrayKvpEntityProperties<TResult>(string propertyName, Type arrayType,
                IDictionary<string, EntityProperty> allValues,
            Func<object, TResult> onBound,
            Func<TResult> onFailedToBind)
        {
            var keyType = arrayType.GenericTypeArguments[0];
            var valueType = arrayType.GenericTypeArguments[1];
            return GetMemberValue(keyType.MakeArrayType(),
                    $"{propertyName}__keys", allValues,
                keyValuesObj =>
                {
                    var keyValues = keyValuesObj.ObjectToEnumerable();
                    return GetMemberValue(valueType.MakeArrayType(),
                            $"{propertyName}__values", allValues,
                        valueValuesObj =>
                        {
                            var propertyValues = valueValuesObj.ObjectToEnumerable();

                            var keyEnumerator = keyValues.GetEnumerator();
                            var propertyEnumerator = propertyValues.GetEnumerator();
                            var refOpt = new System.Collections.ArrayList();
                            while (keyEnumerator.MoveNext())
                            {
                                if (!propertyEnumerator.MoveNext())
                                    return onBound(refOpt.ToArray().CastArray(arrayType));
                                var keyValue = keyEnumerator.Current;
                                var propertyValue = propertyEnumerator.Current;
                                var kvp = Activator.CreateInstance(arrayType, new object[] { keyValue, propertyValue });
                                refOpt.Add(kvp);
                            }
                            return onBound(refOpt.ToArray().CastArray(arrayType));
                        },
                        () => onBound(Array.CreateInstance(arrayType, 0)));
                },
                () => onBound(Array.CreateInstance(arrayType, 0)));
        }

        protected TResult CastArrayKvpEntityProperties<TResult>(object value, Type arrayType,
            Func<KeyValuePair<string, EntityProperty>[], TResult> onValues,
            Func<TResult> onNoCast)
        {
            var enumeration = value
                .ObjectToEnumerable();

            var keyProp = arrayType.GetProperty("Key");
            var keysType = arrayType.GenericTypeArguments[0];
            var keyValues = enumeration
                .Select(obj => keyProp.GetValue(obj))
                .CastArray(keysType);

            var valueProp = arrayType.GetProperty("Value");
            var valuesType = arrayType.GenericTypeArguments[1];
            var valueValues = enumeration
                .Select(obj => valueProp.GetValue(obj))
                .CastArray(valuesType);

            var keysArrayType = keysType.MakeArrayType();
            var keyEntityProperties = CastValue(keysArrayType, keyValues, "keys");
            var valuesArrayType = valuesType.MakeArrayType();
            var valueEntityProperties = CastValue(valuesArrayType, valueValues, "values");

            var entityProperties = keyEntityProperties.Concat(valueEntityProperties).ToArray();
            return onValues(entityProperties);
        }

        protected object BindArrayEntityProperties(string propertyName, Type arrayType,
            IEnumerable<KeyValuePair<MemberInfo, IPersistInAzureStorageTables[]>> storageMembers,
            IDictionary<string, EntityProperty> allValues)
        {
            var entityProperties = allValues
                .Where(kvp => kvp.Key.StartsWith(propertyName + "__"))
                .Select(kvp => kvp.Value.PairWithKey(kvp.Key.Substring(propertyName.Length + 2)))
                .ToDictionary();

            var storageArrays = storageMembers
                .Select(
                    storageMemberKvp =>
                    {
                        var attr = storageMemberKvp.Value.First();
                        var member = storageMemberKvp.Key;
                        var objPropName = attr.GetTablePropertyName(member);

                        var memberType = member.GetPropertyOrFieldType();
                        var propertyArrayEmpty = Array.CreateInstance(memberType, 0);
                        var propertyArrayType = propertyArrayEmpty.GetType();
                        var memberWithValues = this.GetMemberValue(propertyArrayType, objPropName, entityProperties,
                            v =>
                            {
                                return ((IEnumerable)v).Cast<object>().ToArray().PairWithKey(member);
                            },
                            () => (new object[] { }).PairWithKey(member));
                        return memberWithValues;
                    })
                .ToArray();

            var itemsLength = storageArrays.Any() ?
                storageArrays.Max(storageArray => storageArray.Value.Length) 
                :
                0;
            var items = Array.CreateInstance(arrayType, itemsLength);
            foreach (int i in Enumerable.Range(0, itemsLength))
            {
                var item = Activator.CreateInstance(arrayType);
                items.SetValue(item, i);
            }
            foreach (var storageArray in storageArrays)
                foreach (int i in Enumerable.Range(0, storageArray.Value.Length))
                {
                    var value = storageArray.Value[i];
                    var member = storageArray.Key;
                    var item = items.GetValue(i);
                    member.SetValue(ref item, value);
                    items.SetValue(item, i); // needed for copied structs
                }
            return items;
        }

        protected KeyValuePair<string, EntityProperty>[] CastArrayEntityProperties(object value,
            IEnumerable<KeyValuePair<MemberInfo, IPersistInAzureStorageTables[]>> storageMembers)
        {
            var items = ((IEnumerable)value)
                .Cast<object>()
                .ToArray();
            var epsArray = storageMembers
                .SelectMany(
                    storageMember =>
                    {
                        var member = storageMember.Key;
                        var converter = storageMember.Value.First();
                        var propertyNameDefault = converter.GetTablePropertyName(member);
                        var elementType = member.GetPropertyOrFieldType();
                        var arrayOfPropertyValues = items
                            .Select(
                                item =>
                                {
                                    return member.GetValue(item);
                                })
                            .CastArray(elementType);

                        var type = arrayOfPropertyValues.GetType();
                        var entityProperties = this.CastValue(type, arrayOfPropertyValues, propertyNameDefault);
                        return entityProperties;
                    })
                .ToArray();
            return epsArray;

        }

        #endregion

        protected virtual TResult BindEmptyEntityProperty<TResult>(Type type,
            Func<object, TResult> onBound,
            Func<TResult> onFailedToBind)
        {
            if (type.IsAssignableFrom(typeof(Guid)))
                return onBound(default(Guid));

            if (type.IsAssignableFrom(typeof(Guid[])))
                return onBound(new Guid[] { });

            if (type.IsSubClassOfGeneric(typeof(IRef<>)))
            {
                //var resourceType = type.GenericTypeArguments.First();
                var instance = type.GetDefault();
                return onBound(instance);
            }

            if (type.IsSubClassOfGeneric(typeof(IRefObj<>)))
            {
                //var resourceType = type.GenericTypeArguments.First();
                var instance = type.GetDefault();
                return onBound(instance);
            }

            if (type.IsSubClassOfGeneric(typeof(IRefOptional<>)))
            {
                var resourceType = type.GenericTypeArguments.First();
                var instantiatableType = typeof(EastFive.RefOptional<>)
                    .MakeGenericType(resourceType);
                var refOpt = Activator.CreateInstance(instantiatableType, new object[] { });
                return onBound(refOpt);
            }

            if (type.IsSubClassOfGeneric(typeof(IRefObjOptional<>)))
            {
                var resourceType = type.GenericTypeArguments.First();
                var instantiatableType = typeof(EastFive.RefObjOptional<>)
                    .MakeGenericType(resourceType);
                var refOpt = Activator.CreateInstance(instantiatableType, new object[] { });
                return onBound(refOpt);
            }

            if (type.IsSubClassOfGeneric(typeof(IRefs<>)))
            {
                var resourceType = type.GenericTypeArguments.First();
                var instantiatableType = typeof(EastFive.Azure.Persistence.Refs<>).MakeGenericType(resourceType);
                var instance = Activator.CreateInstance(instantiatableType, new object[] { new Guid[] { } });
                return onBound(instance);
            }

            if (type.IsSubClassOfGeneric(typeof(IRefObjs<>)))
            {
                var resourceType = type.GenericTypeArguments.First();
                var instantiatableType = typeof(RefObjs<>).MakeGenericType(resourceType);
                var instance = Activator.CreateInstance(instantiatableType, new object[] { new Guid[] { } });
                return onBound(instance);
            }

            if (type.IsAssignableFrom(typeof(string)))
                return onBound(default(string));

            if (type.IsAssignableFrom(typeof(long)))
                return onBound(default(long));

            if (type.IsAssignableFrom(typeof(int)))
                return onBound(default(int));

            if (type.IsAssignableFrom(typeof(Uri)))
                return onBound(default(Uri));

            if (type.IsSubClassOfGeneric(typeof(Nullable<>)))
            {
                var resourceType = type.GenericTypeArguments.First();
                var instantiatableType = typeof(Nullable<>).MakeGenericType(resourceType);
                var instance = Activator.CreateInstance(instantiatableType, new object[] { });
                return onBound(instance);
            }

            if (typeof(Type) == type)
                return onBound(null);

            if (type.IsArray)
            {
                var arrayInstance = Array.CreateInstance(type.GetElementType(), 0);
                return onBound(arrayInstance);
            }

            return onFailedToBind();
        }

        public TResult Bind<TResult>(IDictionary<string, EntityProperty> value, Type type,
            Func<object, TResult> onBound,
            Func<TResult> onFailedToBind)
        {
            throw new NotImplementedException();
        }

        public TResult Cast<TResult>(object value, Type valueType, Func<KeyValuePair<string, EntityProperty>[], TResult> onValue, Func<TResult> onNoCast)
        {
            throw new NotImplementedException();
        }
    }

    [Obsolete("Use StorageAttribute, RowKeyAttribute, PartitionKeyAttribute")]
    public class StoragePropertyAttribute : StorageAttribute,
        IPersistInAzureStorageTables, IModifyAzureStorageTablePartitionKey
    {
        public string GeneratePartitionKey(string rowKey, object value, MemberInfo memberInfo)
        {
            return rowKey.GeneratePartitionKey();
        }

        public EntityType ParsePartitionKey<EntityType>(EntityType entity, string value, MemberInfo memberInfo)
        {
            // discard since generated from ID
            return entity;
        }
    }
}
