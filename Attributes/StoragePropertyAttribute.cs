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

namespace EastFive.Persistence
{
    public interface IPersistInAzureStorageTables
    {
        string Name { get; }
        KeyValuePair<string, EntityProperty>[] ConvertValue(object value, MemberInfo memberInfo);
        object GetMemberValue(MemberInfo memberInfo, IDictionary<string, EntityProperty> values);
    }

    public interface IModifyAzureStorageTablePartitionKey
    {
        string GeneratePartitionKey(string rowKey, object value, MemberInfo memberInfo);
    }

    public class StoragePropertyAttribute : Attribute, 
        IPersistInAzureStorageTables, IModifyAzureStorageTablePartitionKey
    {
        public string Name { get; set; }
        public bool IsRowKey { get; set; }
        public Type ReferenceType { get; set; }
        public string ReferenceProperty { get; set; }

        public virtual KeyValuePair<string, EntityProperty>[] ConvertValue(object value, MemberInfo memberInfo)
        {
            var propertyName = this.Name.IsNullOrWhiteSpace(
                () => memberInfo.Name,
                (text) => text);

            return ConvertValue(value, propertyName);
        }

        #region Object to EntityProperty

        public KeyValuePair<string, EntityProperty>[] ConvertValue(object value, string propertyName)
        {
            if (value.IsDefaultOrNull())
                return new KeyValuePair<string, EntityProperty>[] { };

            var valueType = value.GetType();
            if(IsMultiProperty(valueType))
            {
                return CastEntityProperties(value, valueType,
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

            return CastEntityProperty(value,
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

            // Look for a custom type
            var storageMembers = type.GetPersistenceAttributes();
            if (storageMembers.Any())
                return true;

            if (type.IsArray)
                if (type.GetElementType().GetPersistenceAttributes().Any())
                    return true;

            return false;
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
                var keyEntityProperties = ConvertValue(keyValues, "keys");
                var valueEntityProperties = ConvertValue(valueValues, "values");
                var entityProperties = keyEntityProperties.Concat(valueEntityProperties).ToArray();
                return onValues(entityProperties);
            }


            if (valueType.IsArray)
            {
                var peristenceAttrs = valueType.GetElementType().GetPersistenceAttributes();
                if (peristenceAttrs.Any())
                {
                    var items = ((IEnumerable)value)
                        .Cast<object>()
                        .ToArray();
                    var epsArray = peristenceAttrs
                        .SelectMany(
                            attr =>
                            {
                                var converter = attr.Value.First();
                                var propertyNameDefault = attr.Key.Name;
                                if (converter is StoragePropertyAttribute)
                                {
                                    var spa = converter as StoragePropertyAttribute;
                                    if (spa.Name.HasBlackSpace())
                                        propertyNameDefault = spa.Name;
                                }
                                var eps = items
                                    .Select(
                                        item =>
                                        {
                                            return attr.Key.GetValue(item);
                                        })
                                    .SelectMany(
                                        itemValue =>
                                        {
                                            var values = converter.ConvertValue(itemValue, attr.Key);
                                            if (values.Any())
                                                return values;
                                            var memberType = attr.Key.GetPropertyOrFieldType();
                                            return CastEntityPropertyEmpty(memberType)
                                                .PairWithKey(propertyNameDefault)
                                                .AsArray();
                                        })
                                    .GroupBy(kvp => kvp.Key)
                                    .Select(
                                        grp =>
                                        {
                                            var values = grp.Select(kvp => kvp.Value).ToByteArrayOfEntityProperties();
                                            return new EntityProperty(values).PairWithKey(grp.Key);
                                        });
                                return eps;
                            })
                        .ToArray();
                    return onValues(epsArray);
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
                            var propName = attr.Name.HasBlackSpace() ? attr.Name : member.Name;
                            var entityProperties = ((IEnumerable)value)
                                .Cast<object>()
                                .Select(v => CastEntityProperty(v,
                                    ep => ep,
                                    () => new EntityProperty(new byte[] { })))
                                .ToArray();
                            var entityBytes = entityProperties.ToByteArrayOfEntityProperties();
                            return new EntityProperty(entityBytes).PairWithKey(propName);
                        })
                    .ToArray();
                return onValues(storageArrays);
            }

            return onNoCast();
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

        public virtual TResult CastEntityProperty<TResult>(object value,
            Func<EntityProperty, TResult> onValue,
            Func<TResult> onNoCast)
        {
            if (typeof(string).IsInstanceOfType(value))
            {
                var stringValue = value as string;
                var ep = new EntityProperty(stringValue);
                return onValue(ep);
            }
            if (typeof(bool).IsInstanceOfType(value))
            {
                var boolValue = (bool)value;
                var ep = new EntityProperty(boolValue);
                return onValue(ep);
            }
            if (typeof(float).IsInstanceOfType(value))
            {
                var floatValue = (float)value;
                var ep = new EntityProperty(floatValue);
                return onValue(ep);
            }
            if (typeof(double).IsInstanceOfType(value))
            {
                var floatValue = (double)value;
                var ep = new EntityProperty(floatValue);
                return onValue(ep);
            }
            if (typeof(int).IsInstanceOfType(value))
            {
                var intValue = (int)value;
                var ep = new EntityProperty(intValue);
                return onValue(ep);
            }
            if (typeof(long).IsInstanceOfType(value))
            {
                var longValue = (long)value;
                var ep = new EntityProperty(longValue);
                return onValue(ep);
            }
            if (typeof(DateTime).IsInstanceOfType(value))
            {
                var dateTimeValue = (DateTime)value;
                var ep = new EntityProperty(dateTimeValue);
                return onValue(ep);
            }
            if (typeof(Guid).IsInstanceOfType(value))
            {
                var guidValue = (Guid)value;
                var ep = new EntityProperty(guidValue);
                return onValue(ep);
            }
            if (typeof(Guid[]).IsInstanceOfType(value))
            {
                var guidsValue = value as Guid[];
                var ep = new EntityProperty(guidsValue.ToByteArrayOfGuids());
                return onValue(ep);
            }
            if (typeof(Type).IsInstanceOfType(value))
            {
                var typeValue = (value as Type);
                var typeString = typeValue.AssemblyQualifiedName;
                var ep = new EntityProperty(typeString);
                return onValue(ep);
            }
            if (value == null)
                return onNoCast();
            var valueType = value.GetType();
            if (typeof(IReferenceable).IsAssignableFrom(valueType))
            {
                var refValue = value as IReferenceable;
                var guidValue = refValue.id;
                var ep = new EntityProperty(guidValue);
                return onValue(ep);
            }
            if (typeof(IReferenceableOptional).IsAssignableFrom(valueType))
            {
                var refValue = value as IReferenceableOptional;
                var guidValueMaybe = refValue.id;
                var ep = new EntityProperty(guidValueMaybe);
                return onValue(ep);
            }
            if (typeof(IReferences).IsAssignableFrom(valueType))
            {
                var refValue = value as IReferences;
                var guidValues = refValue.ids;
                var ep = new EntityProperty(guidValues.ToByteArrayOfGuids());
                return onValue(ep);
            }
            if (valueType.IsArray)
            {
                var arrayType = value.GetType().GetElementType();
                if (arrayType == typeof(object))
                {
                    var valueEnumerable = (System.Collections.IEnumerable)value;
                    var valueEnumerator = valueEnumerable.GetEnumerator();
                    var entityProperties = valueEnumerable
                        .Cast<object>()
                        .Select(
                            (v) =>
                            {
                                return CastEntityProperty(v,
                                    ep => ep,
                                    () => throw new NotImplementedException(
                                        $"Serialization of {arrayType.FullName} is currently not supported on arrays."));
                            })
                        .ToArray();
                    var bytess = entityProperties.ToByteArrayOfEntityProperties();
                    var epArray = new EntityProperty(bytess);
                    return onValue(epArray);
                }
                if (arrayType.IsAssignableFrom(typeof(Guid)))
                {
                    var values = (Guid[])value;
                    var bytes = values.ToByteArrayOfGuids();
                    var ep = new EntityProperty(bytes);
                    return onValue(ep);
                }
                if (arrayType.IsSubClassOfGeneric(typeof(IReferenceable)))
                {
                    var values = (IReferenceable[])value;
                    var guidValues = values.Select(v => v.id).ToArray();
                    var bytes = guidValues.ToByteArrayOfGuids();
                    var ep = new EntityProperty(bytes);
                    return onValue(ep);
                }
                if (arrayType.IsAssignableFrom(typeof(byte)))
                {
                    var values = (byte[])value;
                    var ep = new EntityProperty(values);
                    return onValue(ep);
                }
                if (arrayType.IsAssignableFrom(typeof(bool)))
                {
                    var values = (bool[])value;
                    var bytes = values
                        .Select(b => b ? (byte)1 : (byte)0)
                        .ToArray();
                    var ep = new EntityProperty(bytes);
                    return onValue(ep);
                }
                if (arrayType.IsAssignableFrom(typeof(DateTime)))
                {
                    var values = (DateTime[])value;
                    var bytes = values.ToByteArrayOfDateTimes();
                    var ep = new EntityProperty(bytes);
                    return onValue(ep);
                }
                if (arrayType.IsAssignableFrom(typeof(DateTime?)))
                {
                    var values = (DateTime?[])value;
                    var bytes = values.ToByteArrayOfNullableDateTimes();
                    var ep = new EntityProperty(bytes);
                    return onValue(ep);
                }
                if (arrayType.IsAssignableFrom(typeof(double)))
                {
                    var values = (double[])value;
                    var bytes = values.ToByteArrayOfDoubles();
                    var ep = new EntityProperty(bytes);
                    return onValue(ep);
                }
                if (arrayType.IsAssignableFrom(typeof(decimal)))
                {
                    var values = (decimal[])value;
                    var bytes = values.ToByteArrayOfDecimals();
                    var ep = new EntityProperty(bytes);
                    return onValue(ep);
                }
                if (arrayType.IsAssignableFrom(typeof(int)))
                {
                    var values = (int[])value;
                    var bytes = values.ToByteArrayOfInts();
                    var ep = new EntityProperty(bytes);
                    return onValue(ep);
                }
                if (arrayType.IsAssignableFrom(typeof(long)))
                {
                    var values = (long[])value;
                    var bytes = values.ToByteArrayOfLongs();
                    var ep = new EntityProperty(bytes);
                    return onValue(ep);
                }
                if (arrayType.IsAssignableFrom(typeof(string)))
                {
                    var values = (string[])value;
                    var bytes = values.ToUTF8ByteArrayOfStrings();
                    var ep = new EntityProperty(bytes);
                    return onValue(ep);
                }
                if (arrayType.IsEnum)
                {
                    var values = ((IEnumerable)value).Cast<object>();
                    var bytes = values.ToByteArrayOfEnums(arrayType);
                    var ep = new EntityProperty(bytes);
                    return onValue(ep);
                }

                // Default
                {
                    var valueEnumerable = (System.Collections.IEnumerable)value;
                    var valueEnumerator = valueEnumerable.GetEnumerator();
                    var entityProperties = valueEnumerable
                        .Cast<object>()
                        .Select(
                            (v) =>
                            {
                                return CastEntityProperty(v,
                                    ep => ep,
                                    () => throw new NotImplementedException(
                                        $"Serialization of {arrayType.FullName} is currently not supported on arrays."));
                            })
                        .ToArray();
                    var bytess = entityProperties.ToByteArrayOfEntityProperties();
                    var epArray = new EntityProperty(bytess);
                    return onValue(epArray);
                }
            }
            return onNoCast();
        }
        
        #endregion

        #region EntityProperty to object

        public virtual object GetMemberValue(MemberInfo memberInfo, IDictionary<string, EntityProperty> values)
        {
            var propertyName = this.Name.IsNullOrWhiteSpace(
                () => memberInfo.Name,
                (text) => text);

            var type = memberInfo.GetPropertyOrFieldType();

            if(IsMultiProperty(type))
                return BindEntityProperties(propertyName, type, values,
                    (convertedValue) => convertedValue,
                    () =>
                    {
                        var exceptionText = $"Could not deserialize value for {memberInfo.DeclaringType.FullName}..{memberInfo.Name}[{type.FullName}]" +
                            $"Please override StoragePropertyAttribute's BindEntityProperties for type:{type.FullName}";
                        throw new Exception(exceptionText);
                    });

            if (!values.ContainsKey(propertyName))
                return BindEmptyEntityProperty(type,
                    (convertedValue) => convertedValue,
                    () =>
                    {
                        var exceptionText = $"Could not create empty value for {memberInfo.DeclaringType.FullName}..{memberInfo.Name}[{type.FullName}]" +
                            $"Please override StoragePropertyAttribute's BindEmptyValue for type:{type.FullName}";
                        throw new Exception(exceptionText);
                    });

            var value = values[propertyName];
            return BindEntityProperty(type, value,
                (convertedValue) => convertedValue,
                () =>
                {
                    var exceptionText = $"Could not cast `{{0}}` to {type.FullName}." +
                            "Please override StoragePropertyAttribute's BindEntityProperty for property:" +
                            $"{memberInfo.DeclaringType.FullName}..{memberInfo.Name}";
                    if (value.PropertyType == EdmType.Binary)
                    {
                        var valueString = value.BinaryValue.Select(x => x.ToString("X2")).Join("");
                        throw new Exception(String.Format(exceptionText, valueString));
                    }
                    throw new Exception(String.Format(exceptionText, value.StringValue));
                });

        }

        protected virtual TResult BindEntityProperties<TResult>(string propertyName, Type type,
                IDictionary<string, EntityProperty> allValues,
            Func<object, TResult> onBound,
            Func<TResult> onFailedToBind)
        {
            if(type.IsSubClassOfGeneric(typeof(IDictionary<,>)))
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
                return BindEntityProperty(keyArrayType, allValues[keysPropertyName],
                    (keyValues) => BindEntityProperty(valueArrayType, allValues[valuesPropertyName],
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

            if (type.IsArray)
            {
                var arrayType = type.GetElementType();
                var storageMembers = arrayType.GetPersistenceAttributes();

                if (storageMembers.Any())
                {
                    var storageArrays = storageMembers
                        .Select(
                            storageMemberKvp =>
                            {
                                var attr = storageMemberKvp.Value.First();
                                var member = storageMemberKvp.Key;
                                var objPropName = attr.Name.HasBlackSpace() ? attr.Name : member.Name;
                                var propName = $"{propertyName}__{objPropName}";
                                return propName.PairWithValue(member);
                            })
                        .Where(kvp => allValues.ContainsKey(kvp.Key))
                        .Select(
                            propNameMemberTypeKvp =>
                            {
                                var value = allValues[propNameMemberTypeKvp.Key];
                                var member = propNameMemberTypeKvp.Value;
                                var memberType = member.GetPropertyOrFieldType();
                                var uncastArray = value.ToArray(memberType,
                                    (v) => v,
                                    () => throw new Exception($"Cannot serialize array of type `{memberType.FullName}`"));
                                var array = ((IEnumerable)uncastArray)
                                    .Cast<object>()
                                    .Select(v => v.PairWithValue(member))
                                    .ToArray();
                                return array;
                            })
                        .ToArray();

                    var itemsLength = storageArrays.Any()?
                        storageArrays.Min(storageArray => storageArray.Length)
                        :
                        0;
                    var items = Array.CreateInstance(arrayType, itemsLength);
                    foreach (int i in Enumerable.Range(0, itemsLength))
                    {
                        var item = Activator.CreateInstance(arrayType);
                        items.SetValue(item, i);
                    }
                    foreach(var storageArray in storageArrays)
                        foreach(int i in Enumerable.Range(0, itemsLength))
                        {
                            if (i >= storageArray.Length)
                                break;
                            var kvp = storageArray[i];
                            var value = kvp.Key;
                            var member = kvp.Value;
                            var item = items.GetValue(i);
                            member.SetValue(ref item, value);
                            items.SetValue(item, i); // needed for copied structs
                        }
                    return onBound(items);
                }
            }

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

            if (type.IsSubClassOfGeneric(typeof(IRefs<>)))
            {
                var resourceType = type.GenericTypeArguments.First();
                var instantiatableType = typeof(EastFive.Azure.Persistence.Refs<>).MakeGenericType(resourceType);
                var instance = Activator.CreateInstance(instantiatableType, new object[] { new Guid[] { } });
                return onBound(instance);
            }

            if (type.IsAssignableFrom(typeof(string)))
                return onBound(default(string));

            if (type.IsAssignableFrom(typeof(long)))
                return onBound(default(long));

            if (type.IsSubClassOfGeneric(typeof(Nullable<>)))
            {
                var resourceType = type.GenericTypeArguments.First();
                var instantiatableType = typeof(Nullable<>).MakeGenericType(resourceType);
                var instance = Activator.CreateInstance(instantiatableType, new object[] { });
                return onBound(instance);
            }

            return onFailedToBind();
        }

        private static object IRefInstance(Guid guidValue, Type type)
        {
            var instantiatableType = typeof(EastFive.Azure.Persistence.Ref<>).MakeGenericType(type);
            var instance = Activator.CreateInstance(instantiatableType, new object[] { guidValue });
            return instance;
        }

        private static object IRefObjInstance(Guid guidValue, Type type)
        {
            var instantiatableType = typeof(EastFive.Azure.Persistence.RefObj<>).MakeGenericType(type);
            var instance = Activator.CreateInstance(instantiatableType, new object[] { guidValue });
            return instance;
        }

        protected virtual TResult BindEntityProperty<TResult>(Type type, EntityProperty value,
            Func<object, TResult> onBound,
            Func<TResult> onFailedToBind)
        {
            if (type.IsArray)
            {
                var arrayType = type.GetElementType();
                return value.ToArray(arrayType,
                    onBound,
                    onFailedToBind);
            }

            if (type.IsAssignableFrom(typeof(Guid)))
            {
                var guidValue = value.GuidValue;
                return onBound(guidValue);
            }
            if (type.IsAssignableFrom(typeof(long)))
            {
                var longValue = value.Int64Value;
                return onBound(longValue);
            }
            if (type.IsAssignableFrom(typeof(float)))
            {
                var floatValue = value.DoubleValue;
                return onBound(floatValue);
            }
            if (type.IsAssignableFrom(typeof(double)))
            {
                var floatValue = value.DoubleValue;
                return onBound(floatValue);
            }
            //if (type.IsAssignableFrom(typeof(Guid[])))
            //{
            //    var guidsValueBinary = value.BinaryValue;
            //    var guidsValue = guidsValueBinary.ToGuidsFromByteArray();
            //    return onBound(guidsValue);
            //}
            if (type.IsAssignableFrom(typeof(string)))
            {
                var stringValue = value.StringValue;
                return onBound(stringValue);
            }
            if (type.IsAssignableFrom(typeof(Type)))
            {
                var typeValueString = value.StringValue;
                var typeValue = Type.GetType(typeValueString);
                return onBound(typeValue);
            }
            if (type.IsAssignableFrom(typeof(bool)))
            {
                var boolValue = value.BooleanValue;
                return onBound(boolValue);
            }

            object IRefInstance(Guid guidValue)
            {
                var resourceType = type.GenericTypeArguments.First();
                return StoragePropertyAttribute.IRefInstance(guidValue, resourceType);
            }

            if (type.IsSubClassOfGeneric(typeof(IRef<>)))
            {
                var guidValue = value.GuidValue.Value;
                var instance = IRefInstance(guidValue);
                return onBound(instance);
            }

            if (type.IsSubClassOfGeneric(typeof(IRefOptional<>)))
            {
                var guidValueMaybe = value.PropertyType == EdmType.Binary?
                    default(Guid?)
                    :
                    value.GuidValue;
                var resourceType = type.GenericTypeArguments.First();
                var instantiatableType = typeof(EastFive.RefOptional<>)
                    .MakeGenericType(resourceType);
                if (!guidValueMaybe.HasValue)
                {
                    var refOpt = Activator.CreateInstance(instantiatableType, new object[] { });
                    return onBound(refOpt);
                }
                var guidValue = guidValueMaybe.Value;
                var refValue = IRefInstance(guidValue);
                var instance = Activator.CreateInstance(instantiatableType, new object[] { refValue });
                return onBound(instance);
            }

            if (type.IsSubClassOfGeneric(typeof(IRefs<>)))
            {
                var guidValues = value.BinaryValue.ToGuidsFromByteArray();
                var resourceType = type.GenericTypeArguments.First();
                var instantiatableType = typeof(EastFive.Azure.Persistence.Refs<>).MakeGenericType(resourceType);
                var instance = Activator.CreateInstance(instantiatableType, new object[] { guidValues });
                return onBound(instance);
            }

            object IRefObjInstance(Guid guidValue)
            {
                var resourceType = type.GenericTypeArguments.First();
                return StoragePropertyAttribute.IRefObjInstance(guidValue, resourceType);
            }

            if (type.IsSubClassOfGeneric(typeof(IRefObj<>)))
            {
                var guidValue = value.GuidValue.Value;
                var instance = IRefObjInstance(guidValue);
                return onBound(instance);
            }

            if (type.IsSubClassOfGeneric(typeof(IRefObjOptional<>)))
            {
                var guidValueMaybe = value.PropertyType == EdmType.Binary ?
                    default(Guid?)
                    :
                    value.GuidValue;
                var resourceType = type.GenericTypeArguments.First();
                var instantiatableType = typeof(EastFive.RefObjOptional<>)
                    .MakeGenericType(resourceType);
                if (!guidValueMaybe.HasValue)
                {
                    var refOpt = Activator.CreateInstance(instantiatableType, new object[] { });
                    return onBound(refOpt);
                }
                var guidValue = guidValueMaybe.Value;
                var refValue = IRefObjInstance(guidValue);
                var instance = Activator.CreateInstance(instantiatableType, new object[] { refValue });
                return onBound(instance);
            }

            return onFailedToBind();
        }

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

            if (type.IsSubClassOfGeneric(typeof(IRefs<>)))
            {
                var resourceType = type.GenericTypeArguments.First();
                var instantiatableType = typeof(EastFive.Azure.Persistence.Refs<>).MakeGenericType(resourceType);
                var instance = Activator.CreateInstance(instantiatableType, new object[] { new Guid[] { } });
                return onBound(instance);
            }

            if (type.IsAssignableFrom(typeof(string)))
                return onBound(default(string));

            if (type.IsAssignableFrom(typeof(long)))
                return onBound(default(long));

            if (type.IsSubClassOfGeneric(typeof(Nullable<>)))
            {
                var resourceType = type.GenericTypeArguments.First();
                var instantiatableType = typeof(Nullable<>).MakeGenericType(resourceType);
                var instance = Activator.CreateInstance(instantiatableType, new object[] { });
                return onBound(instance);
            }

            if (type.IsArray)
            {
                var arrayInstance = Array.CreateInstance(type.GetElementType(), 0);
                return onBound(arrayInstance);
            }

            return onFailedToBind();
        }

        #endregion

        public string GeneratePartitionKey(string rowKey, object value, MemberInfo memberInfo)
        {
            return rowKey.GeneratePartitionKey();
        }
    }
}
