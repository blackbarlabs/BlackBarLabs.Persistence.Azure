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

        public KeyValuePair<string, EntityProperty>[] ConvertValue(object value, string propertyName)
        {
            if (value.IsDefaultOrNull())
                return new KeyValuePair<string, EntityProperty>[] { };

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
                var keyEntityProperties = ConvertValue(keyValues, $"{propertyName}__keys");
                var valueEntityProperties = ConvertValue(valueValues, $"{propertyName}__values");
                var entityProperties = keyEntityProperties.Concat(valueEntityProperties).ToArray();
                return entityProperties;
            }

            return CastEntityProperty(value,
                (property) =>
                {
                    var kvp = property.PairWithKey(propertyName);
                    return kvp.AsArray();
                },
                () => new KeyValuePair<string, EntityProperty>[] { });
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
            if (typeof(IReferenceable).IsAssignableFrom(value.GetType()))
            {
                var refValue = value as IReferenceable;
                var guidValue = refValue.id;
                var ep = new EntityProperty(guidValue);
                return onValue(ep);
            }
            if (typeof(IReferenceableOptional).IsAssignableFrom(value.GetType()))
            {
                var refValue = value as IReferenceableOptional;
                var guidValueMaybe = refValue.id;
                var ep = new EntityProperty(guidValueMaybe);
                return onValue(ep);
            }
            if (typeof(IReferences).IsAssignableFrom(value.GetType()))
            {
                var refValue = value as IReferences;
                var guidValues = refValue.ids;
                var ep = new EntityProperty(guidValues.ToByteArrayOfGuids());
                return onValue(ep);
            }
            if (value.GetType().IsArray)
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
                    var bytess = ToByteArrayOfTypedEntityProperties(entityProperties);
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
                    var bytess = ToByteArrayOfEntityProperties(entityProperties);
                    var epArray = new EntityProperty(bytess);
                    return onValue(epArray);
                }
            }
            return onNoCast();
        }

        public byte [] ToByteArrayOfEntityProperties(IEnumerable<EntityProperty> entityProperties)
        {
            return entityProperties
                .ToByteArray(
                    ep => ToByteArrayOfEntityProperty(ep));
        }

        private static Dictionary<EdmType, byte> typeBytes = new Dictionary<EdmType, byte>
            {
                { EdmType.Binary, 1 },
                { EdmType.Boolean, 2 },
                { EdmType.DateTime, 3 },
                { EdmType.Double, 4 },
                { EdmType.Guid, 5 },
                { EdmType.Int32, 6 },
                { EdmType.Int64, 7},
                { EdmType.String, 8 },
            };

        public byte[] ToByteArrayOfTypedEntityProperties(IEnumerable<EntityProperty> entityProperties)
        {
            
            return entityProperties
                .ToByteArray(
                    ep =>
                    {
                        var typeByte = typeBytes[ep.PropertyType];
                        var epBytes = ToByteArrayOfEntityProperty(ep);
                        var compositeBytes = typeByte.AsArray().Concat(epBytes).ToArray();
                        return compositeBytes;
                    });
        }

        public byte [] ToByteArrayOfEntityProperty(EntityProperty ep)
        {
            switch (ep.PropertyType)
            {
                case EdmType.Binary:
                    {
                        if (ep.BinaryValue.IsDefaultOrNull())
                            return new byte[] { };
                        return ep.BinaryValue;
                    }
                case EdmType.Boolean:
                    {
                        if (!ep.BooleanValue.HasValue)
                            return new byte[] { };
                        var epValue = ep.BooleanValue.Value;
                        var boolByte = epValue ? (byte)1 : (byte)0;
                        return boolByte.AsArray();
                    }
                case EdmType.DateTime:
                    {
                        if (!ep.DateTime.HasValue)
                            return new byte[] { };
                        var dtValue = ep.DateTime.Value;
                        return BitConverter.GetBytes(dtValue.Ticks);
                    }
                case EdmType.Double:
                    {
                        if (!ep.DoubleValue.HasValue)
                            return new byte[] { };
                        var epValue = ep.DoubleValue.Value;
                        return BitConverter.GetBytes(epValue);
                    }
                case EdmType.Guid:
                    {
                        if (!ep.GuidValue.HasValue)
                            return new byte[] { };
                        var epValue = ep.GuidValue.Value;
                        return epValue.ToByteArray();
                    }
                case EdmType.Int32:
                    {
                        if (!ep.Int32Value.HasValue)
                            return new byte[] { };
                        var epValue = ep.Int32Value.Value;
                        return BitConverter.GetBytes(epValue);
                    }
                case EdmType.Int64:
                    {
                        if (!ep.Int64Value.HasValue)
                            return new byte[] { };
                        var epValue = ep.Int64Value.Value;
                        return BitConverter.GetBytes(epValue);
                    }
                case EdmType.String:
                    {
                        if (ep.StringValue.IsDefaultOrNull())
                            return new byte[] { };
                        return Encoding.UTF8.GetBytes(ep.StringValue);
                    }
            }
            throw new Exception($"Unrecognized EdmType {ep.PropertyType}");
        }

        public object ToObjectFromEdmTypeByteArray(EdmType type, byte[] values)
        {
            switch (type)
            {
                case EdmType.Binary:
                    {
                        return values;
                    }
                case EdmType.Boolean:
                    {
                        if (!values.Any())
                            return default(bool?);
                        return values[0] != 0;
                    }
                case EdmType.DateTime:
                    {
                        if (!values.Any())
                            return default(DateTime?);
                        var ticks = BitConverter.ToInt64(values, 0);
                        return new DateTime(ticks);
                    }
                case EdmType.Double:
                    {
                        if (!values.Any())
                            return default(double?);
                        var value = BitConverter.ToDouble(values, 0);
                        return value;
                    }
                case EdmType.Guid:
                    {
                        if (!values.Any())
                            return default(Guid?);
                        var value = new Guid(values);
                        return value;
                    }
                case EdmType.Int32:
                    {
                        if (!values.Any())
                            return default(int?);
                        var value = BitConverter.ToInt32(values, 0);
                        return value;
                    }
                case EdmType.Int64:
                    {
                        if (!values.Any())
                            return default(long?);
                        var value = BitConverter.ToInt64(values, 0);
                        return value;
                    }
                case EdmType.String:
                    {
                        if (!values.Any())
                            return default(string);

                        return Encoding.UTF8.GetString(values);
                    }
            }
            throw new Exception($"Unrecognized EdmType {type}");
        }

        public virtual object GetMemberValue(MemberInfo memberInfo, IDictionary<string, EntityProperty> values)
        {
            var propertyName = this.Name.IsNullOrWhiteSpace(
                () => memberInfo.Name,
                (text) => text);

            var type = memberInfo.GetPropertyOrFieldType();

            if (values.ContainsKey(propertyName))
            {
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

            return BindValue(propertyName, type, values,
                (convertedValue) => convertedValue,
                () =>
                {
                    var exceptionText = $"Could not create empty value for {memberInfo.DeclaringType.FullName}..{memberInfo.Name}[{type.FullName}]" +
                    "Please override StoragePropertyAttribute's BindEmptyValue for property:";
                    throw new Exception(exceptionText);
                });
        }

        protected virtual TResult BindValue<TResult>(string propertyName, Type type,
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
                    var defaultDic = instantiatableType.GetDefault();
                    return onBound(defaultDic);
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

                            var refOpt = (IDictionary)Activator.CreateInstance(instantiatableType, new object[] { });
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
                var storageMembers = arrayType.GetMembers()
                    .Where(field => field.ContainsAttributeInterface<IPersistInAzureStorageTables>())
                    .Select(field => field.PairWithValue(field.GetAttributesInterface<IPersistInAzureStorageTables>()));

                if (storageMembers.Any())
                {
                    var storageArrays = storageMembers
                        .Select(
                            storageMemberKvp =>
                            {
                                var attr = storageMemberKvp.Value.First();
                                var member = storageMemberKvp.Key;
                                var objPropName = attr.Name.HasBlackSpace() ? attr.Name : member.Name;
                                var propName = $"{propertyName}___{objPropName}";
                                return propName.PairWithValue(member);
                            })
                        .Where(kvp => allValues.ContainsKey(kvp.Key))
                        .Select(
                            propNameMemberTypeKvp =>
                            {
                                var value = allValues[propNameMemberTypeKvp.Key];
                                var member = propNameMemberTypeKvp.Value;
                                var memberType = member.GetPropertyOrFieldType();
                                var uncastArray = BindArray(memberType, value,
                                    (v) => v,
                                    () => throw new Exception("Cannot serialize array of type"));
                                var array = ((IEnumerable)uncastArray)
                                    .Cast<object>()
                                    .Select(v => v.PairWithValue(member))
                                    .ToArray();
                                return array;
                            })
                        .ToArray();

                    //IEnumerable GetInstances()
                    //{
                    //    int index = 0;
                    //    if()
                    //}
                    //var keysPropertyName = $"{propertyName}__keys";
                    //var values = value.BinaryValue.ToEnumsFromByteArray(arrayType);
                    return onBound(null);
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

        protected virtual TResult BindEntityProperty<TResult>(Type type, EntityProperty value,
            Func<object, TResult> onBound,
            Func<TResult> onFailedToBind)
        {
            if (type.IsAssignableFrom(typeof(Guid)))
            {
                var guidValue = value.GuidValue;
                return onBound(guidValue);
            }
            if (type.IsAssignableFrom(typeof(Guid[])))
            {
                var guidsValueBinary = value.BinaryValue;
                var guidsValue = guidsValueBinary.ToGuidsFromByteArray();
                return onBound(guidsValue);
            }
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
                var instantiatableType = typeof(EastFive.Azure.Persistence.Ref<>).MakeGenericType(resourceType);
                var instance = Activator.CreateInstance(instantiatableType, new object[] { guidValue });
                return instance;
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

            if (type.IsArray)
            {
                var arrayType = type.GetElementType();
                return BindArray(arrayType, value,
                    onBound,
                    onFailedToBind);
            }

            return onFailedToBind();
        }

        public TResult BindArray<TResult>(Type arrayType, EntityProperty value,
            Func<object, TResult> onBound,
            Func<TResult> onFailedToBind)
        {
            if (arrayType == typeof(object))
            {
                var typeFromByte = typeBytes
                    .Select(kvp => kvp.Key.PairWithKey(kvp.Value))
                    .ToDictionary();
                var arrOfObj = value.BinaryValue
                    .FromByteArray()
                    .Select(
                        typeWithBytes =>
                        {
                            var typeByte = typeWithBytes[0];
                            var valueBytes = typeWithBytes.Skip(1).ToArray();
                            var edmType = typeFromByte[typeByte];
                            var valueObj = ToObjectFromEdmTypeByteArray(edmType, valueBytes);
                            return valueObj;
                        })
                    .ToArray();
                return onBound(arrOfObj);
            }

            if (arrayType.IsSubClassOfGeneric(typeof(IRef<>)))
            {
                return BindEntityProperty(typeof(Guid[]), value,
                    objects =>
                    {
                        var resourceType = arrayType.GenericTypeArguments.First();
                        var instantiatableType = typeof(EastFive.Azure.Persistence.Ref<>).MakeGenericType(resourceType);
                        var guids = (Guid[])objects;
                        var refs = guids
                            .Select(
                                guidValue =>
                                {
                                    var instance = Activator.CreateInstance(instantiatableType, new object[] { guidValue });
                                    return instance;
                                })
                           .ToArray();
                        return onBound(refs);
                    },
                    onFailedToBind);
            }

            object IRefInstance(Guid guidValue)
            {
                var instantiatableType = typeof(EastFive.Azure.Persistence.Ref<>).MakeGenericType(arrayType);
                var instance = Activator.CreateInstance(instantiatableType, new object[] { guidValue });
                return instance;
            }

            if (arrayType.IsSubClassOfGeneric(typeof(IRefOptional<>)))
            {
                return BindEntityProperty(typeof(Guid?[]), value,
                    objects =>
                    {
                        var resourceType = arrayType.GenericTypeArguments.First();
                        var instantiatableType = typeof(EastFive.RefOptional<>)
                            .MakeGenericType(resourceType);
                        var guids = (Guid?[])objects;
                        var refs = guids
                            .Select(
                                guidValueMaybe =>
                                {
                                    if (!guidValueMaybe.HasValue)
                                    {
                                        var refOpt = Activator.CreateInstance(instantiatableType, new object[] { });
                                        return onBound(refOpt);
                                    }
                                    var guidValue = guidValueMaybe.Value;
                                    var refValue = IRefInstance(guidValue);
                                    var instance = Activator.CreateInstance(instantiatableType, new object[] { refValue });
                                    return onBound(instance);
                                })
                           .ToArray();
                        return onBound(refs);
                    },
                    onFailedToBind);
            }

            if (arrayType.IsAssignableFrom(typeof(Guid)))
            {
                var values = value.BinaryValue.ToGuidsFromByteArray();
                return onBound(values);
            }
            if (arrayType.IsAssignableFrom(typeof(byte)))
            {
                return onBound(value.BinaryValue);
            }
            if (arrayType.IsAssignableFrom(typeof(bool)))
            {
                var boolArray = value.BinaryValue
                    .Select(b => b != 0)
                    .ToArray();
                return onBound(boolArray);
            }
            if (arrayType.IsAssignableFrom(typeof(DateTime)))
            {
                var values = value.BinaryValue.ToDateTimesFromByteArray();
                return onBound(values);
            }
            if (arrayType.IsAssignableFrom(typeof(DateTime?)))
            {
                var values = value.BinaryValue.ToNullableDateTimesFromByteArray();
                return onBound(values);
            }
            if (arrayType.IsAssignableFrom(typeof(double)))
            {
                var values = value.BinaryValue.ToDoublesFromByteArray();
                return onBound(values);
            }
            if (arrayType.IsAssignableFrom(typeof(decimal)))
            {
                var values = value.BinaryValue.ToDecimalsFromByteArray();
                return onBound(values);
            }
            if (arrayType.IsAssignableFrom(typeof(int)))
            {
                var values = value.BinaryValue.ToIntsFromByteArray();
                return onBound(values);
            }
            if (arrayType.IsAssignableFrom(typeof(long)))
            {
                var values = value.BinaryValue.ToLongsFromByteArray();
                return onBound(values);
            }
            if (arrayType.IsAssignableFrom(typeof(string)))
            {
                var values = value.BinaryValue.ToStringsFromUTF8ByteArray();
                return onBound(values);
            }
            if (arrayType.IsEnum)
            {
                var values = value.BinaryValue.ToEnumsFromByteArray(arrayType);
                return onBound(values);
            }
            throw new Exception($"Cannot serialize array of `{arrayType.FullName}`.");
        }

        public string GeneratePartitionKey(string rowKey, object value, MemberInfo memberInfo)
        {
            return rowKey.GeneratePartitionKey();
        }
    }
}
