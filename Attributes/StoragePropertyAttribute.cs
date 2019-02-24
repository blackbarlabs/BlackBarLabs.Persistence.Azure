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

            var valueType = memberInfo.GetPropertyOrFieldType();
            return CastValue(valueType, value, propertyName);
        }

        public KeyValuePair<string, EntityProperty>[] CastValue(Type typeOfValue, object value, string propertyName)
        {
            if (value.IsDefaultOrNull())
                return new KeyValuePair<string, EntityProperty>[] { };

            if(IsMultiProperty(typeOfValue))
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

            return CastEntityProperty(typeOfValue, value,
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
            var propertyName = this.Name.IsNullOrWhiteSpace(
                () => memberInfo.Name,
                (text) => text);

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
            return BindEntityProperty(type, value,
                onBound,
                onFailureToBind);

        }

        #region Single Value Serialization

        protected virtual TResult BindEntityProperty<TResult>(Type type, EntityProperty value,
            Func<object, TResult> onBound,
            Func<TResult> onFailedToBind)
        {
            if (type.IsArray)
            {
                var arrayType = type.GetElementType();
                return BindSingleEntityValueToArray(arrayType, value,
                    onBound,
                    onFailedToBind);
            }

            #region Basic values

            if (typeof(Guid) == type)
            {
                var guidValue = value.GuidValue;
                return onBound(guidValue);
            }
            if (typeof(long) == type)
            {
                var longValue = value.Int64Value;
                return onBound(longValue);
            }
            if (typeof(int) == type)
            {
                var intValue = value.Int32Value;
                return onBound(intValue);
            }
            if (typeof(float) == type)
            {
                var floatValue = (float)value.DoubleValue;
                return onBound(floatValue);
            }
            if (typeof(double) == type)
            {
                var floatValue = value.DoubleValue;
                return onBound(floatValue);
            }
            if (typeof(string) == type)
            {
                if (value.PropertyType != EdmType.String)
                    return onBound(default(string));
                var stringValue = value.StringValue;
                return onBound(stringValue);
            }
            if (typeof(DateTime) == type)
            {
                var dtValue = value.DateTime;
                return onBound(dtValue);
            }
            if (typeof(Type) == type)
            {
                var typeValueString = value.StringValue;
                var typeValue = Type.GetType(typeValueString);
                return onBound(typeValue);
            }
            if (type.IsEnum)
            {
                var enumNameString = value.StringValue;
                var enumValue = Enum.Parse(type, enumNameString);
                return onBound(enumValue);
            }
            if (typeof(bool) == type)
            {
                var boolValue = value.BooleanValue;
                return onBound(boolValue);
            }

            

            #region refs

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
                Guid? GetIdMaybe()
                {
                    if (value.PropertyType == EdmType.String)
                    {
                        if (Guid.TryParse(value.StringValue, out Guid id))
                            return id;
                        return default(Guid?);
                    }

                    if (value.PropertyType == EdmType.Binary)
                        return default(Guid?);

                    return value.GuidValue;
                }
                var guidValueMaybe = GetIdMaybe();
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

            #endregion

            if (typeof(object) == type)
            {
                switch (value.PropertyType)
                {
                    case EdmType.Binary:
                        return onBound(value.BinaryValue);
                    case EdmType.Boolean:
                        if (value.BooleanValue.HasValue)
                            return onBound(value.BooleanValue.Value);
                        break;
                    case EdmType.DateTime:
                        if (value.DateTime.HasValue)
                            return onBound(value.DateTime.Value);
                        break;
                    case EdmType.Double:
                        if (value.DoubleValue.HasValue)
                            return onBound(value.DoubleValue.Value);
                        break;
                    case EdmType.Guid:
                        if (value.GuidValue.HasValue)
                        {
                            var nullGuidKey = new Guid(EDMExtensions.NullGuidKey);
                            if (value.GuidValue.Value == nullGuidKey)
                                return onBound(null);
                            return onBound(value.GuidValue.Value);
                        }
                        break;
                    case EdmType.Int32:
                        if (value.Int32Value.HasValue)
                            return onBound(value.Int32Value.Value);
                        break;
                    case EdmType.Int64:
                        if (value.Int64Value.HasValue)
                            return onBound(value.Int64Value.Value);
                        break;
                    case EdmType.String:
                        return onBound(value.StringValue);
                }
                return onBound(value.PropertyAsObject);
            }

            #endregion

            return type.IsNullable(
                nullableType =>
                {
                    if (typeof(Guid) == nullableType)
                    {
                        var guidValue = value.GuidValue;
                        return onBound(guidValue);
                    }
                    if (typeof(long) == nullableType)
                    {
                        var longValue = value.Int64Value;
                        return onBound(longValue);
                    }
                    if (typeof(int) == nullableType)
                    {
                        var intValue = value.Int32Value;
                        return onBound(intValue);
                    }
                    if (typeof(float) == nullableType)
                    {
                        var floatValue = value.DoubleValue.HasValue?
                            (float)value.DoubleValue.Value
                            :
                            default(float?);
                        return onBound(floatValue);
                    }
                    if (typeof(double) == nullableType)
                    {
                        var floatValue = value.DoubleValue;
                        return onBound(floatValue);
                    }
                    if (typeof(DateTime) == nullableType)
                    {
                        var dtValue = value.DateTime;
                        return onBound(dtValue);
                    }
                    return onFailedToBind();
                },
                () => onFailedToBind());
        }

        public virtual TResult CastEntityProperty<TResult>(Type valueType, object value,
            Func<EntityProperty, TResult> onValue,
            Func<TResult> onNoCast)
        {
            if (valueType.IsArray)
            {
                var arrayType = valueType.GetElementType();
                return CastSingleValueToArray(arrayType, value,
                    onValue,
                    onNoCast);
            }

            #region Basic values

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
            if (typeof(Type).IsInstanceOfType(value))
            {
                var typeValue = (value as Type);
                var typeString = typeValue.AssemblyQualifiedName;
                var ep = new EntityProperty(typeString);
                return onValue(ep);
            }
            if (valueType.IsEnum)
            {
                var enumNameString = Enum.GetName(valueType, value);
                var ep = new EntityProperty(enumNameString);
                return onValue(ep);
            }

            #region Refs

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
                var guidValueMaybe = refValue.IsDefaultOrNull()? default(Guid?) : refValue.id;
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

            #endregion

            if (typeof(object) == valueType)
            {
                if (null == value)
                {
                    var nullGuidKey = new Guid(EDMExtensions.NullGuidKey);
                    var ep = new EntityProperty(nullGuidKey);
                    return onValue(ep);
                }
                var valueTypeOfInstance = value.GetType();
                if (typeof(object) == valueTypeOfInstance) // Prevent stack overflow recursion
                {
                    var ep = new EntityProperty(new byte[] { });
                    return onValue(ep);
                }
                return CastEntityProperty(valueTypeOfInstance, value, onValue, onNoCast);
            }

            #endregion

            return onNoCast();
        }

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
                            var arrayValues = BindSingleEntityValueToArray<object>(arrayElementType, new EntityProperty(bytes),
                                v => v,
                                () => Array.CreateInstance(arrayElementType, 0));
                            // var arrayValues = bytes.FromEdmTypedByteArray(arrayElementType);
                            return arrayValues;
                        })
                    .ToArray();
                //var values = value.BinaryValue.FromEdmTypedByteArray(arrayElementType);
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
                                if(byteArray.Length == 16)
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
                    if (typeof(string) == arrayType)
                    {
                        var values = value.BinaryValue.ToStringNullOrEmptysFromUTF8ByteArray();
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

        private TResult CastSingleValueToArray<TResult>(Type arrayType, object value,
            Func<EntityProperty, TResult> onValue,
            Func<TResult> onNoCast)
        {
            #region Refs

            if (arrayType.IsSubClassOfGeneric(typeof(IReferenceable)))
            {
                var values = (IReferenceable[])value;
                var guidValues = values.Select(v => v.id).ToArray();
                var bytes = guidValues.ToByteArrayOfGuids();
                var ep = new EntityProperty(bytes);
                return onValue(ep);
            }
            if (arrayType.IsSubClassOfGeneric(typeof(IReferenceableOptional)))
            {
                var values = (IReferenceableOptional[])value;
                var guidMaybeValues = values.Select(v => v.IsDefaultOrNull() ? default(Guid?) : v.id).ToArray();
                var bytes = guidMaybeValues.ToByteArrayOfNullables(guid => guid.ToByteArray());
                var ep = new EntityProperty(bytes);
                return onValue(ep);
            }

            #endregion

            if (arrayType.IsArray)
            {
                var arrayElementType = arrayType.GetElementType();
                var valueEnumerable = (System.Collections.IEnumerable)value;
                var fullBytes = valueEnumerable
                    .Cast<object>()
                    .ToByteArray(
                        (v) =>
                        {
                            var vEnumerable = (System.Collections.IEnumerable)v;
                            var bytess = CastSingleValueToArray(arrayElementType, v,
                                ep => ep.BinaryValue,
                                () => new byte[] { });
                            return bytess;
                        })
                    .ToArray();
                //var bytess = entityProperties.ToByteArrayOfEntityProperties();
                var epArray = new EntityProperty(fullBytes);
                return onValue(epArray);
            }

            if (arrayType == typeof(object))
            {
                var valueEnumerable = (System.Collections.IEnumerable)value;
                var entityProperties = valueEnumerable
                    .Cast<object>()
                    .Select(
                        (v) =>
                        {
                            return CastEntityProperty(arrayType, v,
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
                var bytes = values.ToUTF8ByteArrayOfStringNullOrEmptys();
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
                            return CastEntityProperty(arrayType, v,
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

        #endregion

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
                var storageMembersArray = arrayType.GetPersistenceAttributes();
                if (storageMembersArray.Any())
                {
                    var arrayEps = BindArrayEntityProperties(propertyName, arrayType,
                        storageMembersArray, allValues);
                    return onBound(arrayEps);
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
                    var objPropName = attr.Name.HasBlackSpace() ? attr.Name : member.Name;
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

            if (valueType.IsArray)
            {
                var peristenceAttrs = valueType.GetElementType().GetPersistenceAttributes();
                if (peristenceAttrs.Any())
                {
                    var epsArray = CastArrayEntityProperties(value, peristenceAttrs);
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
                            var memberType = member.GetPropertyOrFieldType();

                            var v = member.GetValue(value);
                            var epValue = CastEntityProperty(memberType, v,
                                ep => ep,
                                () => new EntityProperty(new byte[] { }));

                            return epValue.PairWithKey(propName);
                        })
                    .ToArray();
                return onValues(storageArrays);
            }

            return onNoCast();
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
                        var objPropName = attr.Name.HasBlackSpace() ? attr.Name : member.Name;
                        
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
                storageArrays.Min(storageArray => storageArray.Value.Length)
                :
                0;
            var items = Array.CreateInstance(arrayType, itemsLength);
            foreach (int i in Enumerable.Range(0, itemsLength))
            {
                var item = Activator.CreateInstance(arrayType);
                items.SetValue(item, i);
            }
            foreach (var storageArray in storageArrays)
                foreach (int i in Enumerable.Range(0, itemsLength))
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
                        var propertyNameDefault = member.Name;
                        if (converter is StoragePropertyAttribute)
                        {
                            var spa = converter as StoragePropertyAttribute;
                            if (spa.Name.HasBlackSpace())
                                propertyNameDefault = spa.Name;
                        }
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


        public string GeneratePartitionKey(string rowKey, object value, MemberInfo memberInfo)
        {
            return rowKey.GeneratePartitionKey();
        }
    }
}
