using EastFive.Collections.Generic;
using EastFive.Extensions;
using EastFive.Linq;
using EastFive.Reflection;
using EastFive.Serialization;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence.Azure.StorageTables
{
    public static class EntityPropertyExtensions
    {
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

        public static byte[] ToByteArrayOfEntityProperties(this IEnumerable<EntityProperty> entityProperties)
        {
            return entityProperties
                .ToByteArray(
                    ep =>
                    {
                        var epBytes = ToByteArrayOfEntityProperty(ep);
                        var typeByte = typeBytes[ep.PropertyType];
                        var compositeBytes = typeByte.AsArray().Concat(epBytes).ToArray();
                        return compositeBytes;
                    });
        }

        public static byte[] ToByteArrayOfEntityProperty(this EntityProperty ep)
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
                        if (null == ep.StringValue)
                            return new byte[] { 1 };
                        if (string.Empty == ep.StringValue)
                            return new byte[] { 2 };

                        return (new byte[] { 0 }).Concat(Encoding.UTF8.GetBytes(ep.StringValue)).ToArray();
                    }
            }
            throw new Exception($"Unrecognized EdmType {ep.PropertyType}");
        }

        public static object[] FromEdmTypedByteArray(this byte[] binaryValue, Type type)
        {
            var typeFromByte = typeBytes
                .Select(kvp => kvp.Key.PairWithKey(kvp.Value))
                .ToDictionary();
            var arrOfObj = binaryValue
                .FromByteArray()
                .Select(
                    typeWithBytes =>
                    {
                        if (!typeWithBytes.Any())
                            return type.GetDefault();
                        var typeByte = typeWithBytes[0];
                        if(!typeFromByte.ContainsKey(typeByte))
                            return type.GetDefault();
                        var edmType = typeFromByte[typeByte];
                        var valueBytes = typeWithBytes.Skip(1).ToArray();
                        var valueObj = edmType.ToObjectFromEdmTypeByteArray(valueBytes);
                        if(null == valueObj)
                        {
                            if (type.IsClass)
                                return valueObj;
                            return type.GetDefault();
                        }
                        if (!type.IsAssignableFrom(valueObj.GetType()))
                            return type.GetDefault(); // TODO: Data corruption?
                        return valueObj;
                    })
                .ToArray();
            return arrOfObj;
        }

        public static TResult CastEntityProperty<TResult>(this object value, Type valueType,
            Func<EntityProperty, TResult> onValue,
            Func<TResult> onNoCast)
        {
            if (valueType.IsArray)
            {
                var arrayType = valueType.GetElementType();
                return value.CastSingleValueToArray(arrayType,
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
            if (typeof(decimal).IsInstanceOfType(value))
            {
                var decimalValue = (decimal)value;
                var ep = new EntityProperty((double)decimalValue);
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
            if (typeof(Uri).IsInstanceOfType(value))
            {
                var uriValue = (Uri)value;
                var ep = new EntityProperty(uriValue.ToString());
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
                var guidValueMaybe = refValue.IsDefaultOrNull() ? default(Guid?) : refValue.id;
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
                return CastEntityProperty(value, valueTypeOfInstance, onValue, onNoCast);
            }

            #endregion

            return onNoCast();
        }

        public static TResult Bind<TResult>(this EntityProperty value, Type type, 
            Func<object, TResult> onBound,
            Func<TResult> onFailedToBind)
        {
            if (type.IsArray)
            {
                var arrayType = type.GetElementType();
                return value.BindSingleValueToArray(arrayType,
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
            if (typeof(Uri) == type)
            {
                var strValue = value.StringValue;
                if (Uri.TryCreate(strValue, UriKind.RelativeOrAbsolute, out Uri uriValue))
                    return onBound(uriValue);
                return onBound(uriValue);
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
                return EntityPropertyExtensions.IRefInstance(guidValue, resourceType);
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
                return EntityPropertyExtensions.IRefObjInstance(guidValue, resourceType);
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
                        var floatValue = value.DoubleValue.HasValue ?
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
                    if (typeof(decimal) == nullableType)
                    {
                        var doubleValueMaybe = value.DoubleValue;
                        var decimalMaybeValue = doubleValueMaybe.HasValue ?
                            (decimal)doubleValueMaybe.Value
                            :
                            default(decimal?);
                        return onBound(decimalMaybeValue);
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

        public static TResult CastSingleValueToArray<TResult>(this object value, Type arrayType, 
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
                            var bytess = v.CastSingleValueToArray(arrayElementType,
                                ep => ep.BinaryValue,
                                () => new byte[] { });
                            return bytess;
                        })
                    .ToArray();
                //var bytess = entityProperties.ToByteArrayOfEntityProperties();
                var epArray = new EntityProperty(fullBytes);
                return onValue(epArray);
            }

            #region Basic Types

            if (arrayType == typeof(object))
            {
                var valueEnumerable = (System.Collections.IEnumerable)value;
                var entityProperties = valueEnumerable
                    .Cast<object>()
                    .Select(
                        (v) =>
                        {
                            return v.CastEntityProperty(arrayType,
                                ep => ep,
                                () => throw new NotImplementedException(
                                    $"Serialization of {arrayType.FullName} is currently not supported on arrays."));
                        })
                    .ToArray();
                var bytess = entityProperties.ToByteArrayOfEntityProperties();
                var epArray = new EntityProperty(bytess);
                return onValue(epArray);
            }
            return arrayType.IsNullable(
                nulledType =>
                {
                    if (arrayType.IsAssignableFrom(typeof(decimal)))
                    {
                        var values = (decimal?[])value;
                        var bytes = values.ToByteArrayOfNullables(d => d.ConvertToBytes());
                        var ep = new EntityProperty(bytes);
                        return onValue(ep);
                    }
                    return GetDefault();
                },
                () =>
                {
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
                    return GetDefault();
                });

            #endregion

            TResult GetDefault()
            {
                var valueEnumerable = (System.Collections.IEnumerable)value;
                var valueEnumerator = valueEnumerable.GetEnumerator();
                var entityProperties = valueEnumerable
                    .Cast<object>()
                    .Select(
                        (v) =>
                        {
                            return arrayType
                                .GetAttributesInterface<ICast<EntityProperty>>(true)
                                .First(
                                    (epSerializer, next) =>
                                    {
                                        return epSerializer.Cast(v, arrayType,
                                            ep => ep,
                                            () => throw new NotImplementedException(
                                                $"Serialization of {arrayType.FullName} is currently not supported on arrays."));
                                    },
                                    () =>
                                    {
                                        return v.CastEntityProperty(arrayType,
                                            ep => ep,
                                            () => throw new NotImplementedException(
                                                $"Serialization of {arrayType.FullName} is currently not supported on arrays."));
                                    });
                        })
                    .ToArray();
                var bytess = entityProperties.ToByteArrayOfEntityProperties();
                var epArray = new EntityProperty(bytess);
                return onValue(epArray);
            }
        }

        public static TResult BindSingleValueToArray<TResult>(this EntityProperty value, Type arrayType, 
            Func<object, TResult> onBound,
            Func<TResult> onFailedToBind)
        {
            #region Refs

            object ComposeFromBase<TBase>(Type composedType, Type genericCompositionType,
                Func<Type, TBase, object> instantiate)
            {
                return value.BindSingleValueToArray(typeof(TBase),
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
                    if (typeof(decimal) == nulledType)
                    {
                        var values = value.BinaryValue.ToNullablesFromByteArray<decimal>(
                            byteArray =>
                            {
                                if(byteArray.TryConvertToDecimal(out decimal decimalValue))
                                    return decimalValue;
                                return default(decimal);
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

                    return arrayType
                        .GetAttributesInterface<IBind<EntityProperty>>(true)
                        .First<IBind<EntityProperty>, TResult>(
                            (epSerializer, next) =>
                            {
                                var values = value.BinaryValue.FromEdmTypedByteArray(typeof(byte[]));
                                var boundValues = values
                                    .Where(valueObject => valueObject is byte[])
                                    .Select(
                                        valueObject =>
                                        {
                                            var valueBytes = valueObject as byte[];
                                            var valueEp = new EntityProperty(valueBytes);
                                            return epSerializer.Bind(valueEp, arrayType,
                                                v => v,
                                                () => arrayType.GetDefault());
                                        })
                                    .CastArray(arrayType);
                                return onBound(boundValues);
                            },
                            () =>
                            {
                                throw new Exception($"Cannot serialize array of `{arrayType.FullName}`.");
                            });
                    
                });

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

    }
}
