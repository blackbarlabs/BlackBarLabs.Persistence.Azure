using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage.Table;

using EastFive.Linq.Expressions;
using EastFive.Reflection;
using EastFive.Linq;
using EastFive.Extensions;

namespace EastFive.Persistence.Azure.StorageTables
{
    public static class TableQueryExtensions
    {
        public static string GetTablePropertyName(this MemberInfo member)
        {
            return member
                .GetAttributesInterface<IPersistInAzureStorageTables>()
                .First(
                    (attr, next) =>
                    {
                        var tablePropertyName = attr.GetTablePropertyName(member);
                        return tablePropertyName;
                    },
                    () => member.Name);
           
        }

        public static object GetTableQuery<TEntity>(string whereExpression = null)
        {
            var query = typeof(TEntity)
                .GetAttributesInterface<IProvideTable>()
                .First(
                    (tableProvider, next) =>
                    {
                        var tableQueryDyanmic = tableProvider.GetTableQuery<TEntity>(whereExpression);
                        return tableQueryDyanmic;
                    },
                    () =>
                    {
                        var tableQuery = new TableQuery<TableEntity<TEntity>>();
                        if (!whereExpression.HasBlackSpace())
                            return (object)tableQuery;
                        return (object)tableQuery.Where(whereExpression);
                    });
            return query;
        }

        private static MemberInfo ResolveMemberInType(Type entityType, MemberExpression expression)
        {
            var member = expression.Member;
            if (entityType.GetMembers().Contains(member))
            {
                if (member.ContainsAttributeInterface<IPersistInAzureStorageTables>())
                    return member;
                throw new ArgumentException($"{member.DeclaringType.FullName}..{member.Name} is not storage property/field.");
            }

            if (expression.Expression is MemberExpression)
                return ResolveMemberInType(entityType, expression.Expression as MemberExpression);

            throw new ArgumentException($"{member.DeclaringType.FullName}..{member.Name} is not a property/field of {entityType.FullName}.");
        }

        private static string ExpressionTypeToQueryComparison(ExpressionType comparision)
        {
            if (ExpressionType.Equal == comparision)
                return QueryComparisons.Equal;
            if (ExpressionType.Assign == comparision) // why not
                return QueryComparisons.Equal;
            if (ExpressionType.GreaterThan == comparision)
                return QueryComparisons.GreaterThan;
            if (ExpressionType.GreaterThanOrEqual == comparision)
                return QueryComparisons.GreaterThanOrEqual;
            if (ExpressionType.LessThan == comparision)
                return QueryComparisons.LessThan;
            if (ExpressionType.LessThanOrEqual == comparision)
                return QueryComparisons.LessThanOrEqual;

            throw new ArgumentException($"{comparision} is not a supported query comparison.");
        }

        private static string WhereExpression(ExpressionType comparision, string assignmentName, object assignmentValue)
        {
            var queryComparison = ExpressionTypeToQueryComparison(comparision);

            if (typeof(Guid?).IsInstanceOfType(assignmentValue))
                TableQuery.GenerateFilterConditionForGuid(assignmentName, queryComparison, (assignmentValue as Guid?).Value);

            if (typeof(Guid).IsInstanceOfType(assignmentValue))
                return TableQuery.GenerateFilterConditionForGuid(assignmentName, queryComparison, (Guid)assignmentValue);

            if (typeof(bool).IsInstanceOfType(assignmentValue))
                return TableQuery.GenerateFilterConditionForBool(assignmentName, queryComparison, (bool)assignmentValue);

            if (typeof(DateTime).IsInstanceOfType(assignmentValue))
                return TableQuery.GenerateFilterConditionForDate(assignmentName, queryComparison, (DateTime)assignmentValue);

            if (typeof(int).IsInstanceOfType(assignmentValue))
                return TableQuery.GenerateFilterConditionForInt(assignmentName, queryComparison, (int)assignmentValue);

            if (typeof(string).IsInstanceOfType(assignmentValue))
                return TableQuery.GenerateFilterCondition(assignmentName, queryComparison, (string)assignmentValue);

            throw new NotImplementedException($"No filter condition created for type {assignmentValue.GetType().FullName}");
        }

        private static object ResolveUnaryExpression<TEntity>(UnaryExpression expression, out Func<TEntity, bool> postFilter)
        {
            postFilter = (entity) => true;
            var operand = expression.Operand;
            if (!(operand is MemberExpression))
                throw new NotImplementedException($"Unary expression of type {operand.GetType().FullName} is not supported.");

            var memberOperand = operand as MemberExpression;
            var assignmentMember = ResolveMemberInType(typeof(TEntity), memberOperand);
            var assignmentName = assignmentMember.GetTablePropertyName();

            var query = GetTableQuery<TEntity>();
            var nullableHasValueProperty = typeof(Nullable<>).GetProperty("HasValue");
            if (memberOperand.Member == nullableHasValueProperty)
            {
                postFilter =
                        (entity) =>
                        {
                            var nullableValue = assignmentMember.GetValue(entity);
                            var hasValue = nullableHasValueProperty.GetValue(nullableValue);
                            var hasValueBool = (bool)hasValue;
                            return !hasValueBool;
                        };
                return query;

                //if (expression.NodeType == ExpressionType.Not)
                //{
                //    var whereExpr = TableQuery.GenerateFilterCondition(assignmentName, QueryComparisons.Equal, "");
                //    var whereQuery = query.Where(whereExpr);
                //    return whereQuery;
                //}
                //{
                //    var whereExpr = TableQuery.GenerateFilterCondition(assignmentName, QueryComparisons.NotEqual, "");
                //    var whereQuery = query.Where(whereExpr);
                //    return whereQuery;
                //}
            }

            var refOptionalHasValueProperty = typeof(EastFive.IReferenceableOptional).GetProperty("HasValue");
            if (memberOperand.Member == refOptionalHasValueProperty)
            {
                postFilter =
                    (entity) =>
                    {
                        var nullableValue = assignmentMember.GetValue(entity);
                        var hasValue = refOptionalHasValueProperty.GetValue(nullableValue);
                        var hasValueBool = (bool)hasValue;
                        return !hasValueBool;
                    };
                return query;

                //if (expression.NodeType == ExpressionType.Not)
                //{
                //    var whereExpr = TableQuery.GenerateFilterCondition(assignmentName, QueryComparisons.Equal, null);
                //    var whereQuery = query.Where(whereExpr);
                //    return whereQuery;
                //}
                //{
                //    var whereExpr = TableQuery.GenerateFilterCondition(assignmentName, QueryComparisons.NotEqual, "");
                //    var whereQuery = query.Where(whereExpr);
                //    return whereQuery;
                //}
            }

            throw new NotImplementedException($"Unary expression of type {memberOperand.Member.DeclaringType.FullName}..{memberOperand.Member.Name} is not supported.");
        }

        private static object ResolveConstantExpression<TEntity>(ConstantExpression expression)
        {
            if (!typeof(bool).IsAssignableFrom(expression.Type))
                throw new NotImplementedException($"Constant expression of type {expression.Type.FullName} is not supported.");

            var value = (bool)expression.Value;
            if (!value)
                throw new Exception("Query for nothing?");

            var query = GetTableQuery<TEntity>();
            return query;
        }

        private static object ResolveMemberExpression<TEntity>(MemberExpression expression)
        {
            var assignmentMember = ResolveMemberInType(typeof(TEntity), expression);
            if (!typeof(bool).IsAssignableFrom(expression.Type))
                throw new NotImplementedException($"Member expression of type {expression.Type.FullName} is not supported.");

            var assignmentName = assignmentMember.GetTablePropertyName();
            var filter = TableQuery.GenerateFilterConditionForBool(assignmentName, QueryComparisons.Equal, true);
            var whereQuery = GetTableQuery<TEntity>(filter);
            return whereQuery;
        }

        public static object ResolveExpression<TEntity>(this Expression<Func<TEntity, bool>> filter, out Func<TEntity, bool> postFilter)
        {
            if (filter.Body is UnaryExpression)
                return ResolveUnaryExpression<TEntity>(filter.Body as UnaryExpression, out postFilter);

            postFilter = (entity) => true;
            if (filter.Body is ConstantExpression)
                return ResolveConstantExpression<TEntity>(filter.Body as ConstantExpression);

            if (filter.Body is MemberExpression)
                return ResolveMemberExpression<TEntity>(filter.Body as MemberExpression);

            if (!(filter.Body is BinaryExpression))
                throw new ArgumentException("TableQuery expression is not a binary expression");

            var binaryExpression = filter.Body as BinaryExpression;
            if (!(binaryExpression.Left is MemberExpression))
                throw new ArgumentException("TableQuery expression left side must be an MemberExpression");

            var assignmentMember = ResolveMemberInType(typeof(TEntity), binaryExpression.Left as MemberExpression);
            var assignmentValue = binaryExpression.Right.ResolveExpression();
            var assignmentName = assignmentMember.GetTablePropertyName();

            var whereExpr = WhereExpression(binaryExpression.NodeType, assignmentName, assignmentValue);
            var whereQuery = GetTableQuery<TEntity>(whereExpr);
            return whereQuery;
        }

    }
}
