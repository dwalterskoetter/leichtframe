using LeichtFrame.Core.Expressions;

namespace LeichtFrame.Core.Execution.Materialization.Helpers
{
    internal static class FilterHelpers
    {
        public static CompareOp MapOp(BinaryOp op)
        {
            return op switch
            {
                BinaryOp.Equal => CompareOp.Equal,
                BinaryOp.NotEqual => CompareOp.NotEqual,
                BinaryOp.GreaterThan => CompareOp.GreaterThan,
                BinaryOp.GreaterThanOrEqual => CompareOp.GreaterThanOrEqual,
                BinaryOp.LessThan => CompareOp.LessThan,
                BinaryOp.LessThanOrEqual => CompareOp.LessThanOrEqual,
                _ => throw new NotSupportedException($"Operator {op} not supported in filter.")
            };
        }
    }
}