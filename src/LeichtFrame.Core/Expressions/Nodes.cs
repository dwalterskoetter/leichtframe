namespace LeichtFrame.Core.Expressions
{
    /// <summary>
    /// Defines the supported binary operations in the expression tree.
    /// </summary>
    public enum BinaryOp
    {
        /// <summary>Addition operation.</summary>
        Add,
        /// <summary>Subtraction operation.</summary>
        Subtract,
        /// <summary>Multiplication operation.</summary>
        Multiply,
        /// <summary>Division operation.</summary>
        Divide,
        /// <summary>Greater than comparison.</summary>
        GreaterThan,
        /// <summary>Less than comparison.</summary>
        LessThan,
        /// <summary>Greater than or equal comparison.</summary>
        GreaterThanOrEqual,
        /// <summary>Less than or equal comparison.</summary>
        LessThanOrEqual,
        /// <summary>Equality comparison.</summary>
        Equal,
        /// <summary>Inequality comparison.</summary>
        NotEqual,
        /// <summary>Logical AND operation.</summary>
        And,
        /// <summary>Logical OR operation.</summary>
        Or
    }

    /// <summary>
    /// Represents a constant value in an expression.
    /// </summary>
    public class LitExpr : Expr
    {
        /// <summary>The constant value (e.g., int, string, double).</summary>
        public object? Value { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="LitExpr"/> class.
        /// </summary>
        /// <param name="value">The literal value.</param>
        public LitExpr(object? value)
        {
            Value = value;
        }
    }

    /// <summary>
    /// Represents a reference to a column by name.
    /// </summary>
    public class ColExpr : Expr
    {
        /// <summary>The name of the column.</summary>
        public string Name { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="ColExpr"/> class.
        /// </summary>
        /// <param name="name">The column name.</param>
        public ColExpr(string name)
        {
            Name = name;
        }
    }

    /// <summary>
    /// Represents a binary operation between two expressions.
    /// </summary>
    public class BinaryExpr : Expr
    {
        /// <summary>The left operand.</summary>
        public Expr Left { get; }

        /// <summary>The operation type.</summary>
        public BinaryOp Op { get; }

        /// <summary>The right operand.</summary>
        public Expr Right { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryExpr"/> class.
        /// </summary>
        /// <param name="left">The left operand.</param>
        /// <param name="op">The operation type.</param>
        /// <param name="right">The right operand.</param>
        public BinaryExpr(Expr left, BinaryOp op, Expr right)
        {
            Left = left;
            Op = op;
            Right = right;
        }
    }

    /// <summary>
    /// Represents an alias operation to rename a column or expression result.
    /// </summary>
    public class AliasExpr : Expr
    {
        /// <summary>The expression to alias.</summary>
        public Expr Child { get; }

        /// <summary>The new name.</summary>
        public string Alias { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AliasExpr"/> class.
        /// </summary>
        /// <param name="child">The child expression.</param>
        /// <param name="alias">The alias name.</param>
        public AliasExpr(Expr child, string alias)
        {
            Child = child;
            Alias = alias;
        }
    }

    /// <summary>
    /// Defines the supported aggregation operation types.
    /// </summary>
    public enum AggOpType
    {
        /// <summary>Summation of values.</summary>
        Sum,
        /// <summary>Find the minimum value.</summary>
        Min,
        /// <summary>Find the maximum value.</summary>
        Max,
        /// <summary>Calculate the arithmetic mean.</summary>
        Mean,
        /// <summary>Count the number of rows.</summary>
        Count
    }

    /// <summary>
    /// Represents an aggregation expression (e.g., Sum(Amount)).
    /// </summary>
    public class AggExpr : Expr
    {
        /// <summary>The type of aggregation.</summary>
        public AggOpType Op { get; }

        /// <summary>The expression to aggregate (usually a column).</summary>
        public Expr Child { get; }

        /// <summary>
        /// Initializes a new aggregation expression.
        /// </summary>
        public AggExpr(AggOpType op, Expr child)
        {
            Op = op;
            Child = child;
        }
    }
}