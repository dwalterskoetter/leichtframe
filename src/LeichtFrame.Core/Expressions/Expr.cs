namespace LeichtFrame.Core.Expressions
{
    /// <summary>
    /// Represents the abstract base class for all nodes in the expression tree.
    /// Supports operator overloading to enable a fluent syntax.
    /// </summary>
    public abstract class Expr
    {
        // --- Arithmetic Operators ---

        /// <summary>Creates a generic addition expression.</summary>
        public static Expr operator +(Expr left, Expr right) => new BinaryExpr(left, BinaryOp.Add, right);

        /// <summary>Creates a generic subtraction expression.</summary>
        public static Expr operator -(Expr left, Expr right) => new BinaryExpr(left, BinaryOp.Subtract, right);

        /// <summary>Creates a generic multiplication expression.</summary>
        public static Expr operator *(Expr left, Expr right) => new BinaryExpr(left, BinaryOp.Multiply, right);

        /// <summary>Creates a generic division expression.</summary>
        public static Expr operator /(Expr left, Expr right) => new BinaryExpr(left, BinaryOp.Divide, right);

        // --- Comparison Operators ---

        /// <summary>Creates a generic greater-than expression.</summary>
        public static Expr operator >(Expr left, Expr right) => new BinaryExpr(left, BinaryOp.GreaterThan, right);

        /// <summary>Creates a generic less-than expression.</summary>
        public static Expr operator <(Expr left, Expr right) => new BinaryExpr(left, BinaryOp.LessThan, right);

        /// <summary>Creates a generic greater-than-or-equal expression.</summary>
        public static Expr operator >=(Expr left, Expr right) => new BinaryExpr(left, BinaryOp.GreaterThanOrEqual, right);

        /// <summary>Creates a generic less-than-or-equal expression.</summary>
        public static Expr operator <=(Expr left, Expr right) => new BinaryExpr(left, BinaryOp.LessThanOrEqual, right);

        /// <summary>Creates a generic equality expression (AST node).</summary>
        public static Expr operator ==(Expr left, Expr right)
        {
            // Null-Safety for DSL usage
            if (ReferenceEquals(left, null)) return new LitExpr(null);
            if (ReferenceEquals(right, null)) return new LitExpr(null);
            return new BinaryExpr(left, BinaryOp.Equal, right);
        }

        /// <summary>Creates a generic inequality expression (AST node).</summary>
        public static Expr operator !=(Expr left, Expr right)
        {
            if (ReferenceEquals(left, null)) return new LitExpr(null);
            if (ReferenceEquals(right, null)) return new LitExpr(null);
            return new BinaryExpr(left, BinaryOp.NotEqual, right);
        }

        // --- Boolean Logic ---

        /// <summary>Creates a logical AND expression.</summary>
        public static Expr operator &(Expr left, Expr right) => new BinaryExpr(left, BinaryOp.And, right);

        /// <summary>Creates a logical OR expression.</summary>
        public static Expr operator |(Expr left, Expr right) => new BinaryExpr(left, BinaryOp.Or, right);

        // --- Implicit Conversions ---

        /// <summary>Implicitly converts an int to a literal expression.</summary>
        public static implicit operator Expr(int v) => new LitExpr(v);

        /// <summary>Implicitly converts a double to a literal expression.</summary>
        public static implicit operator Expr(double v) => new LitExpr(v);

        /// <summary>Implicitly converts a string to a literal expression.</summary>
        public static implicit operator Expr(string v) => new LitExpr(v);

        /// <summary>Implicitly converts a bool to a literal expression.</summary>
        public static implicit operator Expr(bool v) => new LitExpr(v);

        // --- Fluent Methods ---

        /// <summary>
        /// Assigns an alias (new name) to the current expression.
        /// </summary>
        /// <param name="alias">The name to assign.</param>
        /// <returns>An <see cref="AliasExpr"/> wrapping this expression.</returns>
        public Expr As(string alias) => new AliasExpr(this, alias);

        // Required overrides to suppress compiler warnings when overloading ==/!=
        /// <inheritdoc />
        public override bool Equals(object? obj) => ReferenceEquals(this, obj);
        /// <inheritdoc />
        public override int GetHashCode() => base.GetHashCode();
    }
}