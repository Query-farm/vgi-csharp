// Copyright 2025, 2026 Query Farm LLC - https://query.farm
using System.Globalization;
using System.Numerics;
using System.Text;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// A small parser for the bounded DuckDB scalar grammars the extension accepts in canonical
/// metadata: CHECK records (<c>ParseNumericCheck</c> in <c>vgi_constraint_contract.cpp</c>) and
/// VIRTUAL generated columns (<c>Scalar(..., generated = true)</c> in
/// <c>vgi_column_expression.cpp</c>). It never evaluates SQL; it parses just enough to reject what
/// the extension would reject at ATTACH, with an error naming the construct. Anything it does not
/// understand is outside both grammars and is rejected.
/// </summary>
internal static class ScalarSqlGrammar
{
    internal enum ConstantType { Null, Boolean, Integer, BigInt, HugeInt, Decimal, Double, Varchar }

    internal abstract record Node;

    internal sealed record Constant(ConstantType Type) : Node;

    internal sealed record ColumnRef(IReadOnlyList<string> Names) : Node;

    internal sealed record Cast(Node Child, IReadOnlyList<string> TypeName, bool Try) : Node;

    internal sealed record Comparison(string Operator, Node Left, Node Right) : Node;

    internal sealed record Conjunction(string Operator, Node Left, Node Right) : Node;

    /// <summary>NOT, IS [NOT] NULL, COALESCE.</summary>
    internal sealed record Operator(string Name, IReadOnlyList<Node> Children) : Node;

    internal sealed record Function(IReadOnlyList<string> Name, IReadOnlyList<Node> Arguments) : Node;

    private static readonly HashSet<string> Unsupported = new(StringComparer.OrdinalIgnoreCase)
    {
        "SELECT", "FROM", "WHERE", "CASE", "WHEN", "THEN", "ELSE", "END", "IN", "BETWEEN", "LIKE", "ILIKE",
        "SIMILAR", "GLOB", "EXISTS", "INTERVAL", "COLLATE", "DISTINCT", "ORDER", "GROUP", "HAVING", "LIMIT",
        "AS", "ANY", "ALL", "SOME", "ARRAY", "STRUCT", "MAP", "LAMBDA", "UNION", "WITH",
    };

    private static readonly HashSet<string> InfixKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "IN", "BETWEEN", "LIKE", "ILIKE", "SIMILAR", "GLOB", "COLLATE",
    };

    /// <summary>Parses <paramref name="sql"/> as exactly one scalar expression. Throws
    /// <see cref="FormatException"/> for anything outside the parser's grammar.</summary>
    internal static Node Parse(string sql)
    {
        var parser = new Parser(Tokenize(sql));
        var node = parser.Expression(0);
        if (!parser.AtEnd)
            throw new FormatException($"unexpected '{parser.Peek.Text}' after the expression");
        return node;
    }

    /// <summary>The children of <paramref name="node"/>, for grammar walks.</summary>
    internal static IEnumerable<Node> Children(Node node) => node switch
    {
        Cast cast => [cast.Child],
        Comparison comparison => [comparison.Left, comparison.Right],
        Conjunction conjunction => [conjunction.Left, conjunction.Right],
        Operator op => op.Children,
        Function function => function.Arguments,
        _ => [],
    };

    // ---------------------------------------------------------------- tokens

    private enum Kind { Identifier, QuotedIdentifier, String, Number, Symbol, End }

    private sealed record Token(Kind Kind, string Text);

    private static List<Token> Tokenize(string sql)
    {
        var tokens = new List<Token>();
        var i = 0;
        while (i < sql.Length)
        {
            var c = sql[i];
            if (char.IsWhiteSpace(c))
            {
                i++;
                continue;
            }

            if (c == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
                throw new FormatException("comments are unsupported");
            if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
                throw new FormatException("comments are unsupported");
            if (char.IsLetter(c) || c == '_')
            {
                var start = i;
                while (i < sql.Length && (char.IsLetterOrDigit(sql[i]) || sql[i] is '_' or '$')) i++;
                tokens.Add(new Token(Kind.Identifier, sql[start..i]));
                continue;
            }

            if (c is '"' or '\'')
            {
                var text = new StringBuilder();
                i++;
                while (true)
                {
                    if (i >= sql.Length) throw new FormatException("unterminated quoted text");
                    if (sql[i] == c)
                    {
                        if (i + 1 < sql.Length && sql[i + 1] == c)
                        {
                            text.Append(c);
                            i += 2;
                            continue;
                        }

                        i++;
                        break;
                    }

                    text.Append(sql[i++]);
                }

                if (c == '"' && text.Length == 0) throw new FormatException("empty quoted identifier");
                tokens.Add(new Token(c == '"' ? Kind.QuotedIdentifier : Kind.String, text.ToString()));
                continue;
            }

            if (char.IsDigit(c) || (c == '.' && i + 1 < sql.Length && char.IsDigit(sql[i + 1])))
            {
                var start = i;
                while (i < sql.Length && char.IsDigit(sql[i])) i++;
                if (i < sql.Length && sql[i] == '.')
                {
                    i++;
                    while (i < sql.Length && char.IsDigit(sql[i])) i++;
                }

                if (i < sql.Length && sql[i] is 'e' or 'E')
                {
                    var save = i;
                    i++;
                    if (i < sql.Length && sql[i] is '+' or '-') i++;
                    if (i < sql.Length && char.IsDigit(sql[i]))
                        while (i < sql.Length && char.IsDigit(sql[i])) i++;
                    else
                        i = save;
                }

                if (i < sql.Length && (char.IsLetter(sql[i]) || sql[i] == '_'))
                    throw new FormatException($"unsupported numeric literal near '{sql[start..(i + 1)]}'");
                tokens.Add(new Token(Kind.Number, sql[start..i]));
                continue;
            }

            string[] symbols = ["::", "<=", ">=", "<>", "!=", "==", "||", ":=", "=>", "(", ")", ",", "+", "-", "*", "/", "%",
                "<", ">", "=", ".", "^", "!", "~", "&", "|", "[", "]", "{", "}", "?", "$", ":", ";", "@", "#"];
            var symbol = symbols.FirstOrDefault(s => string.CompareOrdinal(sql, i, s, 0, s.Length) == 0)
                ?? throw new FormatException($"unexpected character '{c}'");
            tokens.Add(new Token(Kind.Symbol, symbol));
            i += symbol.Length;
        }

        tokens.Add(new Token(Kind.End, ""));
        return tokens;
    }

    // ---------------------------------------------------------------- parser

    private sealed class Parser(List<Token> tokens)
    {
        private int _position;

        public Token Peek => tokens[_position];

        public bool AtEnd => Peek.Kind == Kind.End;

        private Token Next() => tokens[_position++];

        private bool IsSymbol(string text) => Peek.Kind == Kind.Symbol && Peek.Text == text;

        private bool IsKeyword(string text) => Peek.Kind == Kind.Identifier && Peek.Text.Equals(text, StringComparison.OrdinalIgnoreCase);

        private bool IsKeywordAt(int offset, string text) =>
            tokens[Math.Min(_position + offset, tokens.Count - 1)] is { Kind: Kind.Identifier } token
            && token.Text.Equals(text, StringComparison.OrdinalIgnoreCase);

        private void Expect(string symbol)
        {
            if (!IsSymbol(symbol)) throw new FormatException($"expected '{symbol}' but found '{Peek.Text}'");
            _position++;
        }

        private void ExpectKeyword(string keyword)
        {
            if (!IsKeyword(keyword)) throw new FormatException($"expected {keyword} but found '{Peek.Text}'");
            _position++;
        }

        // Precedence (PostgreSQL/DuckDB): OR < AND < NOT < IS < comparison < other operators (||)
        // < + - < * / % < unary < ::.
        public Node Expression(int depth)
        {
            Guard(depth);
            var left = And(depth + 1);
            while (IsKeyword("OR"))
            {
                _position++;
                left = new Conjunction("OR", left, And(depth + 1));
            }

            return left;
        }

        private Node And(int depth)
        {
            Guard(depth);
            var left = Not(depth + 1);
            while (IsKeyword("AND"))
            {
                _position++;
                left = new Conjunction("AND", left, Not(depth + 1));
            }

            return left;
        }

        private Node Not(int depth)
        {
            Guard(depth);
            if (IsKeyword("NOT"))
            {
                _position++;
                return new Operator("NOT", [Not(depth + 1)]);
            }

            return Is(depth + 1);
        }

        private Node Is(int depth)
        {
            Guard(depth);
            var left = Comparison(depth + 1);
            while (true)
            {
                if (IsKeyword("ISNULL") || IsKeyword("NOTNULL"))
                {
                    left = new Operator(Next().Text.Equals("ISNULL", StringComparison.OrdinalIgnoreCase) ? "IS NULL" : "IS NOT NULL", [left]);
                    continue;
                }

                if (!IsKeyword("IS")) return left;
                _position++;
                var negated = IsKeyword("NOT");
                if (negated) _position++;
                if (!IsKeyword("NULL"))
                    throw new FormatException($"IS {(negated ? "NOT " : "")}{Peek.Text} is unsupported");
                _position++;
                left = new Operator(negated ? "IS NOT NULL" : "IS NULL", [left]);
            }
        }

        private static readonly Dictionary<string, string> Comparisons = new(StringComparer.Ordinal)
        {
            ["="] = "=",
            ["=="] = "=",
            ["<>"] = "<>",
            ["!="] = "<>",
            ["<"] = "<",
            [">"] = ">",
            ["<="] = "<=",
            [">="] = ">=",
        };

        private Node Comparison(int depth)
        {
            Guard(depth);
            var left = Concat(depth + 1);
            if (Peek.Kind == Kind.Symbol && Comparisons.TryGetValue(Peek.Text, out var op))
            {
                _position++;
                var right = Concat(depth + 1);
                if (Peek.Kind == Kind.Symbol && Comparisons.ContainsKey(Peek.Text))
                    throw new FormatException("chained comparisons are unsupported");
                return new Comparison(op, left, right);
            }

            if (IsKeyword("NOT") && (IsKeywordAt(1, "IN") || IsKeywordAt(1, "BETWEEN") || IsKeywordAt(1, "LIKE")
                                     || IsKeywordAt(1, "ILIKE") || IsKeywordAt(1, "SIMILAR") || IsKeywordAt(1, "GLOB")))
                throw new FormatException($"NOT {tokens[_position + 1].Text.ToUpperInvariant()} is unsupported");
            if (Peek.Kind == Kind.Identifier && InfixKeywords.Contains(Peek.Text))
                throw new FormatException($"{Peek.Text.ToUpperInvariant()} is unsupported");
            return left;
        }

        private Node Concat(int depth)
        {
            Guard(depth);
            var left = Additive(depth + 1);
            while (IsSymbol("||"))
            {
                _position++;
                left = new Function(["||"], [left, Additive(depth + 1)]);
            }

            return left;
        }

        private Node Additive(int depth)
        {
            Guard(depth);
            var left = Multiplicative(depth + 1);
            while (IsSymbol("+") || IsSymbol("-"))
            {
                var op = Next().Text;
                left = new Function([op], [left, Multiplicative(depth + 1)]);
            }

            return left;
        }

        private Node Multiplicative(int depth)
        {
            Guard(depth);
            var left = Unary(depth + 1);
            while (IsSymbol("*") || IsSymbol("/") || IsSymbol("%"))
            {
                var op = Next().Text;
                left = new Function([op], [left, Unary(depth + 1)]);
            }

            if (IsSymbol("^")) throw new FormatException("'^' is unsupported");
            return left;
        }

        private Node Unary(int depth)
        {
            Guard(depth);
            if (IsSymbol("-") || IsSymbol("+"))
            {
                var op = Next().Text;
                var child = Unary(depth + 1);
                // DuckDB's transformer folds a negated numeric literal into a constant.
                if (op == "-" && child is Constant { Type: not (ConstantType.Null or ConstantType.Boolean or ConstantType.Varchar) })
                    return child;
                return new Function([op], [child]);
            }

            return Postfix(depth + 1);
        }

        private Node Postfix(int depth)
        {
            Guard(depth);
            var node = Primary(depth + 1);
            while (IsSymbol("::"))
            {
                _position++;
                node = new Cast(node, TypeName(), false);
            }

            if (IsSymbol("[")) throw new FormatException("subscripts are unsupported");
            return node;
        }

        private Node Primary(int depth)
        {
            Guard(depth);
            var token = Peek;
            switch (token.Kind)
            {
                case Kind.Number:
                    _position++;
                    return new Constant(NumberType(token.Text));
                case Kind.String:
                    _position++;
                    return new Constant(ConstantType.Varchar);
                case Kind.Symbol when token.Text == "(":
                    {
                        _position++;
                        var inner = Expression(depth + 1);
                        if (IsSymbol(",")) throw new FormatException("row expressions are unsupported");
                        Expect(")");
                        return inner;
                    }
                case Kind.QuotedIdentifier:
                case Kind.Identifier:
                    return Name(depth);
                default:
                    throw new FormatException(token.Kind == Kind.End ? "unexpected end of expression" : $"unexpected '{token.Text}'");
            }
        }

        private Node Name(int depth)
        {
            var first = Peek;
            if (first.Kind == Kind.Identifier)
            {
                var word = first.Text.ToUpperInvariant();
                switch (word)
                {
                    case "NULL":
                        _position++;
                        return new Constant(ConstantType.Null);
                    case "TRUE":
                    case "FALSE":
                        _position++;
                        return new Constant(ConstantType.Boolean);
                    case "CAST":
                    case "TRY_CAST":
                        {
                            _position++;
                            Expect("(");
                            var child = Expression(depth + 1);
                            ExpectKeyword("AS");
                            var type = TypeName();
                            Expect(")");
                            return new Cast(child, type, word == "TRY_CAST");
                        }
                    case "COALESCE" when tokens[_position + 1] is { Kind: Kind.Symbol, Text: "(" }:
                        {
                            _position += 2;
                            var arguments = Arguments(depth);
                            return new Operator("COALESCE", arguments);
                        }
                }

                if (Unsupported.Contains(first.Text) || word is "AND" or "OR" or "NOT" or "IS")
                    throw new FormatException($"{word} is unsupported here");
            }

            var names = new List<string> { Next().Text };
            while (IsSymbol("."))
            {
                _position++;
                if (Peek.Kind is not (Kind.Identifier or Kind.QuotedIdentifier))
                    throw new FormatException("qualified names must be identifiers");
                names.Add(Next().Text);
            }

            if (Peek.Kind == Kind.String)
                throw new FormatException($"typed literals ({string.Join('.', names)} '...') are unsupported");
            if (!IsSymbol("(")) return new ColumnRef(names);
            _position++;
            if (IsKeyword("DISTINCT") || IsKeyword("ALL"))
                throw new FormatException("DISTINCT/ALL function arguments are unsupported");
            return new Function(names, Arguments(depth));
        }

        private List<Node> Arguments(int depth)
        {
            var arguments = new List<Node>();
            if (IsSymbol(")"))
            {
                _position++;
                return arguments;
            }

            while (true)
            {
                if (IsSymbol("*")) throw new FormatException("'*' arguments are unsupported");
                if (tokens[_position + 1] is { Kind: Kind.Symbol, Text: ":=" or "=>" })
                    throw new FormatException("named arguments are unsupported");
                arguments.Add(Expression(depth + 1));
                if (IsKeyword("ORDER")) throw new FormatException("ordered aggregate arguments are unsupported");
                if (IsSymbol(","))
                {
                    _position++;
                    continue;
                }

                Expect(")");
                if (IsKeyword("FILTER") || IsKeyword("OVER"))
                    throw new FormatException($"{Peek.Text.ToUpperInvariant()} clauses are unsupported");
                return arguments;
            }
        }

        private List<string> TypeName()
        {
            if (Peek.Kind is not (Kind.Identifier or Kind.QuotedIdentifier))
                throw new FormatException($"expected a type name but found '{Peek.Text}'");
            var names = new List<string> { Next().Text };
            while (IsSymbol("."))
            {
                _position++;
                names.Add(Next().Text);
            }

            // DOUBLE PRECISION / CHARACTER VARYING spellings.
            if (names.Count == 1 && Peek.Kind == Kind.Identifier
                && (names[0].Equals("DOUBLE", StringComparison.OrdinalIgnoreCase) && IsKeyword("PRECISION")
                    || names[0].Equals("CHARACTER", StringComparison.OrdinalIgnoreCase) && IsKeyword("VARYING")))
                names[0] += " " + Next().Text;
            if (IsSymbol("("))
            {
                // Width/precision modifiers, e.g. VARCHAR(10) or DECIMAL(18, 3).
                _position++;
                while (!IsSymbol(")"))
                {
                    if (Peek.Kind != Kind.Number && !IsSymbol(",")) throw new FormatException("invalid type modifier");
                    _position++;
                }

                _position++;
            }

            if (IsSymbol("[")) throw new FormatException("array types are unsupported");
            return names;
        }

        private static void Guard(int depth)
        {
            if (depth > 512) throw new FormatException("expression is too deeply nested");
        }
    }

    private static ConstantType NumberType(string text)
    {
        if (text.Contains('e') || text.Contains('E')) return ConstantType.Double;
        if (text.Contains('.')) return ConstantType.Decimal;
        var value = BigInteger.Parse(text, CultureInfo.InvariantCulture);
        return value <= int.MaxValue ? ConstantType.Integer
            : value <= long.MaxValue ? ConstantType.BigInt
            : ConstantType.HugeInt;
    }

    /// <summary>The DuckDB type id a (single-component) cast type name binds to, or
    /// <see langword="null"/> when it is not one of the scalar primitives either grammar knows.</summary>
    internal static string? PrimitiveTypeName(IReadOnlyList<string> name)
    {
        if (name.Count != 1) return null;
        return name[0].ToUpperInvariant() switch
        {
            "BOOLEAN" or "BOOL" or "LOGICAL" => "BOOLEAN",
            "TINYINT" or "INT1" => "TINYINT",
            "SMALLINT" or "INT2" or "SHORT" => "SMALLINT",
            "INTEGER" or "INT" or "INT4" or "SIGNED" => "INTEGER",
            "BIGINT" or "INT8" or "LONG" => "BIGINT",
            "VARCHAR" or "TEXT" or "STRING" or "CHAR" or "BPCHAR" or "NVARCHAR" or "CHARACTER VARYING" => "VARCHAR",
            "FLOAT" or "FLOAT4" or "REAL" => "FLOAT",
            "DOUBLE" or "FLOAT8" or "DOUBLE PRECISION" => "DOUBLE",
            "DECIMAL" or "NUMERIC" => "DECIMAL",
            _ => null,
        };
    }
}
