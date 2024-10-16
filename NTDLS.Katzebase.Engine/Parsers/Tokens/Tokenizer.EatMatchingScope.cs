﻿namespace NTDLS.Katzebase.Engine.Parsers.Tokens
{
    internal partial class Tokenizer<TData> where TData : IStringable
    {
        /// <summary>
        /// Matches scope using open and close parentheses and skips the entire scope.
        /// </summary>
        public string EatMatchingScope()
            => EatGetMatchingScope('(', ')');

        /// <summary>
        /// Matches scope using the given open and close values and skips the entire scope.
        /// </summary>
        public string EatMatchingScope(char open, char close)
            => EatGetMatchingScope(open, close);
    }
}
