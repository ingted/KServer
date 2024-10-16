﻿namespace NTDLS.Katzebase.Engine.Parsers.Tokens
{
    internal partial class Tokenizer<TData> where TData : IStringable
    {
        public bool TryEatIfNextCharacter(char character)
        {
            RecordBreadcrumb();
            if (NextCharacter == character)
            {
                _caret++;
                InternalEatWhiteSpace();
                return true;
            }
            return false;
        }
    }
}
