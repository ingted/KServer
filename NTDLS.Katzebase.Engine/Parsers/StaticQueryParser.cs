﻿using NTDLS.Katzebase.Client.Exceptions;
using NTDLS.Katzebase.Client.Types;
using NTDLS.Katzebase.Engine.Parsers.Query.Class;
using NTDLS.Katzebase.Engine.Parsers.Query.Class.Helpers;
using NTDLS.Katzebase.Engine.Parsers.Query.SupportingTypes;
using NTDLS.Katzebase.Engine.Parsers.Tokens;
using static NTDLS.Katzebase.Client.KbConstants;
using static NTDLS.Katzebase.Engine.Library.EngineConstants;

namespace NTDLS.Katzebase.Engine.Parsers
{
    internal class StaticQueryParser<TData> where TData : IStringable
    {
        /// <summary>
        /// Parse the query batch (a single query text containing multiple queries).
        /// </summary>
        /// <param name="queryText"></param>
        /// <param name="userParameters"></param>
        /// <returns></returns>
        static public QueryBatch<TData> ParseBatch(EngineCore<TData> core, string queryText, KbInsensitiveDictionary<KbConstant>? userParameters = null)
        {
            var tokenizerConstants = core.Query.KbGlobalConstants.Clone();

            userParameters ??= new();
            //If we have user parameters, add them to a clone of the global tokenizer constants.
            foreach (var param in userParameters)
            {
                tokenizerConstants.Add(param.Key, param.Value);
            }

            queryText = PreParseQueryVariableDeclarations(queryText, ref tokenizerConstants);

            var tokenizer = new Tokenizer<TData>(queryText, true, tokenizerConstants);
            var queryBatch = new QueryBatch<TData>(tokenizer.Literals);

            while (!tokenizer.IsExhausted())
            {
                int preParseTokenPosition = tokenizer.Caret;
                var preparedQuery = ParseQuery(queryBatch, tokenizer);

                var singleQueryText = tokenizer.Substring(preParseTokenPosition, tokenizer.Caret - preParseTokenPosition);
                preparedQuery.Hash = Library.Helpers.ComputeSHA256(singleQueryText);

                queryBatch.Add(preparedQuery);
            }

            return queryBatch;
        }

        /// <summary>
        /// Parse the single.
        /// </summary>
        static public PreparedQuery<TData> ParseQuery(QueryBatch<TData> queryBatch, Tokenizer<TData> tokenizer)
        {
            string token = tokenizer.GetNext();

            if (StaticParserUtility.IsStartOfQuery(token, out var queryType) == false)
            {
                throw new KbParserException($"Invalid query. Found [{token}], expected: [{string.Join("],[", Enum.GetValues<QueryType>().Where(o => o != QueryType.None))}].");
            }

            tokenizer.EatNext();

            return queryType switch
            {
                QueryType.Select => StaticParserSelect<TData>.Parse(queryBatch, tokenizer),
                QueryType.Delete => StaticParserDelete<TData>.Parse(queryBatch, tokenizer),
                QueryType.Insert => StaticParserInsert<TData>.Parse(queryBatch, tokenizer),
                QueryType.Update => StaticParserUpdate<TData>.Parse(queryBatch, tokenizer),
                QueryType.Begin => StaticParserBegin<TData>.Parse(queryBatch, tokenizer),
                QueryType.Commit => StaticParserCommit<TData>.Parse(queryBatch, tokenizer),
                QueryType.Rollback => StaticParserRollback<TData>.Parse(queryBatch, tokenizer),
                QueryType.Create => StaticParserCreate<TData>.Parse(queryBatch, tokenizer),
                QueryType.Drop => StaticParserDrop<TData>.Parse(queryBatch, tokenizer),

                QueryType.Sample => StaticParserSample<TData>.Parse(queryBatch, tokenizer),
                QueryType.Analyze => StaticParserAnalyze<TData>.Parse(queryBatch, tokenizer),
                QueryType.List => StaticParserList<TData>.Parse(queryBatch, tokenizer),
                QueryType.Alter => StaticParserAlter<TData>.Parse(queryBatch, tokenizer),
                QueryType.Rebuild => StaticParserRebuild<TData>.Parse(queryBatch, tokenizer),
                QueryType.Set => StaticParserSet<TData>.Parse(queryBatch, tokenizer),
                QueryType.Kill => StaticParserKill<TData>.Parse(queryBatch, tokenizer),
                QueryType.Exec => StaticParserExec<TData>.Parse(queryBatch, tokenizer),

                _ => throw new KbParserException($"The query type is not implemented: [{token}]."),
            };
        }

        /// <summary>
        /// Parse the variable declaration in the query and remove them from the query text.
        /// </summary>
        static string PreParseQueryVariableDeclarations(string queryText, ref KbInsensitiveDictionary<KbConstant> tokenizerConstants)
        {
            var lines = queryText.Split("\n", StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim());
            lines = lines.Where(o => o.StartsWith("declare", StringComparison.InvariantCultureIgnoreCase));

            foreach (var line in lines)
            {
                var lineTokenizer = new TokenizerSlim(line);

                if (!lineTokenizer.TryEatIsNextToken("declare", out var token))
                {
                    throw new KbParserException($"Invalid query. Found [{token}], expected: [declare].");
                }

                if (lineTokenizer.NextCharacter != '@')
                {
                    throw new KbParserException($"Invalid query. Found [{lineTokenizer.NextCharacter}], expected: [@].");
                }
                lineTokenizer.EatNextCharacter();

                if (lineTokenizer.TryEatValidateNextToken((o) => TokenizerExtensions.IsIdentifier(o), out var variableName) == false)
                {
                    throw new KbParserException($"Invalid query. Found [{token}], expected: [declare].");
                }

                if (lineTokenizer.NextCharacter != '=')
                {
                    throw new KbParserException($"Invalid query. Found [{lineTokenizer.NextCharacter}], expected: [=].");
                }
                lineTokenizer.EatNextCharacter();

                var variableValue = lineTokenizer.Remainder().Trim();

                KbBasicDataType variableType;
                if (variableValue.StartsWith('\'') && variableValue.EndsWith('\''))
                {
                    variableType = KbBasicDataType.String;
                    variableValue = variableValue.Substring(1, variableValue.Length - 2);
                }
                else
                {
                    variableType = KbBasicDataType.Numeric;
                    if (variableValue != null && double.TryParse(variableValue?.ToString(), out _) == false)
                    {
                        throw new Exception($"Non-string value of [{variableName}] cannot be converted to numeric.");
                    }
                }

                tokenizerConstants.Add($"@{variableName}", new KbConstant(variableValue, variableType));

                queryText = queryText.Replace(line, "");
            }

            return queryText.Trim();
        }

    }
}
