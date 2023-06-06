﻿using Katzebase.Engine.KbLib;
using Katzebase.Engine.Query.Condition;
using Katzebase.Engine.Query.Tokenizers;
using Katzebase.PublicLibrary;
using Katzebase.PublicLibrary.Exceptions;
using static Katzebase.Engine.KbLib.EngineConstants;

namespace Katzebase.Engine.Query
{
    public class ParserEngine
    {
        static public PreparedQuery ParseQuery(string queryText)
        {
            PreparedQuery result = new PreparedQuery();


            var query = new QueryTokenizer(queryText);

            string token = string.Empty;

            token = query.GetNextToken().ToLower();

            var queryType = QueryType.Select;

            if (Enum.TryParse<QueryType>(token, true, out queryType) == false)
            {
                throw new KbParserException("Invalid query. Found [" + token + "], expected select, insert, update or delete.");
            }

            result.QueryType = queryType;

            #region Delete.
            //--------------------------------------------------------------------------------------------------------------------------------------------
            if (queryType == QueryType.Delete)
            {
                /*
                token = query.GetNextToken();
                if (token.ToLower() == "top")
                {
                    token = query.GetNextToken();
                    int rowLimit = 0;

                    if (Int32.TryParse(token, out rowLimit) == false)
                    {
                        throw new KbParserException("Invalid query. Found [" + token + "], expected numeric row limit.");
                    }

                    result.RowLimit = rowLimit;

                    //Get schema name:
                    token = query.GetNextToken();
                }

                if (token == string.Empty || Utilities.IsValidIdentifier(token, "/\\") == false)
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected schema name.");
                }
                result.Schema = token;

                token = query.GetNextToken();
                if (token.ToLower() == "where")
                {
                    string conditionText = query.Substring(position).Trim();
                    if (conditionText == string.Empty)
                    {
                        throw new KbParserException("Invalid query. Found [" + token + "], expected list of conditions.");
                    }

                    result.Conditions = ParseConditions(conditionText, literalStrings);
                }
                else if (token != string.Empty)
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected end of statement.");
                }
                */
            }
            #endregion
            #region Update.
            //--------------------------------------------------------------------------------------------------------------------------------------------
            else if (queryType == QueryType.Update)
            {
                /*
                token = query.GetNextToken();
                if (token.ToLower() == "top")
                {
                    token = query.GetNextToken();
                    int rowLimit = 0;

                    if (Int32.TryParse(token, out rowLimit) == false)
                    {
                        throw new KbParserException("Invalid query. Found [" + token + "], expected numeric row limit.");
                    }

                    result.RowLimit = rowLimit;

                    //Get schema name:
                    token = query.GetNextToken();
                }

                if (token == string.Empty || Utilities.IsValidIdentifier(token, "/\\") == false)
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected schema name.");
                }
                result.Schema = token;

                token = query.GetNextToken();
                if (token.ToLower() != "set")
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected [SET].");
                }

                result.UpsertKeyValuePairs = ParseUpsertKeyValues(query, ref position);

                token = query.GetNextToken();
                if (token != string.Empty && token.ToLower() != "where")
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected [WHERE] or end of statement.");
                }

                if (token.ToLower() == "where")
                {
                    string conditionText = query.Substring(position).Trim();
                    if (conditionText == string.Empty)
                    {
                        throw new KbParserException("Invalid query. Found [" + token + "], expected list of conditions.");
                    }

                    result.Conditions = ParseConditions(conditionText, literalStrings);
                }
                else if (token != string.Empty)
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected end of statement.");
                }
                */
            }
            #endregion
            #region Select.
            //--------------------------------------------------------------------------------------------------------------------------------------------
            else if (queryType == QueryType.Select)
            {
                token = query.PeekNextToken();
                if (token.ToLower() == "top")
                {
                    query.SkipNextToken();
                    result.RowLimit = query.GetNextTokenAsInt();
                }

                while (true)
                {
                    token = query.GetNextToken(); //Grab thr next field name (or FROM).

                    if (token.ToLower() == "from" || token == string.Empty)
                    {
                        break;
                    }

                    string fieldSchemaAlias = string.Empty;

                    if (token.Contains('.'))
                    {
                        var splitTok = token.Split('.');
                        fieldSchemaAlias = splitTok[0];
                        token = splitTok[1];
                    }

                    if (TokenHelpers.IsValidIdentifier(token) == false && token != "*")
                    {
                        throw new KbParserException("Invalid token. Found [" + token + "] a valid identifier.");
                    }

                    result.SelectFields.Add(new QueryField(token, fieldSchemaAlias, token));
                }

                if (token.ToLower() != "from")
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected [FROM].");
                }

                string sourceSchema = query.GetNextToken();
                string schemaAlias = string.Empty;
                if (sourceSchema == string.Empty || TokenHelpers.IsValidIdentifier(sourceSchema, ":") == false)
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected schema name.");
                }

                token = query.PeekNextToken();
                if (token.ToLower() == "as")
                {
                    query.SkipNextToken();
                    schemaAlias = query.GetNextToken();
                }

                result.Schemas.Add(new QuerySchema(sourceSchema, schemaAlias));

                token = query.GetNextToken();
                if (token != string.Empty && token.ToLower() != "where")
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected [WHERE] or end of statement.");
                }

                if (token.ToLower() == "where")
                {
                    string conditionText = query.Remainder();
                    if (conditionText == string.Empty)
                    {
                        throw new KbParserException("Invalid query. Found [" + token + "], expected list of conditions.");
                    }

                    result.Conditions = Conditions.Parse(conditionText, query.LiteralStrings);
                }
                else if (token != string.Empty)
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected end of statement.");
                }
            }
            #endregion
            #region Set.
            //--------------------------------------------------------------------------------------------------------------------------------------------
            else if (queryType == QueryType.Set)
            {
                //Variable 
                string variableName = query.GetNextToken();
                string variableValue = query.Remainder();
                result.VariableValues.Add(new KbNameValuePair(variableName, variableValue));
            }
            #endregion

            if (result.UpsertKeyValuePairs != null)
            {
                foreach (var kvp in result.UpsertKeyValuePairs.Collection)
                {
                    Utility.EnsureNotNull(kvp.Key);
                    Utility.EnsureNotNull(kvp.Value?.ToString());

                    if (query.LiteralStrings.ContainsKey(kvp.Value.ToString()))
                    {
                        kvp.Value.Value = query.LiteralStrings[kvp.Value.ToString()];
                    }
                }
            }

            return result;
        }

        /*
        private static UpsertKeyValues ParseUpsertKeyValues(string conditionsText, ref int position)
        {
            UpsertKeyValues keyValuePairs = new UpsertKeyValues();
            int beforeTokenPosition;

            while (true)
            {
                string token;
                beforeTokenPosition = position;
                if ((token = Utilities.GetNextToken(conditionsText, ref position)) == string.Empty)
                {
                    if (keyValuePairs.Collection.Count > 0)
                    {
                        break; //Completed successfully.
                    }
                    throw new KbParserException("Invalid query. Unexpexted end of query found.");
                }

                if (token.ToLower() == "where")
                {
                    position = beforeTokenPosition;
                    break; //Completed successfully.
                }

                var keyValue = new UpsertKeyValue();

                if (token == string.Empty || Utilities.IsValidIdentifier(token) == false)
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected identifier name.");
                }
                keyValue.Key = token;

                token = Utilities.GetNextToken(conditionsText, ref position);
                if (token != "=")
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected [=].");
                }

                if ((token = Utilities.GetNextToken(conditionsText, ref position)) == string.Empty)
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected condition value.");
                }
                keyValue.Value.Value = token;

                keyValuePairs.Collection.Add(keyValue);
            }

            return keyValuePairs;
        }
        */

        /// <summary>
        /// Extraxts the condition text between parentheses.
        /// </summary>
        /// <param name="conditionsText"></param>
        /// <param name="endPosition"></param>
        /// <returns></returns>
        private static string GetConditionGroupExpression(string conditionsText, out int endPosition)
        {
            string resultingExpression = string.Empty;
            int position = 0;
            int nestLevel = 0;
            string token;

            while (true)
            {
                if ((token = ConditionTokenizer.GetNextClauseToken(conditionsText, ref position)) == string.Empty)
                {
                    break;
                }

                if (token == "(")
                {
                    nestLevel++;
                }
                else if (token == ")")
                {
                    nestLevel--;

                    if (nestLevel <= 0)
                    {
                        resultingExpression += $"{token} ";
                        break;
                    }
                }

                resultingExpression += $"{token} ";
            }

            resultingExpression = resultingExpression.Replace("( ", "(").Replace(" (", "(").Replace(") ", ")").Replace(" )", ")").Trim();

            endPosition = position;

            return resultingExpression;

        }
    }
}
