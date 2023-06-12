﻿using Katzebase.Engine.KbLib;
using Katzebase.Engine.Query.Constraints;
using Katzebase.Engine.Query.Tokenizers;
using Katzebase.PublicLibrary;
using Katzebase.PublicLibrary.Exceptions;
using static Katzebase.Engine.KbLib.EngineConstants;
using static Katzebase.PublicLibrary.Constants;

namespace Katzebase.Engine.Query
{
    internal class ParserEngine
    {
        static public PreparedQuery ParseQuery(string queryText)
        {
            PreparedQuery result = new PreparedQuery();

            var query = new QueryTokenizer(queryText);

            string token = string.Empty;

            token = query.GetNextToken().ToLower();

            if (Enum.TryParse<QueryType>(token, true, out QueryType queryType) == false)
            {
                throw new KbParserException("Invalid query. Found [" + token + "], expected select, insert, update or delete.");
            }

            result.QueryType = queryType;

            //--------------------------------------------------------------------------------------------------------------------------------------------
            #region Delete.
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
            //--------------------------------------------------------------------------------------------------------------------------------------------
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
            //--------------------------------------------------------------------------------------------------------------------------------------------
            #region Sample.
            else if (queryType == QueryType.Sample)
            {
                if (query.IsNextToken("documents") == false)
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected [documents].");
                }

                token = query.GetNextToken();
                if (Enum.TryParse<SubQueryType>(token, true, out SubQueryType subQueryType) == false)
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected select, insert, update or delete.");
                }
                result.SubQueryType = subQueryType;

                token = query.GetNextToken();
                if (token == string.Empty)
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected schema name.");
                }

                result.Schemas.Add(new QuerySchema(token));

                token = query.GetNextToken();
                if (token != string.Empty)
                {
                    if (int.TryParse(token, out int topCount) == false)
                    {
                        throw new KbParserException("Invalid query. Found [" + token + "], expected top count.");
                    }
                    result.RowLimit = topCount;
                }

                if (query.IsEnd() == false)
                {
                    throw new KbParserException("Invalid query. Found [" + query.PeekNextToken() + "], expected end of statement.");
                }
            }
            #endregion
            //--------------------------------------------------------------------------------------------------------------------------------------------
            #region List.
            else if (queryType == QueryType.List)
            {
                if (query.IsNextToken(new string[] { "documents", "schemas" }) == false )
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected [documents, schemas].");
                }

                token = query.GetNextToken();
                if (Enum.TryParse<SubQueryType>(token, true, out SubQueryType subQueryType) == false)
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected select, insert, update or delete.");
                }
                result.SubQueryType = subQueryType;

                token = query.GetNextToken();
                if (token == string.Empty)
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected schema name.");
                }

                result.Schemas.Add(new QuerySchema(token));

                token = query.GetNextToken();
                if (token != string.Empty)
                {
                    if (int.TryParse(token, out int topCount) == false)
                    {
                        throw new KbParserException("Invalid query. Found [" + token + "], expected top count.");
                    }
                    result.RowLimit = topCount;
                }

                if (query.IsEnd() == false)
                {
                    throw new KbParserException("Invalid query. Found [" + query.PeekNextToken() + "], expected end of statement.");
                }
            }
            #endregion
            //--------------------------------------------------------------------------------------------------------------------------------------------
            #region Select.
            else if (queryType == QueryType.Select)
            {
                if (query.IsNextToken("top"))
                {
                    query.SkipNextToken();
                    result.RowLimit = query.GetNextTokenAsInt();
                }

                while (true)
                {
                    if (query.IsNextToken("from"))
                    {
                        break;
                    }

                    string fieldName = query.GetNextToken();
                    if (string.IsNullOrWhiteSpace(fieldName))
                    {
                        throw new KbParserException("Invalid token. Found whitespace expected identifer.");
                    }

                    string fieldPrefix = string.Empty;
                    string fieldAlias;


                    if (fieldName.Contains('.'))
                    {
                        var splitTok = fieldName.Split('.');
                        fieldPrefix = splitTok[0];
                        fieldName = splitTok[1];
                    }

                    query.SwapFieldLiteral(ref fieldName);

                    if (TokenHelpers.IsValidIdentifier(fieldName) == false && fieldName != "*")
                    {
                        throw new KbParserException("Invalid token identifier [" + fieldName + "].");
                    }

                    if (query.IsNextToken("as"))
                    {
                        query.SkipNextToken();
                        fieldAlias = query.GetNextToken();
                    }
                    else
                    {
                        fieldAlias = fieldName;
                    }

                    query.SwapFieldLiteral(ref fieldAlias);

                    result.SelectFields.Add(fieldPrefix, fieldName, fieldAlias);

                    if (query.IsCurrentChar(','))
                    {
                        query.SkipDelimiters();
                    }
                    else
                    {
                        //We should have found a delimiter here, if not either we are done parsing or the query is malformed. The next check will find out.
                        break;
                    }
                }

                if (query.IsNextToken("from"))
                {
                    query.SkipNextToken();
                }
                else
                {
                    throw new KbParserException("Invalid query. Found [" + query.PeekNextToken() + "], expected [FROM].");
                }

                string sourceSchema = query.GetNextToken();
                string schemaAlias = string.Empty;
                if (sourceSchema == string.Empty || TokenHelpers.IsValidIdentifier(sourceSchema, ":") == false)
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected schema name.");
                }

                if (query.IsNextToken("as"))
                {
                    query.SkipNextToken();
                    schemaAlias = query.GetNextToken();
                }

                result.Schemas.Add(new QuerySchema(sourceSchema.ToLower(), schemaAlias.ToLower()));

                while (query.IsNextToken("inner"))
                {
                    query.SkipNextToken();
                    if (query.IsNextToken("join") == false)
                    {
                        throw new KbParserException("Invalid query. Found [" + query.GetNextToken() + "], expected [JOIN].");
                    }
                    query.SkipNextToken();

                    string subSchemaSchema = query.GetNextToken();
                    string subSchemaAlias = string.Empty;
                    if (subSchemaSchema == string.Empty || TokenHelpers.IsValidIdentifier(subSchemaSchema, ":") == false)
                    {
                        throw new KbParserException("Invalid query. Found [" + token + "], expected schema name.");
                    }

                    if (query.IsNextToken("as"))
                    {
                        query.SkipNextToken();
                        subSchemaAlias = query.GetNextToken();
                    }
                    else
                    {
                        throw new KbParserException("Invalid query. Found [" + query.GetNextToken() + "], expected AS (schema alias).");
                    }

                    token = query.GetNextToken();
                    if (token.ToLower() != "on")
                    {
                        throw new KbParserException("Invalid query. Found [" + token + "], expected ON.");
                    }

                    int joinConditionsStartPosition = query.Position;

                    while (true)
                    {
                        if (query.IsNextToken(new string[] { "where", "inner", "" }))
                        {
                            break;
                        }

                        if (query.IsNextToken(new string[] { "and", "or" }))
                        {
                            query.SkipNextToken();
                        }

                        var joinLeftCondition = query.GetNextToken();
                        if (joinLeftCondition == string.Empty || TokenHelpers.IsValidIdentifier(joinLeftCondition, ".") == false)
                        {
                            throw new KbParserException("Invalid query. Found [" + joinLeftCondition + "], expected left side of join expression.");
                        }

                        int logicalQualifierPos = query.Position;

                        token = ConditionTokenizer.GetNextToken(query.Text, ref logicalQualifierPos);
                        if (ConditionTokenizer.ParseLogicalQualifier(token) == LogicalQualifier.None)
                        {
                            throw new KbParserException("Invalid query. Found [" + token + "], logical qualifier.");
                        }

                        query.SetPosition(logicalQualifierPos);

                        var joinRightCondition = query.GetNextToken();
                        if (joinRightCondition == string.Empty || TokenHelpers.IsValidIdentifier(joinRightCondition, ".") == false)
                        {
                            throw new KbParserException("Invalid query. Found [" + joinRightCondition + "], expected right side of join expression.");
                        }
                    }

                    var joinConditionsText = query.Text.Substring(joinConditionsStartPosition, query.Position - joinConditionsStartPosition).Trim();
                    var joinConditions = Conditions.Create(joinConditionsText, query.LiteralStrings, subSchemaAlias);

                    result.Schemas.Add(new QuerySchema(subSchemaSchema.ToLower(), subSchemaAlias.ToLower(), joinConditions));
                }

                token = query.GetNextToken();
                if (token != string.Empty && token.ToLower() != "where")
                {
                    throw new KbParserException("Invalid query. Found [" + token + "], expected [WHERE] or end of statement.");
                }

                if (token.ToLower() == "where")
                {
                    var conditionTokenizer = new ConditionTokenizer(query.Text, query.Position);

                    while (true)
                    {
                        int previousTokenPosition = conditionTokenizer.Position;
                        var conditonToken = conditionTokenizer.GetNextToken();

                        if (((new string[] { "order", "group", "" }).Contains(conditonToken) && conditionTokenizer.IsNextToken("by"))
                            || conditonToken == string.Empty)
                        {
                            //Set both the conditition and query position to the begining of the "ORDER BY" or "GROUP BY".
                            conditionTokenizer.SetPosition(previousTokenPosition);
                            query.SetPosition(previousTokenPosition);
                            break;
                        }
                    }

                    string conditionText = query.Text.Substring(conditionTokenizer.StartPosition, conditionTokenizer.Position - conditionTokenizer.StartPosition).Trim();
                    if (conditionText == string.Empty)
                    {
                        throw new KbParserException("Invalid query. Found [" + token + "], expected list of conditions.");
                    }

                    result.Conditions = Conditions.Create(conditionText, query.LiteralStrings);
                }

                if (query.IsNextToken("group"))
                {
                    query.SkipNextToken();

                    if (query.IsNextToken("by") == false)
                    {
                        throw new KbParserException("Invalid query. Found [" + query.GetNextToken() + "], expected 'by'.");
                    }
                    query.SkipNextToken();

                    while (true)
                    {
                        int previousTokenPosition = query.Position;
                        var fieldToken = query.GetNextToken();

                        if (result.SortFields.Count > 0)
                        {
                            if (query.NextCharacter == ',')
                            {
                                query.SkipDelimiters();
                                fieldToken = query.GetNextToken();
                            }
                            else if (!(query.Position < query.Length || query.IsNextToken("order") == false)) //We should have consumed the entire GROUP BY at this point.
                            {
                                throw new KbParserException("Invalid query. Found [" + fieldToken + "], expected ','.");
                            }
                        }

                        if (((new string[] { "order", "" }).Contains(fieldToken) && query.IsNextToken("by")) || fieldToken == string.Empty)
                        {
                            //Set query position to the begining of the "ORDER BY"..
                            query.SetPosition(previousTokenPosition);
                            break;
                        }

                        result.GroupFields.Add(fieldToken);

                        if (query.NextCharacter == ',')
                        {
                            query.SkipDelimiters();
                        }
                    }
                }

                if (query.IsNextToken("order"))
                {
                    query.SkipNextToken();

                    if (query.IsNextToken("by") == false)
                    {
                        throw new KbParserException("Invalid query. Found [" + query.GetNextToken() + "], expected 'by'.");
                    }
                    query.SkipNextToken();

                    var fields = new List<string>();

                    while (true)
                    {
                        int previousTokenPosition = query.Position;
                        var fieldToken = query.GetNextToken();

                        if (result.SortFields.Count > 0)
                        {
                            if (query.NextCharacter == ',')
                            {
                                query.SkipDelimiters();
                                fieldToken = query.GetNextToken();
                            }
                            else if (query.Position < query.Length) //We should have consumed the entire query at this point.
                            {
                                throw new KbParserException("Invalid query. Found [" + fieldToken + "], expected ','.");
                            }
                        }

                        if (fieldToken == string.Empty)
                        {
                            if (query.Position < query.Length)
                            {
                                throw new KbParserException("Invalid query. Found [" + query.Remainder() + "], expected 'end of statement'.");
                            }

                            query.SetPosition(previousTokenPosition);
                            break;
                        }

                        var sortDirection = KbSortDirection.Ascending;
                        if (query.IsNextToken(new string[] { "asc", "desc" }))
                        {
                            var directionString = query.GetNextToken();
                            if (directionString.StartsWith("desc"))
                            {
                                sortDirection = KbSortDirection.Descending;
                            }
                        }

                        result.SortFields.Add(fieldToken, sortDirection);
                    }
                }
            }
            #endregion
            //--------------------------------------------------------------------------------------------------------------------------------------------
            #region Set.
            else if (queryType == QueryType.Set)
            {
                //Variable 
                string variableName = query.GetNextToken();
                string variableValue = query.Remainder();
                result.VariableValues.Add(new KbNameValuePair(variableName, variableValue));
            }
            #endregion

            #region Cleanup and ValidartiValidation.

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

            foreach (var sortField in result.SortFields)
            {
                if (result.SelectFields.Where(o => o.Key == sortField.Key).Any() == false)
                {
                    throw new KbParserException($"Sorting field {sortField.Key} must be contained in the select list.");
                }
            }

            #endregion

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
                if ((token = ConditionTokenizer.GetNextToken(conditionsText, ref position)) == string.Empty)
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
