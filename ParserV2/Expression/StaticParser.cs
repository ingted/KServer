﻿using NTDLS.Katzebase.Client.Exceptions;
using ParserV2.StandIn;
using System.Text;
using static ParserV2.StandIn.Types;

namespace ParserV2.Expression
{
    internal static class StaticParser
    {
        /// <summary>
        /// Parses the field expressions for a "select" or "select into" query.
        /// </summary>
        public static ExpressionCollection ParseSelectFields(Tokenizer queryTokenizer)
        {
            var result = new ExpressionCollection();

            //Get the position which represents the end of the select list.
            int stopAt = queryTokenizer.InertGetNextIndexOf([" from ", " into "]);

            //Get the text for all of the select expressions.
            var fieldsText = queryTokenizer.SubString(stopAt);

            //Split the select expressions on the comma, respecting any commas in function scopes.
            var fieldExpressionTexts = fieldsText.ScopeSensitiveSplit();

            foreach (var fieldExpressionText in fieldExpressionTexts)
            {
                string fieldExpressionAlias = string.Empty;

                //Parse the field expression alias.
                int aliasIndex = fieldExpressionText.IndexOf(" as ", StringComparison.InvariantCultureIgnoreCase);
                if (aliasIndex > 0)
                {
                    fieldExpressionAlias = fieldExpressionText.Substring(aliasIndex + 4).Trim();
                }

                var aliasRemovedFieldExpressionText = (aliasIndex > 0 ? fieldExpressionText.Substring(0, aliasIndex) : fieldExpressionText).Trim();

                var expression = ParseExpression(aliasRemovedFieldExpressionText, queryTokenizer);

                result.Collection.Add(new NamedExpression(fieldExpressionAlias, expression));
            }

            return result;
        }

        private static IExpression ParseExpression(string givenExpressionText, Tokenizer queryTokenizer)
        {
            #region This is a single value expression (document field, number or string), the simple case.

            Tokenizer tokenizer = new(givenExpressionText);

            string token = tokenizer.GetNext();

            if (tokenizer.IsEnd())
            {
                if (token.IsIdentifier())
                {
                    if (ScalerFunctionCollection.TryGetFunction(token, out var _))
                    {
                        //This is a function call, but it is the only token - that's not a valid function call.
                        throw new KbParserException($"Simple expression function expects parentheses: [{token}]");
                    }

                    //This is not a function (those require evaluation) so its a single identifier, likely a document field name.
                    return new ExpressionIdentifier(token);

                }
                else if (IsNumericExpression(token))
                {
                    return new ExpressionNumericConstant(token);
                }
                else
                {
                    return new ExpressionStringConstant(token);
                }
            }

            #endregion

            #region Expressions that require evaluation.

            var collapsibleExpressionEvaluation = givenExpressionText.ScopeSensitiveSplit();
            if (collapsibleExpressionEvaluation.Count > 1)
            {
                //We are testing to make sure that there are no commas that fall outside of function scopes.
                //This is because each call to ParseExpression should collapse to a single value.
                //E.g. "10 + Length() * 10" is allowed, but "10 + Length() * 10, Length()" is not allowed.
                throw new KbParserException($"Single expression should contain multiple values: [{givenExpressionText}]");
            }

            if (IsNumericExpression(givenExpressionText))
            {

                IExpressionEvaluation expression = new ExpressionNumericEvaluation(givenExpressionText);
                expression.Expression = ParseEvaluationRecursive(ref expression, givenExpressionText);
                return expression;
            }
            else
            {
                IExpressionEvaluation expression = new ExpressionStringEvaluation();
                expression.Expression = ParseEvaluationRecursive(ref expression, givenExpressionText);
                return expression;
            }

            #endregion
        }

        private static string ParseEvaluationRecursive(ref IExpressionEvaluation rootExpressionEvaluation, string givenExpressionText)
        {
            //Console.WriteLine($"Recursive parameter parser: {givenExpressionText}");

            Tokenizer tokenizer = new(givenExpressionText);

            StringBuilder buffer = new();

            int positionBeforeToken = 0;

            while (!tokenizer.IsEnd())
            {
                positionBeforeToken = tokenizer.CaretPosition;

                string token = tokenizer.GetNext();

                if (token.StartsWith("$s_") && token.EndsWith('$')) //A string placeholder.
                {
                    buffer.Append(token);
                }
                else if (token.StartsWith("$n_") && token.EndsWith('$')) //A numeric placeholder.
                {
                    buffer.Append(token);
                }
                else if (ScalerFunctionCollection.TryGetFunction(token, out var function))
                {
                    string functionParameterExpressions = tokenizer.GetMatchingBraces('(', ')');
                    string wholeFunctionExpression = tokenizer.InertSubString(positionBeforeToken, tokenizer.CaretPosition - positionBeforeToken);

                    //The expression key is used to match the function calls to the token in the parent expression.
                    var expressionKey = rootExpressionEvaluation.GetKeyExpressionKey();

                    givenExpressionText = givenExpressionText.Replace(wholeFunctionExpression, expressionKey);

                    var functionCallEvaluation = new FunctionCallEvaluation(function.Name, expressionKey);

                    //expression.ReferencedFunctions = referencedFunctions; //TODO: save these.

                    rootExpressionEvaluation.ReferencedFunctions.Add(new ReferencedFunction(expressionKey, function.Name, function.ReturnType));

                    var expressionParameterTexts = functionParameterExpressions.ScopeSensitiveSplit();
                    foreach (var functionParameter in expressionParameterTexts)
                    {
                        //Recursively process the function parameters.
                        var resultingExpressionString = ParseEvaluationRecursive(ref rootExpressionEvaluation, functionParameter);

                        IExpressionFunctionParameter? parameter = null;

                        if (resultingExpressionString.StartsWith("$p_") && resultingExpressionString.EndsWith('$'))
                        {
                            //This is a function call result placeholder.
                            parameter = new ExpressionFunctionParameterFunctionResult(resultingExpressionString);

                            functionCallEvaluation.FunctionDependencies.Add(resultingExpressionString);
                        }
                        else if (IsNumericExpression(resultingExpressionString))
                        {
                            //This expression contains only numeric placeholders.
                            parameter = new ExpressionFunctionParameterNumeric(resultingExpressionString);
                        }
                        else
                        {
                            //This expression contains non-numeric placeholders.
                            parameter = new ExpressionFunctionParameterString(resultingExpressionString);
                        }

                        Console.WriteLine(resultingExpressionString);

                        functionCallEvaluation.Parameters.Add(parameter);
                    }

                    rootExpressionEvaluation.FunctionCalls.Add(functionCallEvaluation);

                    //TODO start a recursive call here with each parameter?

                    //We need to parse the function and add a child node to the "expressionEvaluation".
                    //We then replace the segment that we extracted from "expressionEvaluation" with the child's key.
                }
                else if (token.IsIdentifier())
                {
                    if (tokenizer.InertIsNextNonIdentifier(['(']))
                    {
                        //The character after this identifier is an open parenthesis, so this
                        //  looks like a function call but the function is undefined.
                        throw new KbParserException($"Function [{token}] is undefined.");
                    }
                }
                else
                {
                    buffer.Append(token);
                }

            }

            return givenExpressionText;
        }

        /// <summary>
        /// Returns true if all variables, placeholders and functions return numeric values.
        /// </summary>
        /// <param name="expressionText"></param>
        /// <returns></returns>
        /// <exception cref="KbParserException"></exception>
        private static bool IsNumericExpression(string expressionText)
        {
            Tokenizer tokenizer = new(expressionText);

            while (!tokenizer.IsEnd())
            {
                if (tokenizer.InertIsNextCharacter(c => c.IsMathematicalOperator()))
                {
                    tokenizer.SkipNextCharacter();
                    continue;
                }

                string token = tokenizer.GetNext();
                if (string.IsNullOrEmpty(token))
                {
                    break;
                }

                if (token.StartsWith("$s_") && token.EndsWith('$'))
                {
                    //This is a string, so this is not a numeric operation.
                    return false;
                }

                if (token.StartsWith("$n_") && token.EndsWith('$'))
                {
                    //This is a number placeholder, so we still have a valid numeric operation.
                    continue;
                }

                if (ScalerFunctionCollection.TryGetFunction(token, out var function))
                {
                    if (function.ReturnType == KbScalerFunctionParameterType.Numeric)
                    {
                        //This function returns a number, so we still have a valid numeric operation.

                        //Skip the function call.
                        string functionBody = tokenizer.GetMatchingBraces('(', ')');
                        continue;
                    }
                    else
                    {
                        //This function returns a non-number, so this is not a numeric operation.
                        return false;
                    }
                }
                else
                {
                    //throw new KbParserException($"Invalid query. Found [{token}], expected: scaler function.");

                    //This is likely a document field.
                    return false;
                }
            }

            return true;
        }
    }
}
