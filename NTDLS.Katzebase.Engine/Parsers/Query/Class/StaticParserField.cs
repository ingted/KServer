﻿using NTDLS.Katzebase.Client.Exceptions;
using NTDLS.Katzebase.Engine.Functions.Aggregate;
using NTDLS.Katzebase.Engine.Functions.Scaler;
using NTDLS.Katzebase.Engine.Parsers.Query.Fields;
using NTDLS.Katzebase.Engine.Parsers.Query.Fields.Expressions;
using NTDLS.Katzebase.Engine.Parsers.Query.Functions;
using NTDLS.Katzebase.Engine.Parsers.Tokens;
using System.Text;
using static NTDLS.Katzebase.Client.KbConstants;

namespace NTDLS.Katzebase.Engine.Parsers.Query.Class
{
    internal class StaticParserField<TData> where TData : IStringable
    {
        /// <summary>
        /// Parses a field expression containing fields, functions. string and math operations.
        /// </summary>
        public static IQueryField<TData> Parse(Tokenizer<TData> parentTokenizer, string givenFieldText, QueryFieldCollection<TData> queryFields)
        {
            Tokenizer<TData> tokenizer = new(givenFieldText);

            string token = tokenizer.EatGetNext();

            if (tokenizer.IsExhausted()) //This is a single value (document field, number or string), the simple case.
            {
                if (token.IsQueryFieldIdentifier())
                {
                    if (ScalerFunctionCollection<TData>.TryGetFunction(token, out var _))
                    {
                        //This is a function call, but it is the only token - that's not a valid function call.
                        throw new KbParserException($"Function scaler expects parentheses: [{token}]");
                    }
                    if (AggregateFunctionCollection<TData>.TryGetFunction(token, out var _))
                    {
                        //This is a function call, but it is the only token - that's not a valid function call.
                        throw new KbParserException($"Function aggregate expects parentheses: [{token}]");
                    }

                    var queryFieldDocumentIdentifier = new QueryFieldDocumentIdentifier<TData>(token);
                    var fieldKey = queryFields.GetNextDocumentFieldKey();
                    queryFields.DocumentIdentifiers.Add(fieldKey, queryFieldDocumentIdentifier);

                    return queryFieldDocumentIdentifier;
                }
                else if (IsNumericExpression(token))
                {
                    return new QueryFieldConstantNumeric<TData>(token.CastToT<TData>(EngineCore<TData>.StrCast));
                }
                else
                {
                    return new QueryFieldConstantString<TData>(token.CastToT<TData>(EngineCore<TData>.StrCast));
                }
            }

            //Fields that require expression evaluation.
            var validateNumberOfParameters = givenFieldText.ScopeSensitiveSplit(',');
            if (validateNumberOfParameters.Count > 1)
            {
                //We are testing to make sure that there are no commas that fall outside of function scopes.
                //This is because each call to ParseField should collapse to a single value.
                //E.g. "10 + Length() * 10" is allowed, but "10 + Length() * 10, Length()" is not allowed.
                throw new KbParserException($"Single field should not contain multiple values: [{givenFieldText}]");
            }

            //This field is going to require evaluation, so figure out if its a number or a string.
            if (IsNumericExpression(givenFieldText))
            {
                IQueryFieldExpression<TData> expression = new QueryFieldExpressionNumeric<TData>(givenFieldText.CastToT<TData>(EngineCore<TData>.StrCast));
                expression.Value = ParseEvaluationRecursive(parentTokenizer, ref expression, givenFieldText, ref queryFields).CastToT<TData>(EngineCore<TData>.StrCast);
                return expression;
            }
            else
            {
                IQueryFieldExpression<TData> expression = new QueryFieldExpressionString<TData>();
                expression.Value = ParseEvaluationRecursive(parentTokenizer, ref expression, givenFieldText, ref queryFields).CastToT<TData>(EngineCore<TData>.StrCast);
                return expression;
            }
        }

        private static string ParseEvaluationRecursive(Tokenizer<TData> parentTokenizer, ref IQueryFieldExpression<TData> rootQueryFieldExpression,
            string givenExpressionText, ref QueryFieldCollection<TData> queryFields)
        {
            Tokenizer<TData> tokenizer = new(givenExpressionText);

            StringBuilder buffer = new();

            while (!tokenizer.IsExhausted())
            {
                int positionBeforeToken = tokenizer.Caret;

                string token = tokenizer.GetNext();

                if (token == "(")
                {
                    var scopeText = tokenizer.EatGetMatchingScope();
                    var parenthesesResult = ParseEvaluationRecursive(parentTokenizer, ref rootQueryFieldExpression, scopeText, ref queryFields);

                    buffer.Append($"({parenthesesResult})");
                }
                else if (token.StartsWith("$s_") && token.EndsWith('$')) //A string placeholder.
                {
                    tokenizer.EatNext();
                    buffer.Append(token);
                }
                else if (token.StartsWith("$n_") && token.EndsWith('$')) //A numeric placeholder.
                {
                    tokenizer.EatNext();
                    buffer.Append(token);
                }
                else if (ScalerFunctionCollection<TData>.TryGetFunction(token, out var scalerFunction))
                {
                    tokenizer.EatNext();
                    //The expression key is used to match the function calls to the token in the parent expression.
                    var expressionKey = queryFields.GetNextExpressionKey();
                    var basicDataType = scalerFunction.ReturnType == KbScalerFunctionParameterType.Numeric ? KbBasicDataType.Numeric : KbBasicDataType.String;
                    var queryFieldExpressionFunction = new QueryFieldExpressionFunctionScaler(scalerFunction.Name, expressionKey, basicDataType);

                    ParseFunctionCallRecursive(parentTokenizer, ref rootQueryFieldExpression, queryFieldExpressionFunction, ref queryFields, tokenizer, positionBeforeToken);

                    buffer.Append(expressionKey);
                }
                else if (AggregateFunctionCollection<TData>.TryGetFunction(token, out var aggregateFunction))
                {
                    tokenizer.EatNext();
                    if (!tokenizer.IsNextNonIdentifier(['(']))
                    {
                        throw new KbParserException($"Function [{token}] must be called with parentheses.");
                    }

                    //The expression key is used to match the function calls to the token in the parent expression.
                    var expressionKey = queryFields.GetNextExpressionKey();
                    var queryFieldExpressionFunction = new QueryFieldExpressionFunctionAggregate(aggregateFunction.Name, expressionKey, KbBasicDataType.Numeric);

                    ParseFunctionCallRecursive(parentTokenizer, ref rootQueryFieldExpression, queryFieldExpressionFunction, ref queryFields, tokenizer, positionBeforeToken);

                    buffer.Append(expressionKey);
                }
                else if (token.IsQueryFieldIdentifier())
                {
                    tokenizer.EatNext();
                    if (tokenizer.IsNextNonIdentifier(['(']))
                    {
                        //The character after this identifier is an open parenthesis, so this
                        //  looks like a function call but the function is undefined.
                        throw new KbParserException($"Function [{token}] is undefined.");
                    }

                    var fieldKey = queryFields.GetNextDocumentFieldKey();
                    queryFields.DocumentIdentifiers.Add(fieldKey, new QueryFieldDocumentIdentifier<TData>(token));
                    buffer.Append(fieldKey);
                }
                else
                {
                    tokenizer.EatNext();
                    buffer.Append(token);
                }

                if (!tokenizer.IsExhausted())
                {
                    //Verify that the next character (if any) is a "connector".
                    if (tokenizer.NextCharacter != null && !tokenizer.TryIsNextCharacter(o => o.IsTokenConnectorCharacter()))
                    {
                        throw new KbParserException($"Connection token is missing after [{parentTokenizer.ResolveLiteral(token)}].");
                    }
                    else
                    {
                        buffer.Append(tokenizer.EatNextCharacter());
                    }
                }
            }

            return buffer.ToString();
        }

        /// <summary>
        /// Parses a function call and its parameters, add them to the passed queryFieldExpressionFunction.
        /// </summary>
        private static void ParseFunctionCallRecursive(Tokenizer<TData> parentTokenizer, ref IQueryFieldExpression<TData> rootQueryFieldExpression,
            IQueryFieldExpressionFunction queryFieldExpressionFunction, ref QueryFieldCollection<TData> queryFields,
            Tokenizer<TData> tokenizer, int positionBeforeToken)
        {
            //This contains the text between the open and close parenthesis of a function call, but not the parenthesis themselves or the function name.
            string functionCallParametersSegmentText = tokenizer.EatGetMatchingScope('(', ')');

            var functionCallParametersText = functionCallParametersSegmentText.ScopeSensitiveSplit(',');
            foreach (var functionCallParameterText in functionCallParametersText)
            {
                //Recursively process the function parameters.
                var resultingExpressionString = ParseEvaluationRecursive(parentTokenizer, ref rootQueryFieldExpression, functionCallParameterText, ref queryFields);

                IExpressionFunctionParameter? parameter = null;

                //TODO: ensure that this is a single token.
                if (resultingExpressionString.StartsWith("$x_") && resultingExpressionString.EndsWith('$') && IsSingleToken(resultingExpressionString))
                {
                    //This is a function call result placeholder.
                    parameter = new ExpressionFunctionParameterFunction(resultingExpressionString);
                }
                else if (IsNumericExpression(resultingExpressionString, rootQueryFieldExpression.FunctionDependencies))
                {
                    //This expression contains only numeric placeholders.
                    parameter = new ExpressionFunctionParameterNumeric(resultingExpressionString);
                }
                else
                {
                    //This expression contains non-numeric placeholders.
                    parameter = new ExpressionFunctionParameterString(resultingExpressionString);
                }

                queryFieldExpressionFunction.Parameters.Add(parameter);
            }

            rootQueryFieldExpression.FunctionDependencies.Add(queryFieldExpressionFunction);
        }

        private static bool IsSingleToken(string token)
        {
            if (token.StartsWith('$') == token.EndsWith('$') && token.Length >= 5) //Example: $n_0$, $x_0$
            {
                if (char.IsLetter(token[1]) && token[2] == '_')
                {
                    return token.Substring(3, token.Length - 4).All(char.IsDigit); //Validate the number in the middle of the markers.
                }
            }

            return false;
        }

        /// <summary>
        /// Returns true if all variables, placeholders and functions return numeric values.
        /// </summary>
        public static bool IsNumericExpression(string expressionText, List<IQueryFieldExpressionFunction>? functionDependencies = null)
        {
            Tokenizer<TData> tokenizer = new(expressionText, [' ', '+']);

            while (!tokenizer.IsExhausted())
            {
                if (tokenizer.TryIsNextCharacter(c => c.IsMathematicalOperator()))
                {
                    tokenizer.EatNextCharacter();
                    continue;
                }

                string token = tokenizer.EatGetNext();
                if (string.IsNullOrEmpty(token))
                {
                    break;
                }

                if (token.StartsWith("$x_") && token.EndsWith('$'))
                {
                    //This is a function result placeholder.
                    if (functionDependencies == null)
                    {
                        throw new KbParserException($"Function reference found without root expression: [{token}].");
                    }

                    //Find the function call so we can check the function return type.
                    var referencedFunction = functionDependencies.Single(f => f.ExpressionKey == token);

                    if (referencedFunction.ReturnType != KbBasicDataType.Numeric)
                    {
                        //This function returns something other then numeric, so we are evaluating strings.
                        return false;
                    }
                    continue;
                }
                else if (token.StartsWith("$s_") && token.EndsWith('$'))
                {
                    //This is a string, so this is not a numeric operation.
                    return false;
                }
                else if (token.StartsWith("$n_") && token.EndsWith('$'))
                {
                    //This is a number placeholder, so we still have a valid numeric operation.
                    continue;
                }
                else if (ScalerFunctionCollection<TData>.TryGetFunction(token, out var scalerFunction))
                {
                    if (scalerFunction.ReturnType == KbScalerFunctionParameterType.Numeric)
                    {
                        //This function returns a number, so we still have a valid numeric operation.

                        //Skip the function call.
                        string functionBody = tokenizer.EatGetMatchingScope('(', ')');
                        continue;
                    }
                    else
                    {
                        //This function returns a non-number, so this is not a numeric operation.
                        return false;
                    }
                }
                else if (AggregateFunctionCollection<TData>.TryGetFunction(token, out var aggregateFunction))
                {
                    if (aggregateFunction.ReturnType == KbAggregateFunctionParameterType.Numeric)
                    {
                        //This function returns a number, so we still have a valid numeric operation.

                        //Skip the function call.
                        string functionBody = tokenizer.EatGetMatchingScope('(', ')');
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
                    //This is likely a document field, we'll assume its a number
                    //return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Returns true if all variables and placeholders are constant expressions (e.g. does not contain document fields or functions).
        /// </summary>
        public static bool IsConstantExpression(string? expressionText)
        {
            if (expressionText == null)
            {
                return true;
            }

            Tokenizer<TData> tokenizer = new(expressionText, [' ', '+']);

            while (!tokenizer.IsExhausted())
            {
                if (tokenizer.TryIsNextCharacter(c => c.IsMathematicalOperator()))
                {
                    tokenizer.EatNextCharacter();
                    continue;
                }

                string token = tokenizer.EatGetNext();
                if (string.IsNullOrEmpty(token))
                {
                    break;
                }

                if (token.StartsWith("$x_") && token.EndsWith('$'))
                {
                    //This is a function result placeholder, and functions are not constant.
                    return false;
                }
                else if (token.StartsWith("$s_") && token.EndsWith('$'))
                {
                    //This is a string constant, we're all good.
                    continue;
                }
                else if (token.StartsWith("$n_") && token.EndsWith('$'))
                {
                    //This is a numeric constant, we're all good.
                    continue;
                }
                else if (ScalerFunctionCollection<TData>.TryGetFunction(token, out var scalerFunction))
                {
                    //Functions are not constant.
                    return false;
                }
                else if (AggregateFunctionCollection<TData>.TryGetFunction(token, out var aggregateFunction))
                {
                    //Functions are not constant.
                    return false;
                }
                else
                {
                    //This is likely a document field, we'll assume its a number
                    return false;
                }
            }

            return true;
        }
    }
}
