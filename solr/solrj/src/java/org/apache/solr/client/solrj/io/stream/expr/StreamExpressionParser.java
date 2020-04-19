/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.client.solrj.io.stream.expr;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Takes a prefix notation expression and returns a tokenized expression
 */
public class StreamExpressionParser {


  static char[] wordChars = {'_','.','-'};

  static {
    Arrays.sort(wordChars);
  }

  public static StreamExpression parse(String clause){
    clause = stripComments(clause);
    StreamExpressionParameter expr = generateStreamExpression(clause);
    if(null != expr && expr instanceof StreamExpression){
      return (StreamExpression)expr;
    }

    return null;
  }


  static String stripComments(String clause) throws RuntimeException {
    StringBuilder builder = new StringBuilder();

    try (BufferedReader reader = new BufferedReader(new StringReader(clause))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.trim().startsWith("#")) {
          builder.append(line).append('\n');
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return builder.toString();
  }

  private static StreamExpressionParameter generateStreamExpression(String clause){
    String working = clause.trim();

    if(!isExpressionClause(working)){
      throw new IllegalArgumentException(String.format(Locale.ROOT,"'%s' is not a proper expression clause", working));
    }

    // Get functionName
    int firstOpenParen = findNextClear(working, 0, '(');
    StreamExpression expression = new StreamExpression(working.substring(0, firstOpenParen).trim());

    // strip off functionName and ()
    working = working.substring(firstOpenParen + 1,working.length() - 1).trim();
    List<String> parts = splitOn(working,',');

    for(int idx = 0; idx < parts.size(); ++idx){
      String part = parts.get(idx).trim();
      if(isExpressionClause(part)){
        StreamExpressionParameter parameter = generateStreamExpression(part);
        if(null != parameter){
          expression.addParameter(parameter);
        }
      }
      else if(isNamedParameterClause(part)){
        StreamExpressionNamedParameter parameter = generateNamedParameterExpression(part);
        if(null != parameter){
          expression.addParameter(parameter);
        }
      }
      else{
        expression.addParameter(new StreamExpressionValue(part));
      }
    }

    return expression;
  }

  private static StreamExpressionNamedParameter generateNamedParameterExpression(String clause){
    String working = clause.trim();

    // might be overkill as the only place this is called from does this check already
    if(!isNamedParameterClause(working)){
      throw new IllegalArgumentException(String.format(Locale.ROOT,"'%s' is not a proper named parameter clause", working));
    }

    // Get name
    int firstOpenEquals = findNextClear(working, 0, '=');
    StreamExpressionNamedParameter namedParameter = new StreamExpressionNamedParameter(working.substring(0, firstOpenEquals).trim());

    // we know this is ok because of the check in isNamedParameter
    String parameter = working.substring(firstOpenEquals + 1, working.length());
    if(isExpressionClause(parameter)){
      namedParameter.setParameter(generateStreamExpression(parameter));
    }
    else{
      // if wrapped in quotes, remove them
      if(parameter.startsWith("\"") && parameter.endsWith("\"")){
        parameter = parameter.substring(1, parameter.length() - 1).trim();
        if(0 == parameter.length()){
          throw new IllegalArgumentException(String.format(Locale.ROOT,"'%s' is not a proper named parameter clause", working));
        }
      }

      // if contain \" replace with "
      if(parameter.contains("\\\"")){
        parameter = parameter.replace("\\\"", "\"");
        if(0 == parameter.length()){
          throw new IllegalArgumentException(String.format(Locale.ROOT,"'%s' is not a proper named parameter clause", working));
        }
      }

      // If contains ` replace with "
      // This allows ` to be used as a quote character

      if(parameter.contains("`")){
        parameter = parameter.replace('`', '"');
        if(0 == parameter.length()){
          throw new IllegalArgumentException(String.format(Locale.ROOT,"'%s' is not a proper named parameter clause", working));
        }
      }


      namedParameter.setParameter(new StreamExpressionValue(parameter));
    }

    return namedParameter;
  }


  /* Returns true if the clause is a valid expression clause. This is defined to
   * mean it begins with ( and ends with )
   * Expects that the passed in clause has already been trimmed of leading and
   * trailing spaces*/
  private static boolean isExpressionClause(String clause){
    // operator(.....something.....)

    // must be balanced
    if(!isBalanced(clause)){ return false; }

    // find first (, then check from start to that location and only accept alphanumeric
    int firstOpenParen = findNextClear(clause, 0, '(');
    if(firstOpenParen <= 0 || firstOpenParen == clause.length() - 1){ return false; }
    String functionName = clause.substring(0, firstOpenParen).trim();
    if(!wordToken(functionName)){ return false; }

    // Must end with )
    return clause.endsWith(")");
  }

  private static boolean isNamedParameterClause(String clause){
    // name=thing

    // find first = then check from start to that location and only accept alphanumeric
    int firstOpenEquals = findNextClear(clause, 0, '=');
    if(firstOpenEquals <= 0 || firstOpenEquals == clause.length() - 1){ return false; }
    String name = clause.substring(0, firstOpenEquals);
    if(!wordToken(name.trim())){ return false; }

    return true;
  }

  /* Finds index of the next char equal to findThis that is not within a quote or set of parens
   * Does not work with the following values of findThis: " ' \ ) -- well, it might but wouldn't
   * really give you what you want. Don't call with those characters */
  private static int findNextClear(String clause, int startingIdx, char findThis){
    int openParens = 0;
    boolean isDoubleQuote = false;
    boolean isSingleQuote = false;
    boolean isEscaped = false;

    for(int idx = startingIdx; idx < clause.length(); ++idx){
      char c = clause.charAt(idx);

      // if we're not in a non-escaped quote or paren state, then we've found the space we want
      if(c == findThis && !isEscaped && !isSingleQuote && !isDoubleQuote && 0 == openParens){
        return idx;
      }


      switch(c){
        case '\\':
          // We invert to support situations where \\ exists
          isEscaped = !isEscaped;
          break;

        case '"':
          // if we're not in a non-escaped single quote state, then invert the double quote state
          if(!isEscaped && !isSingleQuote){
            isDoubleQuote = !isDoubleQuote;
          }
          isEscaped = false;
          break;

        case '\'':
          // if we're not in a non-escaped double quote state, then invert the single quote state
          if(!isEscaped && !isDoubleQuote){
            isSingleQuote = !isSingleQuote;
          }
          isEscaped = false;
          break;

        case '(':
          // if we're not in a non-escaped quote state, then increment the # of open parens
          if(!isEscaped && !isSingleQuote && !isDoubleQuote){
            openParens += 1;
          }
          isEscaped = false;
          break;

        case ')':
          // if we're not in a non-escaped quote state, then decrement the # of open parens
          if(!isEscaped && !isSingleQuote && !isDoubleQuote){
            openParens -= 1;
          }
          isEscaped = false;
          break;
        default:
          isEscaped = false;
      }
    }

    // Not found
    return -1;
  }


  /* Returns a list of the tokens found. Assumed to be of the form
   * 'foo bar baz' and not of the for '(foo bar baz)'
   * 'foo bar (baz jaz)' is ok and will return three tokens of
   * 'foo', 'bar', and '(baz jaz)'
   */
  private static List<String> splitOn(String clause, char splitOnThis){
    String working = clause.trim();

    List<String> parts = new ArrayList<String>();

    while(true){ // will break when next splitOnThis isn't found
      int nextIdx = findNextClear(working, 0, splitOnThis);

      if(nextIdx < 0){
        parts.add(working);
        break;
      }

      parts.add(working.substring(0, nextIdx));

      // handle ending splitOnThis
      if(nextIdx+1 == working.length()){
        break;
      }

      working = working.substring(nextIdx + 1).trim();
    }

    return parts;
  }

  /* Returns true if the clause has balanced parenthesis */
  private static boolean isBalanced(String clause){
    int openParens = 0;
    boolean isDoubleQuote = false;
    boolean isSingleQuote = false;
    boolean isEscaped = false;

    for(int idx = 0; idx < clause.length(); ++idx){
      char c = clause.charAt(idx);

      switch(c){
        case '\\':
          // We invert to support situations where \\ exists
          isEscaped = !isEscaped;
          break;

        case '"':
          // if we're not in a non-escaped single quote state, then invert the double quote state
          if(!isEscaped && !isSingleQuote){
            isDoubleQuote = !isDoubleQuote;
          }
          isEscaped = false;
          break;

        case '\'':
          // if we're not in a non-escaped double quote state, then invert the single quote state
          if(!isEscaped && !isDoubleQuote){
            isSingleQuote = !isSingleQuote;
          }
          isEscaped = false;
          break;

        case '(':
          // if we're not in a non-escaped quote state, then increment the # of open parens
          if(!isEscaped && !isSingleQuote && !isDoubleQuote){
            openParens += 1;
          }
          isEscaped = false;
          break;

        case ')':
          // if we're not in a non-escaped quote state, then decrement the # of open parens
          if(!isEscaped && !isSingleQuote && !isDoubleQuote){
            openParens -= 1;

            // If we're ever < 0 then we know we're not balanced
            if(openParens < 0){
              return false;
            }
          }
          isEscaped = false;
          break;

        default:
          isEscaped = false;
      }
    }

    return (0 == openParens);
  }

  public static boolean wordToken(String token) {
    for(int i=0; i<token.length(); i++) {
      char c = token.charAt(i);
      if (!Character.isLetterOrDigit(c) && Arrays.binarySearch(wordChars, c) < 0) {
        return false;
      }
    }
    return true;
  }
}
