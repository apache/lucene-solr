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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class with which to safely build a streaming expression. Three types of parameters
 * (String, Numeric, Expression) are accepted and minimally type checked. All parameters
 * are positional (unnamed) so the order in which parameters are added must correspond to
 * the order of the parameters in the supplied expression string.<br><br>
 *
 * <p>Specifically, this class verifies that the parameter substitutions do not inject
 * additional expressions, and that the parameters are strings, valid numbers or valid
 * expressions producing the expected number of sub-expressions. The idea is not to provide
 * full type safety but rather to heuristically prevent the injection of malicious
 * expressions. The template expression and the parameters supplied must not contain
 * comments since injection of  comments could be used to hide one or more of the expected
 * expressions. Use {@link #stripComments(String)} to remove comments.<br><br>
 *
 * <p>Valid patterns for parameters are:
 * <ul>
 * <li>?$? for strings</li>
 * <li>?#? for numeric parameters in integer or decimal format (no exponents)</li>
 * <li>?(n)? for expressions producing n sub-expressions (minimum n=1)</li>
 * </ul>
 *
 * @since 8.0.0
 */

public class InjectionDefense {

  private static final Pattern STRING_PARAM = Pattern.compile("\\?\\$\\?");
  private static final Pattern NUMBER_PARAM = Pattern.compile("\\?#\\?");
  private static final Pattern EXPRESSION_PARAM = Pattern.compile("\\?\\(\\d+\\)\\?");
  private static final Pattern EXPRESSION_COUNT = Pattern.compile("\\d+");
  private static final Pattern ANY_PARAM = Pattern.compile("\\?(?:[$#]|(?:\\(\\d+\\)))\\?");
  private static final Pattern INT_OR_FLOAT = Pattern.compile("-?\\d+(?:\\.\\d+)?");

  private String exprString;
  private int expressionCount;
  private List<String> params = new ArrayList<>();

  @SuppressWarnings("WeakerAccess")
  public InjectionDefense(String exprString) {
    this.exprString = exprString;
    checkExpression(exprString);
  }

  @SuppressWarnings("WeakerAccess")
  public static String stripComments(String exprString) {
    return StreamExpressionParser.stripComments(exprString);
  }

  public void addParameter(String param) {
    params.add(param);
  }

  /**
   * Provides an expression that is guaranteed to have the expected number of sub-expressions
   *
   * @return An expression object that should be safe from injection of additional expressions
   */
  @SuppressWarnings("WeakerAccess")
  public StreamExpression safeExpression() {
    String exprStr = buildExpression();
    StreamExpression parsed = StreamExpressionParser.parse(exprStr);
    int actual = countExpressions(parsed);
    if (actual != expressionCount) {
      throw new InjectedExpressionException("Expected Expression count ("+expressionCount+") does not match actual final " +
          "expression count ("+actual+")! (possible injection attack?)");
    } else {
      return parsed;
    }
  }

  /**
   * Provides a string that is guaranteed to parse to a legal expression and to have the expected
   * number of sub-expressions.
   *
   * @return A string that should be safe from injection of additional expressions.
   */
  @SuppressWarnings("WeakerAccess")
  public String safeExpressionString() {
    String exprStr = buildExpression();
    StreamExpression parsed = StreamExpressionParser.parse(exprStr);
    if (countExpressions(parsed) != expressionCount) {
      throw new InjectedExpressionException("Expected Expression count does not match Actual final " +
          "expression count! (possible injection attack?)");
    } else {
      return exprStr;
    }

  }

  String buildExpression() {
    Matcher anyParam = ANY_PARAM.matcher(exprString);
    StringBuffer buff = new StringBuffer();
    int pIdx = 0;
    while (anyParam.find()) {
      String found = anyParam.group();
      String p = params.get(pIdx++);
      if (found.contains("#")) {
        if (!INT_OR_FLOAT.matcher(p).matches()) {
          throw new NumberFormatException("Argument " + pIdx + " (" + p + ") is not numeric!");
        }
      }
      anyParam.appendReplacement(buff, p);
    }
    anyParam.appendTail(buff);

    // strip comments may add '\n' at the end so trim()
    String result = buff.toString().trim();
    String noComments = stripComments(result).trim();
    if (!result.equals(noComments)) {
      throw new IllegalStateException("Comments are not allowed in prepared expressions for security reasons " +
          "please pre-process stripComments() first. If there were no comments, then they have been injected by " +
          "a parameter value.");
    }
    return buff.toString().trim();
  }

  /**
   * Perform some initial checks and establish the expected number of expressions
   *
   * @param exprString the expression to check.
   */
  private void checkExpression(String exprString) {
    exprString = STRING_PARAM.matcher(exprString).replaceAll("foo");
    exprString = NUMBER_PARAM.matcher(exprString).replaceAll("0");
    Matcher eMatcher = EXPRESSION_PARAM.matcher(exprString);
    StringBuffer temp = new StringBuffer();
    while (eMatcher.find()) {
      Matcher counter = EXPRESSION_COUNT.matcher(eMatcher.group());
      eMatcher.appendReplacement(temp, "noop()");
      if (counter.find()) {
        Integer subExprCount = Integer.valueOf(counter.group());
        if (subExprCount < 1) {
          throw new IllegalStateException("Expression Param must contribute at least 1 expression!" +
              " ?(1)? is the minimum allowed ");
        }
        expressionCount += (subExprCount - 1); // the noop() we insert will get counted later.
      }
    }
    eMatcher.appendTail(temp);
    exprString = temp.toString();

    StreamExpression parsed = StreamExpressionParser.parse(exprString);
    if (parsed != null) {
      expressionCount += countExpressions(parsed);
    } else {
      throw new IllegalStateException("Invalid expression (parse returned null):" + exprString);
    }
  }

  private int countExpressions(StreamExpression expression) {
    int result = 0;
    List<StreamExpressionParameter> exprToCheck = new ArrayList<>();
    exprToCheck.add(expression);
    while (exprToCheck.size() > 0) {
      StreamExpressionParameter remove = exprToCheck.remove(0);
      if (remove instanceof StreamExpressionNamedParameter) {
        remove = ((StreamExpressionNamedParameter) remove).getParameter();
      }
      if (remove instanceof StreamExpression) {
        result++;
        for (StreamExpressionParameter parameter : ((StreamExpression) remove).getParameters()) {
          if (parameter instanceof StreamExpressionNamedParameter) {
            parameter = ((StreamExpressionNamedParameter) parameter).getParameter();
          }
          if (parameter instanceof StreamExpression) {
            exprToCheck.add(parameter);
          }
        }
      }
    }
    return result;
  }

}
