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
package org.apache.solr.handler.dataimport;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * <p>
 * Pluggable functions for resolving variables
 * </p>
 * <p>
 * Implementations of this abstract class must provide a public no-arg constructor.
 * </p>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @since solr 1.3
 */
public abstract class Evaluator {

  /**
   * Return a String after processing an expression and a {@link VariableResolver}
   *
   * @see VariableResolver
   * @param expression string to be evaluated
   * @param context instance
   * @return the value of the given expression evaluated using the resolver
   */
  public abstract String evaluate(String expression, Context context);

  /**
   * Parses a string of expression into separate params. The values are separated by commas. each value will be
   * translated into one of the following:
   * &lt;ol&gt;
   * &lt;li&gt;If it is in single quotes the value will be translated to a String&lt;/li&gt;
   * &lt;li&gt;If is is not in quotes and is a number a it will be translated into a Double&lt;/li&gt;
   * &lt;li&gt;else it is a variable which can be resolved and it will be put in as an instance of VariableWrapper&lt;/li&gt;
   * &lt;/ol&gt;
   *
   * @param expression the expression to be parsed
   * @param vr the VariableResolver instance for resolving variables
   *
   * @return a List of objects which can either be a string, number or a variable wrapper
   */
  protected List<Object> parseParams(String expression, VariableResolver vr) {
    List<Object> result = new ArrayList<>();
    expression = expression.trim();
    String[] ss = expression.split(",");
    for (int i = 0; i < ss.length; i++) {
      ss[i] = ss[i].trim();
      if (ss[i].startsWith("'")) {//a string param has started
        StringBuilder sb = new StringBuilder();
        while (true) {
          sb.append(ss[i]);
          if (ss[i].endsWith("'")) break;
          i++;
          if (i >= ss.length)
            throw new DataImportHandlerException(SEVERE, "invalid string at " + ss[i - 1] + " in function params: " + expression);
          sb.append(",");
        }
        String s = sb.substring(1, sb.length() - 1);
        s = s.replaceAll("\\\\'", "'");
        result.add(s);
      } else {
        if (Character.isDigit(ss[i].charAt(0))) {
          try {
            Double doub = Double.parseDouble(ss[i]);
            result.add(doub);
          } catch (NumberFormatException e) {
            if (vr.resolve(ss[i]) == null) {
              wrapAndThrow(
                      SEVERE, e, "Invalid number :" + ss[i] +
                              "in parameters  " + expression);
            }
          }
        } else {
          result.add(getVariableWrapper(ss[i], vr));
        }
      }
    }
    return result;
  }

  protected VariableWrapper getVariableWrapper(String s, VariableResolver vr) {
    return new VariableWrapper(s,vr);
  }

  static protected class VariableWrapper {
    public final String varName;
    public final VariableResolver vr;

    public VariableWrapper(String s, VariableResolver vr) {
      this.varName = s;
      this.vr = vr;
    }

    public Object resolve() {
      return vr.resolve(varName);
    }

    @Override
    public String toString() {
      Object o = vr.resolve(varName);
      return o == null ? null : o.toString();
    }
  }

  static Pattern IN_SINGLE_QUOTES = Pattern.compile("^'(.*?)'$");
  
  public static final String DATE_FORMAT_EVALUATOR = "formatDate";

  public static final String URL_ENCODE_EVALUATOR = "encodeUrl";

  public static final String ESCAPE_SOLR_QUERY_CHARS = "escapeQueryChars";

  public static final String SQL_ESCAPE_EVALUATOR = "escapeSql";
}
