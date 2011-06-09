package org.apache.solr.handler.dataimport;
/**
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

import org.apache.solr.core.SolrCore;
import static org.apache.solr.handler.dataimport.DataConfig.CLASS;
import static org.apache.solr.handler.dataimport.DataConfig.NAME;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;
import static org.apache.solr.handler.dataimport.DocBuilder.loadClass;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p> Holds definitions for evaluators provided by DataImportHandler </p> <p/> <p> Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a> for more
 * details. </p>
 * <p/>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @since solr 1.3
 */
public class EvaluatorBag {
  private static final Logger LOG = LoggerFactory.getLogger(EvaluatorBag.class);
  
  public static final String DATE_FORMAT_EVALUATOR = "formatDate";

  public static final String URL_ENCODE_EVALUATOR = "encodeUrl";

  public static final String ESCAPE_SOLR_QUERY_CHARS = "escapeQueryChars";

  public static final String SQL_ESCAPE_EVALUATOR = "escapeSql";
  static final Pattern FORMAT_METHOD = Pattern
          .compile("^(\\w*?)\\((.*?)\\)$");

  /**
   * <p/> Returns an <code>Evaluator</code> instance meant to be used for escaping values in SQL queries. </p> <p/> It
   * escapes the value of the given expression by replacing all occurrences of single-quotes by two single-quotes and
   * similarily for double-quotes </p>
   *
   * @return an <code>Evaluator</code> instance capable of SQL-escaping expressions.
   */
  public static Evaluator getSqlEscapingEvaluator() {
    return new Evaluator() {
      @Override
      public String evaluate(String expression, Context context) {
        List l = parseParams(expression, context.getVariableResolver());
        if (l.size() != 1) {
          throw new DataImportHandlerException(SEVERE, "'escapeSql' must have at least one parameter ");
        }
        String s = l.get(0).toString();
        // escape single quote with two single quotes, double quote
        // with two doule quotes, and backslash with double backslash.
        // See:  http://dev.mysql.com/doc/refman/4.1/en/mysql-real-escape-string.html
        return s.replaceAll("'", "''").replaceAll("\"", "\"\"").replaceAll("\\\\", "\\\\\\\\");
      }
    };
  }

  /**
   * <p/>Returns an <code>Evaluator</code> instance meant to be used for escaping reserved characters in Solr
   * queries</p>
   *
   * @return an <code>Evaluator</code> instance capable of escaping reserved characters in solr queries.
   *
   * @see org.apache.solr.client.solrj.util.ClientUtils#escapeQueryChars(String)
   */
  public static Evaluator getSolrQueryEscapingEvaluator() {
    return new Evaluator() {
      @Override
      public String evaluate(String expression, Context context) {
        List l = parseParams(expression, context.getVariableResolver());
        if (l.size() != 1) {
          throw new DataImportHandlerException(SEVERE, "'escapeQueryChars' must have at least one parameter ");
        }
        String s = l.get(0).toString();
        return ClientUtils.escapeQueryChars(s);
      }
    };
  }

  /**
   * <p/> Returns an <code>Evaluator</code> instance capable of URL-encoding expressions. The expressions are evaluated
   * using a <code>VariableResolver</code> </p>
   *
   * @return an <code>Evaluator</code> instance capable of URL-encoding expressions.
   */
  public static Evaluator getUrlEvaluator() {
    return new Evaluator() {
      @Override
      public String evaluate(String expression, Context context) {
        List l = parseParams(expression, context.getVariableResolver());
        if (l.size() != 1) {
          throw new DataImportHandlerException(SEVERE, "'encodeUrl' must have at least one parameter ");
        }
        String s = l.get(0).toString();

        try {
          return URLEncoder.encode(s.toString(), "UTF-8");
        } catch (Exception e) {
          wrapAndThrow(SEVERE, e, "Unable to encode expression: " + expression + " with value: " + s);
          return null;
        }
      }
    };
  }

  /**
   * <p/> Returns an <code>Evaluator</code> instance capable of formatting values using a given date format. </p> <p/>
   * The value to be formatted can be a entity.field or a date expression parsed with <code>DateMathParser</code> class.
   * If the value is in a String, then it is assumed to be a datemath expression, otherwise it resolved using a
   * <code>VariableResolver</code> instance </p>
   *
   * @return an Evaluator instance capable of formatting values to a given date format
   *
   * @see DateMathParser
   */
  public static Evaluator getDateFormatEvaluator() {
    return new Evaluator() {
      @Override
      public String evaluate(String expression, Context context) {
        List l = parseParams(expression, context.getVariableResolver());
        if (l.size() != 2) {
          throw new DataImportHandlerException(SEVERE, "'formatDate()' must have two parameters ");
        }
        Object o = l.get(0);
        Object format = l.get(1);
        if (format instanceof VariableWrapper) {
          VariableWrapper wrapper = (VariableWrapper) format;
          o = wrapper.resolve();
          if (o == null)  {
            format = wrapper.varName;
            LOG.warn("Deprecated syntax used. The syntax of formatDate has been changed to formatDate(<var>, '<date_format_string>'). " +
                    "The old syntax will stop working in Solr 1.5");
          } else  {
            format = o.toString();
          }
        }
        String dateFmt = format.toString();
        SimpleDateFormat fmt = new SimpleDateFormat(dateFmt);
        Date date = null;
        if (o instanceof VariableWrapper) {
          VariableWrapper variableWrapper = (VariableWrapper) o;
          Object variableval = variableWrapper.resolve();
          if (variableval instanceof Date) {
            date = (Date) variableval;
          } else {
            String s = variableval.toString();
            try {
              date = DataImporter.DATE_TIME_FORMAT.get().parse(s);
            } catch (ParseException exp) {
              wrapAndThrow(SEVERE, exp, "Invalid expression for date");
            }
          }
        } else {
          String datemathfmt = o.toString();
          datemathfmt = datemathfmt.replaceAll("NOW", "");
          try {
            date = dateMathParser.parseMath(datemathfmt);
          } catch (ParseException e) {
            wrapAndThrow(SEVERE, e, "Invalid expression for date");
          }
        }
        return fmt.format(date);
      }

    };
  }

  static Map<String, Object> getFunctionsNamespace(final List<Map<String, String>> fn, DocBuilder docBuilder) {
    final Map<String, Evaluator> evaluators = new HashMap<String, Evaluator>();
    evaluators.put(DATE_FORMAT_EVALUATOR, getDateFormatEvaluator());
    evaluators.put(SQL_ESCAPE_EVALUATOR, getSqlEscapingEvaluator());
    evaluators.put(URL_ENCODE_EVALUATOR, getUrlEvaluator());
    evaluators.put(ESCAPE_SOLR_QUERY_CHARS, getSolrQueryEscapingEvaluator());
    SolrCore core = docBuilder == null ? null : docBuilder.dataImporter.getCore();
    for (Map<String, String> map : fn) {
      try {
        evaluators.put(map.get(NAME), (Evaluator) loadClass(map.get(CLASS), core).newInstance());
      } catch (Exception e) {
        wrapAndThrow(SEVERE, e, "Unable to instantiate evaluator: " + map.get(CLASS));
      }
    }

    return new HashMap<String, Object>() {
      @Override
      public String get(Object key) {
        if (key == null)
          return null;
        Matcher m = FORMAT_METHOD.matcher((String) key);
        if (!m.find())
          return null;
        String fname = m.group(1);
        Evaluator evaluator = evaluators.get(fname);
        if (evaluator == null)
          return null;
        VariableResolverImpl vri = VariableResolverImpl.CURRENT_VARIABLE_RESOLVER.get();
        return evaluator.evaluate(m.group(2), Context.CURRENT_CONTEXT.get());
      }

    };
  }

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
  public static List parseParams(String expression, VariableResolver vr) {
    List result = new ArrayList();
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
          result.add(new VariableWrapper(ss[i], vr));
        }
      }
    }
    return result;
  }

  public static class VariableWrapper {
    String varName;
    VariableResolver vr;

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

  static DateMathParser dateMathParser = new DateMathParser(TimeZone
          .getDefault(), Locale.getDefault()){
    @Override
    public Date getNow() {
      return new Date();
    }
  };

}
