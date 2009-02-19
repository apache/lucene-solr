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

import static org.apache.solr.handler.dataimport.DocBuilder.loadClass;
import static org.apache.solr.handler.dataimport.DataConfig.CLASS;
import static org.apache.solr.handler.dataimport.DataConfig.NAME;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.core.SolrCore;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * Holds definitions for evaluators provided by DataImportHandler
 * </p>
 * <p/>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p/>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class EvaluatorBag {

  public static final String DATE_FORMAT_EVALUATOR = "formatDate";

  public static final String URL_ENCODE_EVALUATOR = "encodeUrl";

  public static final String SQL_ESCAPE_EVALUATOR = "escapeSql";
  static final Pattern FORMAT_METHOD = Pattern
          .compile("^(\\w*?)\\((.*?)\\)$");

  /**
   * <p/>
   * Returns an <code>Evaluator</code> instance meant to be used for escaping
   * values in SQL queries.
   * </p>
   * <p/>
   * It escapes the value of the given expression by replacing all occurrences
   * of single-quotes by two single-quotes and similarily for double-quotes
   * </p>
   *
   * @return an <code>Evaluator</code> instance capable of SQL-escaping
   *         expressions.
   */
  public static Evaluator getSqlEscapingEvaluator() {
    return new Evaluator() {
      public String evaluate(String expression, Context context) {
        Object o = context.getVariableResolver().resolve(expression);

        if (o == null)
          return null;

        return o.toString().replaceAll("'", "''").replaceAll("\"", "\"\"");
      }
    };
  }

  /**
   * <p/>
   * Returns an <code>Evaluator</code> instance capable of URL-encoding
   * expressions. The expressions are evaluated using a
   * <code>VariableResolver</code>
   * </p>
   *
   * @return an <code>Evaluator</code> instance capable of URL-encoding
   *         expressions.
   */
  public static Evaluator getUrlEvaluator() {
    return new Evaluator() {
      public String evaluate(String expression, Context context) {
        Object value = null;
        try {
          value = context.getVariableResolver().resolve(expression);
          if (value == null)
            return null;

          return URLEncoder.encode(value.toString(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
          throw new DataImportHandlerException(
                  DataImportHandlerException.SEVERE,
                  "Unable to encode expression: " + expression + " with value: "
                          + value, e);
        }
      }
    };
  }

  /**
   * <p/>
   * Returns an <code>Evaluator</code> instance capable of formatting values
   * using a given date format.
   * </p>
   * <p/>
   * The value to be formatted can be a entity.field or a date expression parsed
   * with <code>DateMathParser</code> class. If the value is in single quotes,
   * then it is assumed to be a datemath expression, otherwise it resolved using
   * a <code>VariableResolver</code> instance
   * </p>
   *
   * @return an Evaluator instance capable of formatting values to a given date
   *         format
   * @see DateMathParser
   */
  public static Evaluator getDateFormatEvaluator() {
    return new Evaluator() {
      public String evaluate(String expression, Context context) {
        CacheEntry e = getCachedData(expression);
        String expr = e.key;
        SimpleDateFormat fmt = e.format;
        Matcher m = IN_SINGLE_QUOTES.matcher(expr);
        if (m.find()) {
          String datemathExpr = m.group(1);
          try {
            Date date = dateMathParser.parseMath(datemathExpr);
            return fmt.format(date);
          } catch (ParseException exp) {
            throw new DataImportHandlerException(
                    DataImportHandlerException.SEVERE,
                    "Invalid expression for date", exp);
          }
        } else {
          Object o = context.getVariableResolver().resolve(expr);
          if (o == null)
            return "";
          Date date = null;
          if (o instanceof Date) {
            date = (Date) o;
          } else {
            String s = o.toString();
            try {
              date = DataImporter.DATE_TIME_FORMAT.get().parse(s);
            } catch (ParseException exp) {
              throw new DataImportHandlerException(
                      DataImportHandlerException.SEVERE,
                      "Invalid expression for date", exp);
            }
          }
          return fmt.format(date);
        }
      }

      private CacheEntry getCachedData(String str) {
        CacheEntry result = cache.get(str);
        if (result != null)
          return result;
        Matcher m = FORMAT_METHOD.matcher(str);
        String expr, pattern;
        if (m.find()) {
          expr = m.group(1).trim();
          if (IN_SINGLE_QUOTES.matcher(expr).find()) {
            expr = expr.replaceAll("NOW", "");
          }
          pattern = m.group(2).trim();
          cache.put(str, new CacheEntry(expr, new SimpleDateFormat(pattern)));
          return cache.get(str);
        } else {
          throw new DataImportHandlerException(
                  DataImportHandlerException.SEVERE, "Invalid format String : "
                  + "${dataimporter.functions." + str + "}");
        }
      }

      Map<String, CacheEntry> cache = new HashMap<String, CacheEntry>();

      Pattern FORMAT_METHOD = Pattern.compile("^(.*?),(.*?)$");
    };
  }

  static Map<String, Object> getFunctionsNamespace(final List<Map<String, String>> fn, DocBuilder docBuilder) {
    final Map<String, Evaluator> evaluators = new HashMap<String, Evaluator>();
    evaluators.put(DATE_FORMAT_EVALUATOR, getDateFormatEvaluator());
    evaluators.put(SQL_ESCAPE_EVALUATOR, getSqlEscapingEvaluator());
    evaluators.put(URL_ENCODE_EVALUATOR, getUrlEvaluator());
    SolrCore core = docBuilder == null ? null : docBuilder.dataImporter.getCore();
    for (Map<String, String> map : fn) {
      try {
        evaluators.put(map.get(NAME), (Evaluator) loadClass(map.get(CLASS), core).newInstance());
      } catch (Exception e) {
         throw new DataImportHandlerException(
                  DataImportHandlerException.SEVERE,
                  "Unable to instantiate evaluator: " + map.get(CLASS), e);
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
        Context ctx = vri == null ? null : vri.context;
        return evaluator.evaluate(m.group(2), ctx);
      }

    };
  }


  static class CacheEntry {
    public String key;

    public SimpleDateFormat format;

    public CacheEntry(String key, SimpleDateFormat format) {
      this.key = key;
      this.format = format;
    }
  }

  static Pattern IN_SINGLE_QUOTES = Pattern.compile("^'(.*?)'$");

  static DateMathParser dateMathParser = new DateMathParser(TimeZone
          .getDefault(), Locale.getDefault());

}
