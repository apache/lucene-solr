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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IllformedLocaleException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.handler.dataimport.config.EntityField;
import org.apache.solr.util.DateMathParser;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

/**
 * <p>Formats values using a given date format. </p>
 * <p>Pass three parameters:
 * <ul>
 *  <li>An {@link EntityField} or a date expression to be parsed with 
 *      the {@link DateMathParser} class  If the value is in a String, 
 *      then it is assumed to be a datemath expression, otherwise it 
 *      resolved using a {@link VariableResolver} instance</li>
 *  <li>A date format see {@link SimpleDateFormat} for the syntax.</li>
 *  <li>The {@link Locale} to parse.  
 *      (optional. Defaults to the Root Locale) </li>
 * </ul>
 */
public class DateFormatEvaluator extends Evaluator {
  
  public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
  protected Map<String, Locale> availableLocales = new HashMap<>();
  protected Set<String> availableTimezones = new HashSet<>();

  @SuppressForbidden(reason = "Usage of outdated locale parsing with Locale#toString() because of backwards compatibility")
  public DateFormatEvaluator() {  
    for (Locale locale : Locale.getAvailableLocales()) {
      availableLocales.put(locale.toString(), locale);
    }
    for (String tz : TimeZone.getAvailableIDs()) {
      availableTimezones.add(tz);
    }
  }
  
  private SimpleDateFormat getDateFormat(String pattern, TimeZone timezone, Locale locale) {
    final SimpleDateFormat sdf = new SimpleDateFormat(pattern, locale);
    sdf.setTimeZone(timezone);
    return sdf;
  }
  
  @Override
  public String evaluate(String expression, Context context) {
    List<Object> l = parseParams(expression, context.getVariableResolver());
    if (l.size() < 2 || l.size() > 4) {
      throw new DataImportHandlerException(SEVERE, "'formatDate()' must have two, three or four parameters ");
    }
    Object o = l.get(0);
    Object format = l.get(1);
    if (format instanceof VariableWrapper) {
      VariableWrapper wrapper = (VariableWrapper) format;
      o = wrapper.resolve();
      format = o.toString();
    }
    Locale locale = Locale.ENGLISH; // we default to ENGLISH for dates for full Java 9 compatibility
    if(l.size()>2) {
      Object localeObj = l.get(2);
      String localeStr = null;
      if (localeObj  instanceof VariableWrapper) {
        localeStr = ((VariableWrapper) localeObj).resolve().toString();
      } else {
        localeStr = localeObj.toString();
      }
      locale = availableLocales.get(localeStr);
      if (locale == null) try {
        locale = new Locale.Builder().setLanguageTag(localeStr).build();
      } catch (IllformedLocaleException ex) {
        throw new DataImportHandlerException(SEVERE, "Malformed / non-existent locale: " + localeStr, ex);
      }
    }
    TimeZone tz = TimeZone.getDefault(); // DWS TODO: is this the right default for us?  Deserves explanation if so.
    if(l.size()==4) {
      Object tzObj = l.get(3);
      String tzStr = null;
      if (tzObj  instanceof VariableWrapper) {
        tzStr = ((VariableWrapper) tzObj).resolve().toString();
      } else {
        tzStr = tzObj.toString();
      }
      if(availableTimezones.contains(tzStr)) {
        tz = TimeZone.getTimeZone(tzStr);
      } else {
        throw new DataImportHandlerException(SEVERE, "Unsupported Timezone: " + tzStr);
      }
    }
    String dateFmt = format.toString();
    SimpleDateFormat fmt = getDateFormat(dateFmt, tz, locale);
    Date date = null;
    if (o instanceof VariableWrapper) {
      date = evaluateWrapper((VariableWrapper) o, locale, tz);
    } else {
      date = evaluateString(o.toString(), locale, tz);
    }
    return fmt.format(date);
  }

  /**
   * NOTE: declared as a method to allow for extensibility
   *
   * @lucene.experimental this API is experimental and subject to change
   * @return the result of evaluating a string
   */
  protected Date evaluateWrapper(VariableWrapper variableWrapper, Locale locale, TimeZone tz) {
    Date date = null;
    Object variableval = resolveWrapper(variableWrapper,locale,tz);
    if (variableval instanceof Date) {
      date = (Date) variableval;
    } else {
      String s = variableval.toString();
      try {
        date = getDateFormat(DEFAULT_DATE_FORMAT, tz, locale).parse(s);
      } catch (ParseException exp) {
        wrapAndThrow(SEVERE, exp, "Invalid expression for date");
      }
    }
    return date;
  }

  /**
   * NOTE: declared as a method to allow for extensibility
   * @lucene.experimental
   * @return the result of evaluating a string
   */
  protected Date evaluateString(String datemathfmt, Locale locale, TimeZone tz) {
    // note: DMP does not use the locale but perhaps a subclass might use it, for e.g. parsing a date in a custom
    // string that doesn't necessarily have date math?
    //TODO refactor DateMathParser.parseMath a bit to have a static method for this logic.
    if (datemathfmt.startsWith("NOW")) {
      datemathfmt = datemathfmt.substring("NOW".length());
    }
    try {
      DateMathParser parser = new DateMathParser(tz);
      parser.setNow(new Date());// thus do *not* use SolrRequestInfo
      return parser.parseMath(datemathfmt);
    } catch (ParseException e) {
      throw wrapAndThrow(SEVERE, e, "Invalid expression for date");
    }
  }

  /**
   * NOTE: declared as a method to allow for extensibility
   * @lucene.experimental
   * @return the result of resolving the variable wrapper
   */
  protected Object resolveWrapper(VariableWrapper variableWrapper, Locale locale, TimeZone tz) {
    return variableWrapper.resolve();
  }

}
