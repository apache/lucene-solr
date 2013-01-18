package org.apache.solr.handler.dataimport;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.WeakHashMap;

import org.apache.solr.handler.dataimport.config.EntityField;
import org.apache.solr.util.DateMathParser;

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
 * </p>
 */
public class DateFormatEvaluator extends Evaluator {
  
  public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
  Map<DateFormatCacheKey, SimpleDateFormat> cache = new WeakHashMap<DateFormatCacheKey, SimpleDateFormat>();
  Map<String, Locale> availableLocales = new HashMap<String, Locale>();
  Set<String> availableTimezones = new HashSet<String>();
  
  class DateFormatCacheKey {
    DateFormatCacheKey(Locale l, TimeZone tz, String df) {
      this.locale = l;
      this.timezone = tz;
      this.dateFormat = df;
    }
    Locale locale;
    TimeZone timezone;
    String dateFormat;
  }
  
  public DateFormatEvaluator() {  
    for (Locale locale : Locale.getAvailableLocales()) {
      availableLocales.put(locale.toString(), locale);
    }
    for (String tz : TimeZone.getAvailableIDs()) {
      availableTimezones.add(tz);
    }
  }
  private SimpleDateFormat getDateFormat(String pattern, TimeZone timezone, Locale locale) {
    DateFormatCacheKey dfck = new DateFormatCacheKey(locale, timezone, pattern);
    SimpleDateFormat sdf = cache.get(dfck);
    if(sdf == null) {
      sdf = new SimpleDateFormat(pattern, locale);
      sdf.setTimeZone(timezone);
      cache.put(dfck, sdf);
    }
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
    Locale locale = Locale.ROOT;
    if(l.size()>2) {
      Object localeObj = l.get(2);
      String localeStr = null;
      if (localeObj  instanceof VariableWrapper) {
        localeStr = ((VariableWrapper) localeObj).resolve().toString();        
      } else {
        localeStr = localeObj.toString();
      }
      locale = availableLocales.get(localeStr);
      if(locale==null) {
        throw new DataImportHandlerException(SEVERE, "Unsupported locale: " + localeStr);
      }
    }
    TimeZone tz = TimeZone.getDefault();
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
      VariableWrapper variableWrapper = (VariableWrapper) o;
      Object variableval = variableWrapper.resolve();
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
    } else {
      String datemathfmt = o.toString();
      datemathfmt = datemathfmt.replaceAll("NOW", "");
      try {
        date = getDateMathParser(locale, tz).parseMath(datemathfmt);
      } catch (ParseException e) {
        wrapAndThrow(SEVERE, e, "Invalid expression for date");
      }
    }
    return fmt.format(date);
  }
  static DateMathParser getDateMathParser(Locale l, TimeZone tz) {
    return new DateMathParser(tz, l) {
      @Override
      public Date getNow() {
        return new Date();
      }
    };
  }
}
