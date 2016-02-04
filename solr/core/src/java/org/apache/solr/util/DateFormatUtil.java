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
package org.apache.solr.util;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.request.SolrQueryRequest;

public final class DateFormatUtil {

  public static final TimeZone UTC = TimeZone.getTimeZone("UTC");
  /**
   * Fixed TimeZone (UTC) needed for parsing/formatting Dates in the
   * canonical representation.
   */
  public static final TimeZone CANONICAL_TZ = UTC;
  /**
   * Fixed Locale needed for parsing/formatting Milliseconds in the
   * canonical representation.
   */
  public static final Locale CANONICAL_LOCALE = Locale.ROOT;
  public static final String NOW = "NOW";
  public static final char Z = 'Z';
  /**
   * Thread safe DateFormat that can <b>format</b> in the canonical
   * ISO8601 date format, not including the trailing "Z" (since it is
   * left off in the internal indexed values)
   */
  public final static ThreadLocalDateFormat FORMAT_THREAD_LOCAL
      = new ThreadLocalDateFormat(new ISO8601CanonicalDateFormat());

  private DateFormatUtil() {}

  /**
   * Parses a String which may be a date (in the standard format)
   * followed by an optional math expression.
   * @param now an optional fixed date to use as "NOW" in the DateMathParser
   * @param val the string to parse
   */
  public static Date parseMath(Date now, String val) {
    String math;
    final DateMathParser p = new DateMathParser();
  
    if (null != now) p.setNow(now);
  
    if (val.startsWith(NOW)) {
      math = val.substring(NOW.length());
    } else {
      final int zz = val.indexOf(Z);
      if (0 < zz) {
        math = val.substring(zz+1);
        try {
          // p.setNow(toObject(val.substring(0,zz)));
          p.setNow(parseDate(val.substring(0,zz+1)));
        } catch (ParseException e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                  "Invalid Date in Date Math String:'" + val + '\'', e);
        }
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                "Invalid Date String:'" +val+'\'');
      }
    }
  
    if (null == math || math.equals("")) {
      return p.getNow();
    }
  
    try {
      return p.parseMath(math);
    } catch (ParseException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                              "Invalid Date Math String:'" +val+'\'',e);
    }
  }

  /**
   * Return the standard human readable form of the date (with trailing 'Z')
   */
  public static String formatExternal(Date d) {
    return FORMAT_THREAD_LOCAL.get().format(d) + Z;
  }

  /**
   * Return the standard human readable form of the date
   */
  public static String formatDate(Date d) {
    return FORMAT_THREAD_LOCAL.get().format(d);
  }

  /**
   * Thread safe method that can be used to parse a Date
   * without the trailing 'Z'
   */
  public static Date parseDate(String s) throws ParseException {
    return FORMAT_THREAD_LOCAL.get().parse(s);
  }
  
  /** Parse a date string in the standard format, or any supported by DateUtil.parseDate */
  public static Date parseDateLenient(String s, SolrQueryRequest req) throws ParseException {
    // request could define timezone in the future
    try {
      return DateFormatUtil.FORMAT_THREAD_LOCAL.get().parse(s);
    } catch (Exception e) {
      return DateUtil.parseDate(s);
    }
  }

  /**
   * Parses a String which may be a date
   * followed by an optional math expression.
   * @param now an optional fixed date to use as "NOW" in the DateMathParser
   * @param val the string to parse
   */
  public static Date parseMathLenient(Date now, String val, SolrQueryRequest req) {
    String math;
    final DateMathParser p = new DateMathParser();

    if (null != now) p.setNow(now);

    if (val.startsWith(DateFormatUtil.NOW)) {
      math = val.substring(DateFormatUtil.NOW.length());
    } else {
      final int zz = val.indexOf(DateFormatUtil.Z);
      if (0 < zz) {
        math = val.substring(zz+1);
        try {
          // p.setNow(toObject(val.substring(0,zz)));
          p.setNow(parseDateLenient(val.substring(0,zz+1), req));
        } catch (ParseException e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                  "Invalid Date in Date Math String: '" + val + '\'', e);
        }
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                "Invalid Date String: '" +val+'\'');
      }
    }

    if (null == math || math.equals("")) {
      return p.getNow();
    }

    try {
      return p.parseMath(math);
    } catch (ParseException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                              "Invalid Date Math String: '" +val+'\'',e);
    }
  }

  @SuppressWarnings("serial")
  static class ISO8601CanonicalDateFormat extends SimpleDateFormat {
    
    protected NumberFormat millisParser
        = NumberFormat.getIntegerInstance(CANONICAL_LOCALE);
  
    protected NumberFormat millisFormat = new DecimalFormat
        (".###", new DecimalFormatSymbols(CANONICAL_LOCALE));
  
    public ISO8601CanonicalDateFormat() {
      super("yyyy-MM-dd'T'HH:mm:ss", CANONICAL_LOCALE);
      this.setTimeZone(CANONICAL_TZ);
    }
  
    @Override
    public Date parse(String i, ParsePosition p) {
      /* delegate to SimpleDateFormat for easy stuff */
      Date d = super.parse(i, p);
      int milliIndex = p.getIndex();
      /* worry about the milliseconds ourselves */
      if (null != d &&
          -1 == p.getErrorIndex() &&
          milliIndex + 1 < i.length() &&
          '.' == i.charAt(milliIndex)) {
        p.setIndex(++milliIndex); // NOTE: ++ to chomp '.'
        Number millis = millisParser.parse(i, p);
        if (-1 == p.getErrorIndex()) {
          int endIndex = p.getIndex();
          d = new Date(d.getTime()
              + (long)(millis.doubleValue() * Math.pow(10, (3 - endIndex + milliIndex))));
        }
      }
      return d;
    }
  
    @Override
    public StringBuffer format(Date d, StringBuffer toAppendTo, FieldPosition pos) {
      /* delegate to SimpleDateFormat for easy stuff */
      super.format(d, toAppendTo, pos);
      /* worry about the milliseconds ourselves */
      long millis = d.getTime() % 1000l;
      if (0L == millis) {
        return toAppendTo;
      }
      if (millis < 0L) {
        // original date was prior to epoch
        millis += 1000L;
      }
      int posBegin = toAppendTo.length();
      toAppendTo.append(millisFormat.format(millis / 1000d));
      if (DateFormat.MILLISECOND_FIELD == pos.getField()) {
        pos.setBeginIndex(posBegin);
        pos.setEndIndex(toAppendTo.length());
      }
      return toAppendTo;
    }
  
    @Override
    public DateFormat clone() {
      ISO8601CanonicalDateFormat c = (ISO8601CanonicalDateFormat)super.clone();
      c.millisParser = NumberFormat.getIntegerInstance(CANONICAL_LOCALE);
      c.millisFormat = new DecimalFormat(".###", new DecimalFormatSymbols(CANONICAL_LOCALE));
      return c;
    }
  }

  public static class ThreadLocalDateFormat extends ThreadLocal<DateFormat> {
    private final DateFormat proto;
    
    public ThreadLocalDateFormat(DateFormat d) {
      super();
      proto = d;
    }
    
    @Override
    protected DateFormat initialValue() {
      return (DateFormat) proto.clone();
    }
  }
}
