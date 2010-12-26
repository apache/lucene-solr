package org.apache.solr.common.util;
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

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.TimeZone;


/**
 * This class has some code from HttpClient DateUtil.
 */
public class DateUtil {
  //start HttpClient
  /**
   * Date format pattern used to parse HTTP date headers in RFC 1123 format.
   */
  public static final String PATTERN_RFC1123 = "EEE, dd MMM yyyy HH:mm:ss zzz";

  /**
   * Date format pattern used to parse HTTP date headers in RFC 1036 format.
   */
  public static final String PATTERN_RFC1036 = "EEEE, dd-MMM-yy HH:mm:ss zzz";

  /**
   * Date format pattern used to parse HTTP date headers in ANSI C
   * <code>asctime()</code> format.
   */
  public static final String PATTERN_ASCTIME = "EEE MMM d HH:mm:ss yyyy";
  //These are included for back compat
  private static final Collection<String> DEFAULT_HTTP_CLIENT_PATTERNS = Arrays.asList(
          PATTERN_ASCTIME, PATTERN_RFC1036, PATTERN_RFC1123);

  private static final Date DEFAULT_TWO_DIGIT_YEAR_START;

  static {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.US);
    calendar.set(2000, Calendar.JANUARY, 1, 0, 0);
    DEFAULT_TWO_DIGIT_YEAR_START = calendar.getTime();
  }

  private static final TimeZone GMT = TimeZone.getTimeZone("GMT");

  //end HttpClient

  //---------------------------------------------------------------------------------------

  /**
   * A suite of default date formats that can be parsed, and thus transformed to the Solr specific format
   */
  public static final Collection<String> DEFAULT_DATE_FORMATS = new ArrayList<String>();

  static {
    DEFAULT_DATE_FORMATS.add("yyyy-MM-dd'T'HH:mm:ss'Z'");
    DEFAULT_DATE_FORMATS.add("yyyy-MM-dd'T'HH:mm:ss");
    DEFAULT_DATE_FORMATS.add("yyyy-MM-dd");
    DEFAULT_DATE_FORMATS.add("yyyy-MM-dd hh:mm:ss");
    DEFAULT_DATE_FORMATS.add("yyyy-MM-dd HH:mm:ss");
    DEFAULT_DATE_FORMATS.add("EEE MMM d hh:mm:ss z yyyy");
    DEFAULT_DATE_FORMATS.addAll(DEFAULT_HTTP_CLIENT_PATTERNS);
  }

  /**
   * Returns a formatter that can be use by the current thread if needed to
   * convert Date objects to the Internal representation.
   *
   * @param d The input date to parse
   * @return The parsed {@link java.util.Date}
   * @throws java.text.ParseException If the input can't be parsed
   * @throws org.apache.commons.httpclient.util.DateParseException
   *                                  If the input can't be parsed
   */
  public static Date parseDate(String d) throws ParseException {
    return parseDate(d, DEFAULT_DATE_FORMATS);
  }

  public static Date parseDate(String d, Collection<String> fmts) throws ParseException {
    // 2007-04-26T08:05:04Z
    if (d.endsWith("Z") && d.length() > 20) {
      return getThreadLocalDateFormat().parse(d);
    }
    return parseDate(d, fmts, null);
  }

  /**
   * Slightly modified from org.apache.commons.httpclient.util.DateUtil.parseDate
   * <p/>
   * Parses the date value using the given date formats.
   *
   * @param dateValue   the date value to parse
   * @param dateFormats the date formats to use
   * @param startDate   During parsing, two digit years will be placed in the range
   *                    <code>startDate</code> to <code>startDate + 100 years</code>. This value may
   *                    be <code>null</code>. When <code>null</code> is given as a parameter, year
   *                    <code>2000</code> will be used.
   * @return the parsed date
   * @throws ParseException if none of the dataFormats could parse the dateValue
   */
  public static Date parseDate(
          String dateValue,
          Collection<String> dateFormats,
          Date startDate
  ) throws ParseException {

    if (dateValue == null) {
      throw new IllegalArgumentException("dateValue is null");
    }
    if (dateFormats == null) {
      dateFormats = DEFAULT_HTTP_CLIENT_PATTERNS;
    }
    if (startDate == null) {
      startDate = DEFAULT_TWO_DIGIT_YEAR_START;
    }
    // trim single quotes around date if present
    // see issue #5279
    if (dateValue.length() > 1
            && dateValue.startsWith("'")
            && dateValue.endsWith("'")
            ) {
      dateValue = dateValue.substring(1, dateValue.length() - 1);
    }

    SimpleDateFormat dateParser = null;
    Iterator formatIter = dateFormats.iterator();

    while (formatIter.hasNext()) {
      String format = (String) formatIter.next();
      if (dateParser == null) {
        dateParser = new SimpleDateFormat(format, Locale.US);
        dateParser.setTimeZone(GMT);
        dateParser.set2DigitYearStart(startDate);
      } else {
        dateParser.applyPattern(format);
      }
      try {
        return dateParser.parse(dateValue);
      } catch (ParseException pe) {
        // ignore this exception, we will try the next format
      }
    }

    // we were unable to parse the date
    throw new ParseException("Unable to parse the date " + dateValue, 0);
  }


  /**
   * Returns a formatter that can be use by the current thread if needed to
   * convert Date objects to the Internal representation.
   *
   * @return The {@link java.text.DateFormat} for the current thread
   */
  public static DateFormat getThreadLocalDateFormat() {
    return fmtThreadLocal.get();
  }

  public static TimeZone UTC = TimeZone.getTimeZone("UTC");
  private static ThreadLocalDateFormat fmtThreadLocal = new ThreadLocalDateFormat();

  private static class ThreadLocalDateFormat extends ThreadLocal<DateFormat> {
    DateFormat proto;

    public ThreadLocalDateFormat() {
      super();
      //2007-04-26T08:05:04Z
      SimpleDateFormat tmp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
      tmp.setTimeZone(UTC);
      proto = tmp;
    }

    @Override
    protected DateFormat initialValue() {
      return (DateFormat) proto.clone();
    }
  }

  /** Formats the date and returns the calendar instance that was used (which may be reused) */
  public static Calendar formatDate(Date date, Calendar cal, Appendable out) throws IOException {
    // using a stringBuilder for numbers can be nice since
    // a temporary string isn't used (it's added directly to the
    // builder's buffer.

    StringBuilder sb = out instanceof StringBuilder ? (StringBuilder)out : new StringBuilder();
    if (cal==null) cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.US);
    cal.setTime(date);

    int i = cal.get(Calendar.YEAR);
    sb.append(i);
    sb.append('-');
    i = cal.get(Calendar.MONTH) + 1;  // 0 based, so add 1
    if (i<10) sb.append('0');
    sb.append(i);
    sb.append('-');
    i=cal.get(Calendar.DAY_OF_MONTH);
    if (i<10) sb.append('0');
    sb.append(i);
    sb.append('T');
    i=cal.get(Calendar.HOUR_OF_DAY); // 24 hour time format
    if (i<10) sb.append('0');
    sb.append(i);
    sb.append(':');
    i=cal.get(Calendar.MINUTE);
    if (i<10) sb.append('0');
    sb.append(i);
    sb.append(':');
    i=cal.get(Calendar.SECOND);
    if (i<10) sb.append('0');
    sb.append(i);
    i=cal.get(Calendar.MILLISECOND);
    if (i != 0) {
      sb.append('.');
      if (i<100) sb.append('0');
      if (i<10) sb.append('0');
      sb.append(i);

      // handle canonical format specifying fractional
      // seconds shall not end in '0'.  Given the slowness of
      // integer div/mod, simply checking the last character
      // is probably the fastest way to check.
      int lastIdx = sb.length()-1;
      if (sb.charAt(lastIdx)=='0') {
        lastIdx--;
        if (sb.charAt(lastIdx)=='0') {
          lastIdx--;
        }
        sb.setLength(lastIdx+1);
      }

    }
    sb.append('Z');

    if (out != sb)
      out.append(sb);

    return cal;
  }


}