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

package org.apache.solr.schema;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.update.processor.TimestampUpdateProcessorFactory; //jdoc
import org.apache.solr.util.DateMathParser;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.NumericRangeQuery;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Date;
import java.util.TimeZone;

/**
 * FieldType that can represent any Date/Time with millisecond precision.
 * <p>
 * Date Format for the XML, incoming and outgoing:
 * </p>
 * <blockquote>
 * A date field shall be of the form 1995-12-31T23:59:59Z
 * The trailing "Z" designates UTC time and is mandatory
 * (See below for an explanation of UTC).
 * Optional fractional seconds are allowed, as long as they do not end
 * in a trailing 0 (but any precision beyond milliseconds will be ignored).
 * All other parts are mandatory.
 * </blockquote>
 * <p>
 * This format was derived to be standards compliant (ISO 8601) and is a more
 * restricted form of the
 * <a href="http://www.w3.org/TR/xmlschema-2/#dateTime-canonical-representation">canonical
 * representation of dateTime</a> from XML schema part 2.  Examples...
 * </p>
 * <ul>
 *   <li>1995-12-31T23:59:59Z</li>
 *   <li>1995-12-31T23:59:59.9Z</li>
 *   <li>1995-12-31T23:59:59.99Z</li>
 *   <li>1995-12-31T23:59:59.999Z</li>
 * </ul>
 * <p>
 * Note that TrieDateField is lenient with regards to parsing fractional
 * seconds that end in trailing zeros and will ensure that those values
 * are indexed in the correct canonical format.
 * </p>
 * <p>
 * This FieldType also supports incoming "Date Math" strings for computing
 * values by adding/rounding internals of time relative either an explicit
 * datetime (in the format specified above) or the literal string "NOW",
 * ie: "NOW+1YEAR", "NOW/DAY", "1995-12-31T23:59:59.999Z+5MINUTES", etc...
 * -- see {@link DateMathParser} for more examples.
 * </p>
 * <p>
 * <b>NOTE:</b> Although it is possible to configure a <code>TrieDateField</code>
 * instance with a default value of "<code>NOW</code>" to compute a timestamp
 * of when the document was indexed, this is not advisable when using SolrCloud
 * since each replica of the document may compute a slightly different value.
 * {@link TimestampUpdateProcessorFactory} is recommended instead.
 * </p>
 *
 * <p>
 * Explanation of "UTC"...
 * </p>
 * <blockquote>
 * "In 1970 the Coordinated Universal Time system was devised by an
 * international advisory group of technical experts within the International
 * Telecommunication Union (ITU).  The ITU felt it was best to designate a
 * single abbreviation for use in all languages in order to minimize
 * confusion.  Since unanimous agreement could not be achieved on using
 * either the English word order, CUT, or the French word order, TUC, the
 * acronym UTC was chosen as a compromise."
 * </blockquote>
 *
 * @see TrieField
 */
public class TrieDateField extends TrieField implements DateValueFieldType {
  {
    type = TrieTypes.DATE;
  }
  
  public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  /**
   * Fixed TimeZone (UTC) needed for parsing/formatting Dates in the
   * canonical representation.
   */
  protected static final TimeZone CANONICAL_TZ = UTC;
  /**
   * Fixed Locale needed for parsing/formatting Milliseconds in the
   * canonical representation.
   */
  protected static final Locale CANONICAL_LOCALE = Locale.ROOT;

  protected static final String NOW = "NOW";
  protected static final char Z = 'Z';


  /**
   * Parses a String which may be a date (in the standard format)
   * followed by an optional math expression.
   * @param now an optional fixed date to use as "NOW" in the DateMathParser
   * @param val the string to parse
   */
  public Date parseMath(Date now, String val) {
    String math = null;
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
   * Thread safe method that can be used by subclasses to format a Date
   * without the trailing 'Z'.
   */
  protected String formatDate(Date d) {
    return fmtThreadLocal.get().format(d);
  }

  /**
   * Return the standard human readable form of the date
   */
  public static String formatExternal(Date d) {
    return fmtThreadLocal.get().format(d) + Z;
  }

  /**
   * @see #formatExternal
   */
  public String toExternal(Date d) {
    return formatExternal(d);
  }

  /**
   * Thread safe method that can be used by subclasses to parse a Date
   * without the trailing 'Z'
   */
  public static Date parseDate(String s) throws ParseException {
    return fmtThreadLocal.get().parse(s);
  }

  /** Parse a date string in the standard format, or any supported by DateUtil.parseDate */
  public Date parseDateLenient(String s, SolrQueryRequest req) throws ParseException {
    // request could define timezone in the future
    try {
      return fmtThreadLocal.get().parse(s);
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
  public Date parseMathLenient(Date now, String val, SolrQueryRequest req) {
    String math = null;
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

  /**
   * Thread safe DateFormat that can <b>format</b> in the canonical
   * ISO8601 date format, not including the trailing "Z" (since it is
   * left off in the internal indexed values)
   */
  private final static ThreadLocalDateFormat fmtThreadLocal
      = new ThreadLocalDateFormat(new ISO8601CanonicalDateFormat());

  private static class ISO8601CanonicalDateFormat extends SimpleDateFormat {

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

  private static class ThreadLocalDateFormat extends ThreadLocal<DateFormat> {
    DateFormat proto;
    public ThreadLocalDateFormat(DateFormat d) {
      super();
      proto = d;
    }
    @Override
    protected DateFormat initialValue() {
      return (DateFormat)proto.clone();
    }
  }

  @Override
  public Date toObject(StorableField f) {
    return (Date)super.toObject(f);
  }

  /** TrieDateField specific range query */
  public Query getRangeQuery(QParser parser, SchemaField sf, Date min, Date max, boolean minInclusive, boolean maxInclusive) {
    return NumericRangeQuery.newLongRange(sf.getName(), precisionStep,
              min == null ? null : min.getTime(),
              max == null ? null : max.getTime(),
              minInclusive, maxInclusive);
  }

  @Override
  public Object toNativeType(Object val) {
    if(val==null) return null;
    if (val instanceof Date) return  val;

    if (val instanceof String) return parseMath(null,(String)val);

    return super.toNativeType(val);
  }
}
