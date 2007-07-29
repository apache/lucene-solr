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

package org.apache.solr.schema;

import org.apache.solr.common.SolrException;
import org.apache.solr.request.XMLWriter;
import org.apache.solr.request.TextResponseWriter;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.SortField;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.function.OrdFieldSource;
import org.apache.solr.util.DateMathParser;
  
import java.util.Map;
import java.io.IOException;
import java.util.Date;
import java.util.TimeZone;
import java.util.Locale;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.text.ParseException;

// TODO: make a FlexibleDateField that can accept dates in multiple
// formats, better for human entered dates.

// TODO: make a DayField that only stores the day?


/**
 * FieldType that can represent any Date/Time with millisecond precisison.
 * <p>
 * Date Format for the XML, incoming and outgoing:
 * </p>
 * <blockquote>
 * A date field shall be of the form 1995-12-31T23:59:59Z
 * The trailing "Z" designates UTC time and is mandatory.
 * Optional fractional seconds are allowed: 1995-12-31T23:59:59.999Z
 * All other parts are mandatory.
 * </blockquote>
 * <p>
 * This format was derived to be standards compliant (ISO 8601) and is a more
 * restricted form of the canonical representation of dateTime from XML
 * schema part 2.
 * http://www.w3.org/TR/xmlschema-2/#dateTime
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
 * <p>
 * This FieldType also supports incoming "Date Math" strings for computing
 * values by adding/rounding internals of time relative either an explicit
 * datetime (in theformat specified above) or the literal string "NOW",
 * ie: "NOW+1YEAR", "NOW/DAY", 1995-12-31T23:59:59.999Z+5MINUTES, etc...
 * -- see {@link DateMathParser} for more examples.
 * </p>
 *
 * @version $Id$
 * @see <a href="http://www.w3.org/TR/xmlschema-2/#dateTime">XML schema part 2</a>
 *
 */
public class DateField extends FieldType {

  public static TimeZone UTC = TimeZone.getTimeZone("UTC");
  
  // The XML (external) date format will sort correctly, except if
  // fractions of seconds are present (because '.' is lower than 'Z').
  // The easiest fix is to simply remove the 'Z' for the internal
  // format.
  
  protected void init(IndexSchema schema, Map<String,String> args) {
  }

  protected static String NOW = "NOW";
  protected static char Z = 'Z';
  
  public String toInternal(String val) {
    final int len=val.length();
    if (val.charAt(len-1) == Z) {
      // check common case first, simple datetime
      // NOTE: not parsed to ensure correctness
      return val.substring(0,len-1);
    }
    return toInternal(parseMath(null, val));
  }

  /**
   * Parses a String which may be a date (in the standard format)
   * followed by an optional math expression.
   * @param now an optional fixed date to use as "NOW" in the DateMathParser
   * @param val the string to parse
   */
  public Date parseMath(Date now, String val) {
    String math = null;
    /* :TODO: let Locale/TimeZone come from init args for rounding only */
    final DateMathParser p = new DateMathParser(UTC, Locale.US);
    
    if (null != now) p.setNow(now);
    
    if (val.startsWith(NOW)) {
      math = val.substring(NOW.length());
    } else {
      final int zz = val.indexOf(Z);
      if (0 < zz) {
        math = val.substring(zz+1);
        try {
          p.setNow(toObject(val.substring(0,zz)));
        } catch (ParseException e) {
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
                                   "Invalid Date in Date Math String:'"
                                   +val+'\'',e);
        }
      } else {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
                                 "Invalid Date String:'" +val+'\'');
      }
    }

    if (null == math || math.equals("")) {
      return p.getNow();
    }
    
    try {
      return p.parseMath(math);
    } catch (ParseException e) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
                               "Invalid Date Math String:'" +val+'\'',e);
    }
  }
  
  public String toInternal(Date val) {
    return getThreadLocalDateFormat().format(val);
  }

  public String indexedToReadable(String indexedForm) {
    return indexedForm + Z;
  }

  public String toExternal(Fieldable f) {
    return indexedToReadable(f.stringValue());
  }
  public Date toObject(String indexedForm) throws java.text.ParseException {
    return getThreadLocalDateFormat().parse(indexedToReadable(indexedForm));
  }

  @Override
  public Date toObject(Fieldable f) {
    try {
      return getThreadLocalDateFormat().parse( toExternal(f) );
    }
    catch( ParseException ex ) {
      throw new RuntimeException( ex );
    }
  }

  public SortField getSortField(SchemaField field,boolean reverse) {
    return getStringSort(field,reverse);
  }

  public ValueSource getValueSource(SchemaField field) {
    return new OrdFieldSource(field.name);
  }

  public void write(XMLWriter xmlWriter, String name, Fieldable f) throws IOException {
    xmlWriter.writeDate(name, toExternal(f));
  }

  public void write(TextResponseWriter writer, String name, Fieldable f) throws IOException {
    writer.writeDate(name, toExternal(f));
  }

  /**
   * Returns a formatter that can be use by the current thread if needed to
   * convert Date objects to the Internal representation.
   */
  protected DateFormat getThreadLocalDateFormat() {
  
    return fmtThreadLocal.get();
  }

  private static ThreadLocalDateFormat fmtThreadLocal
    = new ThreadLocalDateFormat();
  
  private static class ThreadLocalDateFormat extends ThreadLocal<DateFormat> {
    DateFormat proto;
    public ThreadLocalDateFormat() {
      super();
      SimpleDateFormat tmp =
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.US);
      tmp.setTimeZone(UTC);
      proto = tmp;
    }
    
    protected DateFormat initialValue() {
      return (DateFormat) proto.clone();
    }
  }
  
}
