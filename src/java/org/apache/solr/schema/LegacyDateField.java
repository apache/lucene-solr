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
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.text.ParseException;

/**
 * This class is <b>NOT</b> recommended for new users and should be
 * considered <b>UNSUPPORTED</b>.
 * <p>
 * In Solr 1.2, <tt>DateField</tt> did not enforce
 * the canonical representation of the ISO 8601 format when parsing
 * incoming data, and did not generation the canonical format when
 * generating dates from "Date Math" strings (particularly as
 * it pertains to milliseconds ending in trailing zeros) -- As a result
 * equivalent dates could not always be compared properly.
 * </p>
 * <p>
 * This class is provided as possible alternative for people who depend on
 * the "broken" behavior of DateField in Solr 1.2
 * (specificly: accepting any input that ends in a 'Z', and
 * formating DateMath expressions using 3 decimals of milliseconds) while
 * still supporting some newer functionality of DateField (ie: DateMath on
 * explicit strings in addition to "NOW")
 * </p>
 * <p>
 * Users that desire 100% backwards compatibility should consider using
 * the Solr 1.2 version of <tt>DateField</tt>
 * </p>
 *
 * @see <a href="https://issues.apache.org/jira/browse/SOLR-552">SOLR-552</a>
 * @see <a href="https://issues.apache.org/jira/browse/SOLR-470">SOLR-470</a>
 * @see <a href="https://issues.apache.org/jira/browse/SOLR-521">SOLR-521</a>
 * @deprecated use {@link DateField}
 */
@Deprecated
public final class LegacyDateField extends DateField {

  /**
   * Overrides the super class to short circut and do no enforcing of
   * the canonical format
   */
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
   * This method returns a DateFormat which does <b>NOT</b> respect the
   * ISO 8601 canonical format with regards to trailing zeros in milliseconds,
   * instead if always formats milliseconds to 3 decimal points.
   */
  protected DateFormat getThreadLocalDateFormat() {
    return fmtThreadLocal.get();
  }

  protected String formatDate(Date d) {
    return getThreadLocalDateFormat().format(d);
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
