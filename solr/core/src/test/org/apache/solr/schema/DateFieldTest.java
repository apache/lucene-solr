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

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.util.DateMathParser;

import org.junit.Ignore;

import java.util.Date;
import java.util.TimeZone;
import java.util.Locale;

public class DateFieldTest extends LuceneTestCase {
  public static TimeZone UTC = TimeZone.getTimeZone("UTC");
  protected DateField f = null;
  protected DateMathParser p = new DateMathParser(UTC, Locale.US);

  @Override
  public void setUp()  throws Exception {
    super.setUp();
    f = new DateField();
  }

  public void assertToI(String expected, String input) {
    assertEquals("Input: " + input, expected, f.toInternal(input));
  }
  
  public void assertToI(String expected, long input) {
    assertEquals("Input: " + input, expected, f.toInternal(new Date(input)));
  }

  public void testToInternal() throws Exception {
    assertToI("1995-12-31T23:59:59.999", "1995-12-31T23:59:59.999666Z");
    assertToI("1995-12-31T23:59:59.999", "1995-12-31T23:59:59.999Z");
    assertToI("1995-12-31T23:59:59.99",  "1995-12-31T23:59:59.99Z");
    assertToI("1995-12-31T23:59:59.9",   "1995-12-31T23:59:59.9Z");
    assertToI("1995-12-31T23:59:59",     "1995-12-31T23:59:59Z");

    // here the input isn't in the canonical form, but we should be forgiving
    assertToI("1995-12-31T23:59:59.99",  "1995-12-31T23:59:59.990Z");
    assertToI("1995-12-31T23:59:59.9",   "1995-12-31T23:59:59.900Z");
    assertToI("1995-12-31T23:59:59.9",   "1995-12-31T23:59:59.90Z");
    assertToI("1995-12-31T23:59:59",     "1995-12-31T23:59:59.000Z");
    assertToI("1995-12-31T23:59:59",     "1995-12-31T23:59:59.00Z");
    assertToI("1995-12-31T23:59:59",     "1995-12-31T23:59:59.0Z");

    // kind of kludgy, but we have other tests for the actual date math
    assertToI(f.toInternal(p.parseMath("/DAY")), "NOW/DAY");

    // as of Solr 1.3
    assertToI("1995-12-31T00:00:00", "1995-12-31T23:59:59Z/DAY");
    assertToI("1995-12-31T00:00:00", "1995-12-31T23:59:59.123Z/DAY");
    assertToI("1995-12-31T00:00:00", "1995-12-31T23:59:59.123999Z/DAY");
  }
  
  public void testToInternalObj() throws Exception {
    assertToI("1995-12-31T23:59:59.999", 820454399999l);
    assertToI("1995-12-31T23:59:59.99",  820454399990l);
    assertToI("1995-12-31T23:59:59.9",   820454399900l);
    assertToI("1995-12-31T23:59:59",     820454399000l);
  }
    
  public void assertParseMath(long expected, String input) {
    Date d = new Date(0);
    assertEquals("Input: "+input, expected, f.parseMath(d, input).getTime());
  }
  
  // as of Solr1.3
  public void testParseMath() {
    assertParseMath(820454699999l, "1995-12-31T23:59:59.999765Z+5MINUTES");
    assertParseMath(820454699999l, "1995-12-31T23:59:59.999Z+5MINUTES");
    assertParseMath(820454699990l, "1995-12-31T23:59:59.99Z+5MINUTES");
    assertParseMath(194918400000l, "1976-03-06T03:06:00Z/DAY");
    
    // here the input isn't in the canonical form, but we should be forgiving
    assertParseMath(820454699990l, "1995-12-31T23:59:59.990Z+5MINUTES");
    assertParseMath(194918400000l, "1976-03-06T03:06:00.0Z/DAY");
    assertParseMath(194918400000l, "1976-03-06T03:06:00.00Z/DAY");
    assertParseMath(194918400000l, "1976-03-06T03:06:00.000Z/DAY");
  }

  public void assertToObject(long expected, String input) throws Exception {
    assertEquals("Input: "+input, expected, f.toObject(input).getTime());
  }
  
  // as of Solr1.3
  public void testToObject() throws Exception {

    // just after epoch
    assertToObject(  5L, "1970-01-01T00:00:00.005Z");
    assertToObject(  0L, "1970-01-01T00:00:00Z");
    assertToObject(370L, "1970-01-01T00:00:00.37Z");
    assertToObject(900L, "1970-01-01T00:00:00.9Z");

    // well after epoch
    assertToObject(820454399987l, "1995-12-31T23:59:59.987666Z");
    assertToObject(820454399987l, "1995-12-31T23:59:59.987Z");
    assertToObject(820454399980l, "1995-12-31T23:59:59.98Z");
    assertToObject(820454399900l, "1995-12-31T23:59:59.9Z");
    assertToObject(820454399000l, "1995-12-31T23:59:59Z");

    // waaaay after epoch
    assertToObject(327434918399005L, "12345-12-31T23:59:59.005Z");
    assertToObject(327434918399000L, "12345-12-31T23:59:59Z");
    assertToObject(327434918399370L, "12345-12-31T23:59:59.37Z");
    assertToObject(327434918399900L, "12345-12-31T23:59:59.9Z");

    // well before epoch
    assertToObject(-52700112001000L, "0299-12-31T23:59:59Z");
    assertToObject(-52700112000877L, "0299-12-31T23:59:59.123Z");
    assertToObject(-52700112000910L, "0299-12-31T23:59:59.09Z");

    // flexible in parsing years less then 4 digits
    assertToObject(-52700112001000L,  "299-12-31T23:59:59Z");

  }
  
  public void testFormatter() {
    // just after epoch
    assertFormat("1970-01-01T00:00:00.005", 5L);
    assertFormat("1970-01-01T00:00:00",     0L);
    assertFormat("1970-01-01T00:00:00.37",  370L);
    assertFormat("1970-01-01T00:00:00.9",   900L);

    // well after epoch
    assertFormat("1999-12-31T23:59:59.005", 946684799005L);
    assertFormat("1999-12-31T23:59:59",     946684799000L);
    assertFormat("1999-12-31T23:59:59.37",  946684799370L);
    assertFormat("1999-12-31T23:59:59.9",   946684799900L);

    // waaaay after epoch
    assertFormat("12345-12-31T23:59:59.005", 327434918399005L);
    assertFormat("12345-12-31T23:59:59",     327434918399000L);
    assertFormat("12345-12-31T23:59:59.37",  327434918399370L);
    assertFormat("12345-12-31T23:59:59.9",   327434918399900L);

    // well before epoch
    assertFormat("0299-12-31T23:59:59",     -52700112001000L);
    assertFormat("0299-12-31T23:59:59.123", -52700112000877L);
    assertFormat("0299-12-31T23:59:59.09",  -52700112000910L);

  }

  /** 
   * Using dates in the canonical format, verify that parsing+formating 
   * is an identify function
   */
  public void testRoundTrip() throws Exception {

    // typical dates, various precision
    assertRoundTrip("1995-12-31T23:59:59.987Z");
    assertRoundTrip("1995-12-31T23:59:59.98Z");
    assertRoundTrip("1995-12-31T23:59:59.9Z");
    assertRoundTrip("1995-12-31T23:59:59Z");
    assertRoundTrip("1976-03-06T03:06:00Z");

    // dates with atypical years
    assertRoundTrip("0001-01-01T01:01:01Z");
    assertRoundTrip("12021-12-01T03:03:03Z");
  }

  @Ignore("SOLR-2773: Non-Positive years don't work")
  public void testRoundTripNonPositiveYear() throws Exception {

    // :TODO: ambiguity about year zero
    // assertRoundTrip("0000-04-04T04:04:04Z");
    
    // dates with negative years
    assertRoundTrip("-0005-05-05T05:05:05Z");
    assertRoundTrip("-2021-12-01T04:04:04Z");
    assertRoundTrip("-12021-12-01T02:02:02Z");
    
    // :TODO: assertFormat and assertToObject some negative years

  }

  protected void assertFormat(final String expected, final long millis) {
    assertEquals(expected, f.formatDate(new Date(millis)));
  }

  protected void assertRoundTrip(String canonicalDate) throws Exception {
    Date d = DateField.parseDate(canonicalDate);
    String result = DateField.formatExternal(d);
    assertEquals("d:" + d.getTime(), canonicalDate, result);

  }


  public void testCreateField() {
    int props = FieldProperties.INDEXED ^ FieldProperties.STORED;
    SchemaField sf = new SchemaField( "test", f, props, null );
    IndexableField out = (Field)f.createField(sf, "1995-12-31T23:59:59Z", 1.0f );
    assertEquals(820454399000l, f.toObject( out ).getTime() );
    
    out = (Field)f.createField(sf, new Date(820454399000l), 1.0f );
    assertEquals(820454399000l, f.toObject( out ).getTime() );
  }
}
