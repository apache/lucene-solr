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
    assertToObject(820454399987l, "1995-12-31T23:59:59.987666Z");
    assertToObject(820454399987l, "1995-12-31T23:59:59.987Z");
    assertToObject(820454399980l, "1995-12-31T23:59:59.98Z");
    assertToObject(820454399900l, "1995-12-31T23:59:59.9Z");
    assertToObject(820454399000l, "1995-12-31T23:59:59Z");
  }
  
  public void testFormatter() {
    assertEquals("1970-01-01T00:00:00.005", f.formatDate(new Date(5)));
    assertEquals("1970-01-01T00:00:00",     f.formatDate(new Date(0)));
    assertEquals("1970-01-01T00:00:00.37",  f.formatDate(new Date(370)));
    assertEquals("1970-01-01T00:00:00.9",   f.formatDate(new Date(900)));

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
