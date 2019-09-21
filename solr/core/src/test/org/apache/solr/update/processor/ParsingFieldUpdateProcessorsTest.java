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
package org.apache.solr.update.processor;

import java.io.IOException;
import java.text.DecimalFormatSymbols;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.hamcrest.core.IsInstanceOf;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.IndexSchema;
import org.junit.BeforeClass;
/**
 * Tests for the field mutating update processors
 * that parse Dates, Longs, Doubles, and Booleans.
 */
public class ParsingFieldUpdateProcessorsTest extends UpdateProcessorTestBase {
  private static final double EPSILON = 1E-15;
  private static final DateTimeFormatter isoDateOptionalTimeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd['T'HH:mm[:ss[.SSS]]][z", Locale.ROOT).withZone(ZoneOffset.UTC);

  private static final IsInstanceOf IS_BOOLEAN = new IsInstanceOf(Boolean.class);
  private static final IsInstanceOf IS_STRING = new IsInstanceOf(String.class);
  private static final IsInstanceOf IS_DATE = new IsInstanceOf(Date.class);
  private static final IsInstanceOf IS_FLOAT = new IsInstanceOf(Float.class);
  private static final IsInstanceOf IS_DOUBLE = new IsInstanceOf(Double.class);
  private static final IsInstanceOf IS_INTEGER = new IsInstanceOf(Integer.class);
  private static final IsInstanceOf IS_LONG = new IsInstanceOf(Long.class);
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-parsing-update-processor-chains.xml", "schema12.xml");
  }

  public void testParseDateRoundTrip() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("date_dt")); // should match "*_dt" dynamic field
    String dateString = "2010-11-12T13:14:15.168Z";
    SolrInputDocument d = processAdd("parse-date", doc(f("id", "9"), f("date_dt", dateString)));
    assertNotNull(d);
    assertThat(d.getFieldValue("date_dt"), IS_DATE);
    assertEquals(Instant.parse(dateString), ((Date) d.getFieldValue("date_dt")).toInstant());
    assertU(commit());
    assertQ(req("id:9"), "//date[@name='date_dt'][.='" + dateString + "']");
  }

  public void testParseTrieDateRoundTrip() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("date_tdt")); // should match "*_tdt" dynamic field
    String dateString = "2010-11-12T13:14:15.168Z";
    SolrInputDocument d = processAdd("parse-date", doc(f("id", "39"), f("date_tdt", dateString)));
    assertNotNull(d);
    assertThat(d.getFieldValue("date_tdt"), IS_DATE);
    assertEquals(Instant.parse(dateString), ((Date) d.getFieldValue("date_tdt")).toInstant());
    assertU(commit());
    assertQ(req("id:39"), "//date[@name='date_tdt'][.='" + dateString + "']");
  }


  public void testParseDateFieldNotInSchema() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNull(schema.getFieldOrNull("not_in_schema"));
    String dateString = "2010-11-12T13:14:15.168Z";

    SolrInputDocument d = processAdd("parse-date-no-run-processor",
                                     doc(f("id", "18"), f("not_in_schema", dateString)));
    assertNotNull(d);
    assertThat(d.getFieldValue("not_in_schema"), IS_DATE);
    assertEquals(Instant.parse(dateString), ((Date)d.getFieldValue("not_in_schema")).toInstant());
    
    d = processAdd("parse-date-no-run-processor", 
                   doc(f("id", "36"), f("not_in_schema", "not a date", dateString)));
    assertNotNull(d);
    for (Object val : d.getFieldValues("not_in_schema")) {
      // check that nothing was mutated, since not all field values are parseable as dates 
      assertThat(val, IS_STRING);
    }

    d = processAdd("parse-date-no-run-processor",
        doc(f("id", "72"), f("not_in_schema", dateString, "not a date")));
    assertNotNull(d);
    for (Object val : d.getFieldValues("not_in_schema")) {
      // check again that nothing was mutated, but with a valid date first this time 
      assertThat(val, IS_STRING);
    }
  }
  
  public void testParseDateNonUTCdefaultTimeZoneRoundTrip() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("date_dt")); // should match "*_dt" dynamic field
    String dateStringNoTimeZone         = "2010-11-12T13:14:15.168";
    String dateStringUTC = dateStringNoTimeZone + "Z";

    // dateStringNoTimeZone interpreted as being in timeZone America/New_York, then printed as UTC
    String dateStringUSEasternTimeAsUTC = "2010-11-12T18:14:15.168Z";
    
    SolrInputDocument d = processAdd
        ("parse-date-non-UTC-defaultTimeZone", doc(f("id", "99"), f("dateUTC_dt", dateStringUTC), 
                                                   f("dateNoTimeZone_dt", dateStringNoTimeZone)));
    assertNotNull(d);
    assertThat(d.getFieldValue("dateUTC_dt"), IS_DATE);
    assertThat(d.getFieldValue("dateNoTimeZone_dt"), IS_DATE);
    assertU(commit());
    assertQ(req("id:99") 
        ,"//date[@name='dateUTC_dt'][.='" + dateStringUTC + "']"
        ,"//date[@name='dateNoTimeZone_dt'][.='" + dateStringUSEasternTimeAsUTC + "']");
  }
  
  public void testParseDateExplicitNotInSchemaSelector() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNull(schema.getFieldOrNull("not_in_schema"));
    String dateString = "2010-11-12T13:14:15.168Z";

    SolrInputDocument d = processAdd("parse-date-explicit-not-in-schema-selector-no-run-processor",
                                     doc(f("id", "88"), f("not_in_schema", dateString)));
    assertNotNull(d);
    assertThat(d.getFieldValue("not_in_schema"), IS_DATE);
    assertEquals(Instant.parse(dateString), ((Date)d.getFieldValue("not_in_schema")).toInstant());
  }

  public void testParseDateExplicitTypeClassSelector() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("date_dt"));
    String dateString = "2010-11-12T13:14:15.168Z";
    SolrInputDocument d;
    if (schema.getField("date_dt").getType().isPointField()) {
      d = processAdd("parse-date-explicit-typeclass-point-selector-no-run-processor",
          doc(f("id", "77"), f("date_dt", dateString)));
    } else {
      d = processAdd("parse-date-explicit-typeclass-selector-no-run-processor",
          doc(f("id", "77"), f("date_dt", dateString)));
    }

    assertNotNull(d);
    assertThat(d.getFieldValue("date_dt"), IS_DATE);
    assertEquals(Instant.parse(dateString), ((Date)d.getFieldValue("date_dt")).toInstant());
  }

  public void testParseUSPacificDate() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNull(schema.getFieldOrNull("not_in_schema"));
    String dateString = "8/9/2010";  // Interpreted as 00:00 US Pacific Daylight Time = UTC+07:00
    SolrInputDocument d = processAdd("US-Pacific-parse-date-no-run-processor",
                                     doc(f("id", "288"), f("not_in_schema", dateString)));
    assertNotNull(d);
    assertThat(d.getFieldValue("not_in_schema"), IS_DATE);
    assertEquals(Instant.parse("2010-08-09T07:00:00.000Z"), ((Date)d.getFieldValue("not_in_schema")).toInstant());
  }
  
  public void testParseDateManyFormats() throws Exception {
    String[] formatExamples = { 
        "2010-01-15T00:00:00.000Z",
        "2010-01-15T00:00:00,000Z",
        "2010-01-15T00:00:00.000",
        "2010-01-15T00:00:00,000",
        "2010-01-15T00:00:00Z",
        "2010-01-15T00:00:00",
        "2010-01-15T00:00Z",
        "2010-01-15T00:00",
        "2010-01-15 00:00:00.000Z",
        "2010-01-15 00:00:00,000Z",
        "2010-01-15 00:00:00.000",
        "2010-01-15 00:00:00,000",
        "2010-01-15 00:00:00Z",
        "2010-01-15 00:00:00",
        "2010-01-15 00:00Z",
        "2010-01-15 00:00",
        "2010-01-15 12:00 AM",
        "2010-01-15 12:00AM",
        "2010-01-15",
        "Fri Jan 15 00:00:00 +0000 2010",
        "Fri Jan 15 00:00:00 2010 +00:00",
        "Fri Jan 15 00:00:00 2010",
        "Fri, 15 Jan 2010 00:00:00 +00:00",
        "Friday, 15-Jan-10 00:00:00 +00:00",
        "Friday, January 15, 2010",
        "January 15, 2010",
        "Jan. 15, 2010"
    };

    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("dateUTC_dt")); // should match "*_dt" dynamic field

    Instant expectedInstant = Instant.parse(formatExamples[0]);

    for (int i = 0 ; i < formatExamples.length ; ++i) {
      String dateString = formatExamples[i];
      String id = "95" + i;
      SolrInputDocument d = processAdd("parse-date-many-formats-no-run-processor",
                                       doc(f("id", id), f("dateUTC_dt", dateString)));
      assertNotNull(d);
      assertThat("index: " + i + " date '" + dateString + "' is not mutated to a Date",
          d.getFieldValue("dateUTC_dt"), IS_DATE);
      assertEquals("date '" + dateString + "' mismatched milliseconds",
          expectedInstant, ((Date)d.getFieldValue("dateUTC_dt")).toInstant());
    }
  }
  
  public void testParseFrenchDate() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNull(schema.getFieldOrNull("not_in_schema"));
    String frenchDateString = "le vendredi 15 janvier 2010";
    String dateString = "2010-01-15T00:00:00.000Z";
    SolrInputDocument d = processAdd("parse-french-date-UTC-defaultTimeZone-no-run-processor",
                                     doc(f("id", "88"), f("not_in_schema", frenchDateString)));
    assertNotNull(d);
    assertThat(d.getFieldValue("not_in_schema"), IS_DATE);
    assertEquals(Instant.parse(dateString), ((Date)d.getFieldValue("not_in_schema")).toInstant());
  }
  
  public void testFailedParseMixedDate() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNull(schema.getFieldOrNull("not_in_schema"));
    Map<Object,Object> mixed = new HashMap<>();
    String[] dateStrings = { "2020-05-13T18:47", "1989-12-14", "1682-07-22T18:33:00.000Z" };
    for (String dateString : dateStrings) {
      mixed.put(parse(isoDateOptionalTimeFormatter, dateString), dateString);
    }
    Double extraDouble = 29.554d;
    mixed.put(extraDouble, extraDouble); // Double-typed field value
    SolrInputDocument d = processAdd("parse-date-no-run-processor", 
                                     doc(f("id", "7201"), f("not_in_schema", mixed.values())));
    assertNotNull(d);
    boolean foundDouble = false;
    for (Object o : d.getFieldValues("not_in_schema")) {
      if (extraDouble == o) {
        foundDouble = true;
      } else {
        assertThat(o, IS_STRING);
      }
      mixed.values().remove(o);
    }
    assertTrue(foundDouble);
    assertTrue(mixed.isEmpty());
  }

  public void testParseIntRoundTrip() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("int1_i")); // should match dynamic field "*_i"
    assertNotNull(schema.getFieldOrNull("int2_i")); // should match dynamic field "*_i"
    int value = 1089883491;
    String intString1 = "1089883491";
    String intString2 = "1,089,883,491";
    SolrInputDocument d = processAdd("parse-int",
        doc(f("id", "113"), f("int1_i", intString1), f("int2_i", intString2)));
    assertNotNull(d);
    assertThat(d.getFieldValue("int1_i"), IS_INTEGER);
    assertEquals(value, ((Integer)d.getFieldValue("int1_i")).intValue());
    assertThat(d.getFieldValue("int2_i"), IS_INTEGER);
    assertEquals(value, ((Integer)d.getFieldValue("int2_i")).intValue());

    assertU(commit());
    assertQ(req("id:113")
        ,"//int[@name='int1_i'][.='" + value + "']"
        ,"//int[@name='int2_i'][.='" + value + "']");
  }

  public void testParseIntNonRootLocale() throws Exception {
    final DecimalFormatSymbols ru_RU = DecimalFormatSymbols.getInstance(new Locale("ru","RU"));
    final char groupChar = ru_RU.getGroupingSeparator();
    
    int value = 1089883491;
    String intString1 = "1089883491";
    String intString2 = "1"+groupChar+"089"+groupChar+"883"+groupChar+"491";
    
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("int_i")); // should match dynamic field "*_i"
    assertNull(schema.getFieldOrNull("not_in_schema"));
    SolrInputDocument d = processAdd("parse-int-russian-no-run-processor",
        doc(f("id", "113"), f("int_i", intString1), f("not_in_schema", intString2)));
    assertNotNull(d);
    assertThat(d.getFieldValue("int_i"), IS_INTEGER);
    assertEquals(value, ((Integer)d.getFieldValue("int_i")).intValue());
    assertThat(d.getFieldValue("not_in_schema"), IS_INTEGER);
    assertEquals(value, ((Integer)d.getFieldValue("not_in_schema")).intValue());
  }

  public void testParseTrieIntRoundTrip() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("int1_ti")); // should match dynamic field "*_ti"
    assertNotNull(schema.getFieldOrNull("int2_ti")); // should match dynamic field "*_ti"
    int value = 1089883491;
    String intString1 = "1089883491";
    String intString2 = "1,089,883,491";
    SolrInputDocument d = processAdd("parse-int",
        doc(f("id", "113"), f("int1_ti", intString1), f("int2_ti", intString2)));
    assertNotNull(d);
    assertThat(d.getFieldValue("int1_ti"), IS_INTEGER);
    assertEquals(value, ((Integer)d.getFieldValue("int1_ti")).intValue());
    assertThat(d.getFieldValue("int2_ti"), IS_INTEGER);
    assertEquals(value, ((Integer)d.getFieldValue("int2_ti")).intValue());

    assertU(commit());
    assertQ(req("id:113")
        ,"//int[@name='int1_ti'][.='" + value + "']"
        ,"//int[@name='int2_ti'][.='" + value + "']");
  }

  public void testIntOverflow() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNull(schema.getFieldOrNull("not_in_schema1"));
    assertNull(schema.getFieldOrNull("not_in_schema2"));
    long longValue1 = (long)Integer.MAX_VALUE + 100L;
    long longValue2 = (long)Integer.MIN_VALUE - 100L;
    String longString1 = Long.toString(longValue1);
    String longString2 = Long.toString(longValue2);
    SolrInputDocument d = processAdd("parse-int-no-run-processor",
        doc(f("id", "282"), f("not_in_schema1", longString1), f("not_in_schema2", longString2)));
    assertNotNull(d);
    assertThat(d.getFieldValue("not_in_schema1"), IS_STRING);
    assertThat(d.getFieldValue("not_in_schema2"), IS_STRING);
  }
  
  public void testFailedParseMixedInt() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNull(schema.getFieldOrNull("not_in_schema"));
    Map<Object,Object> mixed = new HashMap<>();
    Float floatVal = 294423.0f;
    mixed.put(85, "85");
    mixed.put(floatVal, floatVal); // Float-typed field value
    mixed.put(-2894518, "-2,894,518");
    mixed.put(1879472193, "1,879,472,193");
    SolrInputDocument d = processAdd("parse-int-no-run-processor",
                                     doc(f("id", "7202"), f("not_in_schema", mixed.values())));
    assertNotNull(d);
    boolean foundFloat = false;
    for (Object o : d.getFieldValues("not_in_schema")) {
      if (floatVal == o) {
        foundFloat = true;
      } else {
        assertThat(o, IS_STRING);
      }
      mixed.values().remove(o);
    }
    assertTrue(foundFloat);
    assertTrue(mixed.isEmpty());
  }

  public void testParseLongRoundTrip() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("long1_l")); // should match dynamic field "*_l"
    assertNotNull(schema.getFieldOrNull("long2_l")); // should match dynamic field "*_l"
    long value = 1089883491L;
    String longString1 = "1089883491";
    String longString2 = "1,089,883,491";
    SolrInputDocument d = processAdd("parse-long", 
                                     doc(f("id", "113"), f("long1_l", longString1), f("long2_l", longString2)));
    assertNotNull(d);
    assertThat(d.getFieldValue("long1_l"), IS_LONG);
    assertEquals(value, ((Long) d.getFieldValue("long1_l")).longValue());
    assertThat(d.getFieldValue("long2_l"), IS_LONG);
    assertEquals(value, ((Long)d.getFieldValue("long2_l")).longValue());
    
    assertU(commit());
    assertQ(req("id:113")
        ,"//long[@name='long1_l'][.='" + value + "']"
        ,"//long[@name='long2_l'][.='" + value + "']");
  }

  public void testParseLongNonRootLocale() throws Exception {
    final DecimalFormatSymbols ru_RU = DecimalFormatSymbols.getInstance(new Locale("ru","RU"));
    final char groupChar = ru_RU.getGroupingSeparator();
    
    long value = 1089883491L;
    String longString1 = "1089883491";
    String longString2 = "1"+groupChar+"089"+groupChar+"883"+groupChar+"491";
    
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("long_l")); // should match dynamic field "*_l"
    assertNull(schema.getFieldOrNull("not_in_schema"));
    SolrInputDocument d = processAdd("parse-long-russian-no-run-processor",
                                     doc(f("id", "113"), f("long_l", longString1), f("not_in_schema", longString2)));
    assertNotNull(d);
    assertThat(d.getFieldValue("long_l"), IS_LONG);
    assertEquals(value, ((Long)d.getFieldValue("long_l")).longValue());
    assertThat(d.getFieldValue("not_in_schema"), IS_LONG);
    assertEquals(value, ((Long)d.getFieldValue("not_in_schema")).longValue());
  }

  public void testParseTrieLongRoundTrip() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("long1_tl")); // should match dynamic field "*_tl"
    assertNotNull(schema.getFieldOrNull("long2_tl")); // should match dynamic field "*_tl"
    long value = 1089883491L;
    String longString1 = "1089883491";
    String longString2 = "1,089,883,491";
    SolrInputDocument d = processAdd("parse-long",
        doc(f("id", "113"), f("long1_tl", longString1), f("long2_tl", longString2)));
    assertNotNull(d);
    assertThat(d.getFieldValue("long1_tl"), IS_LONG);
    assertEquals(value, ((Long)d.getFieldValue("long1_tl")).longValue());
    assertThat(d.getFieldValue("long2_tl"), IS_LONG);
    assertEquals(value, ((Long)d.getFieldValue("long2_tl")).longValue());

    assertU(commit());
    assertQ(req("id:113")
        ,"//long[@name='long1_tl'][.='" + value + "']"
        ,"//long[@name='long2_tl'][.='" + value + "']");
  }

  public void testFailedParseMixedLong() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNull(schema.getFieldOrNull("not_in_schema"));
    Map<Object,Object> mixed = new HashMap<>();
    Float floatVal = 294423.0f;
    mixed.put(85L, "85");
    mixed.put(floatVal, floatVal); // Float-typed field value
    mixed.put(-2894518L, "-2,894,518");
    mixed.put(1879472193L, "1,879,472,193");
    SolrInputDocument d = processAdd("parse-long-no-run-processor",
                                     doc(f("id", "7204"), f("not_in_schema", mixed.values())));
    assertNotNull(d);
    boolean foundFloat = false;
    for (Object o : d.getFieldValues("not_in_schema")) {
      if (floatVal == o) {
        foundFloat = true;
      } else {
        assertThat(o, IS_STRING);
      }
      mixed.values().remove(o);
    }
    assertTrue(foundFloat);
    assertTrue(mixed.isEmpty());
  }

  public void testParseFloatRoundTrip() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("float1_f")); // should match dynamic field "*_f"
    assertNotNull(schema.getFieldOrNull("float2_f")); // should match dynamic field "*_f"
    float value = 10898.83491f;
    String floatString1 = "10898.83491";
    String floatString2 = "10,898.83491";
    SolrInputDocument d = processAdd("parse-float",
        doc(f("id", "128"), f("float1_f", floatString1), f("float2_f", floatString2)));
    assertNotNull(d);
    assertThat(d.getFieldValue("float1_f"), IS_FLOAT);
    assertEquals(value, (Float)d.getFieldValue("float1_f"), EPSILON);
    assertThat(d.getFieldValue("float2_f"), IS_FLOAT);
    assertEquals(value, (Float)d.getFieldValue("float2_f"), EPSILON);

    assertU(commit());
    assertQ(req("id:128")
        ,"//float[@name='float1_f'][.='" + value + "']"
        ,"//float[@name='float2_f'][.='" + value + "']");
  }

  public void testParseFloatNonRootLocale() throws Exception {
    final DecimalFormatSymbols fr_FR = DecimalFormatSymbols.getInstance(new Locale("fr","FR"));
    final char groupChar = fr_FR.getGroupingSeparator();
    final char decimalChar = fr_FR.getDecimalSeparator();

    float value = 10898.83491F;
    String floatString1 = "10898"+decimalChar+"83491";
    String floatString2 = "10"+groupChar+"898"+decimalChar+"83491";
    
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("float_f")); // should match dynamic field "*_f"
    assertNull(schema.getFieldOrNull("not_in_schema"));
    SolrInputDocument d = processAdd("parse-float-french-no-run-processor",
        doc(f("id", "140"), f("float_f", floatString1),
            f("not_in_schema", floatString2)));
    assertNotNull(d);
    assertThat(d.getFieldValue("float_f"), IS_FLOAT);
    assertEquals(value, (Float)d.getFieldValue("float_f"), EPSILON);
    assertThat(d.getFieldValue("not_in_schema"), IS_FLOAT);
    assertEquals(value, (Float)d.getFieldValue("not_in_schema"), EPSILON);
  }

  public void testParseTrieFloatRoundTrip() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("float1_tf")); // should match dynamic field "*_tf"
    assertNotNull(schema.getFieldOrNull("float2_tf")); // should match dynamic field "*_tf"
    float value = 10898.83491f;
    String floatString1 = "10898.83491";
    String floatString2 = "10,898.83491";
    SolrInputDocument d = processAdd("parse-float",
        doc(f("id", "728"), f("float1_tf", floatString1), f("float2_tf", floatString2)));
    assertNotNull(d);
    assertThat(d.getFieldValue("float1_tf"), IS_FLOAT);
    assertEquals(value, (Float)d.getFieldValue("float1_tf"), EPSILON);
    assertThat(d.getFieldValue("float2_tf"), IS_FLOAT);
    assertEquals(value, (Float)d.getFieldValue("float2_tf"), EPSILON);

    assertU(commit());
    assertQ(req("id:728")
        ,"//float[@name='float1_tf'][.='" + value + "']"
        ,"//float[@name='float2_tf'][.='" + value + "']");
  }
  
  public void testMixedFloats() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("float_tf")); // should match dynamic field "*_tf"
    Map<Float,Object> mixedFloats = new HashMap<>();
    mixedFloats.put(85.0f, "85");
    mixedFloats.put(2894518.0f, "2,894,518");
    mixedFloats.put(2.94423E-9f, 2.94423E-9f); // Float-typed field value
    mixedFloats.put(48794721.937f, "48,794,721.937");
    SolrInputDocument d = processAdd("parse-float-no-run-processor", 
                                     doc(f("id", "342"), f("float_tf", mixedFloats.values())));
    assertNotNull(d);
    for (Object o : d.getFieldValues("float_tf")) {
      assertThat(o, IS_FLOAT);
      mixedFloats.remove(o);
    }
    assertTrue(mixedFloats.isEmpty());
  }

  public void testFailedParseMixedFloat() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNull(schema.getFieldOrNull("not_in_schema"));
    Map<Object,Object> mixed = new HashMap<>();
    Long longVal = 294423L;
    mixed.put(85L, "85");
    mixed.put(longVal, longVal); // Float-typed field value
    mixed.put(-2894518L, "-2,894,518");
    mixed.put(1879472193L, "1,879,472,193");
    SolrInputDocument d = processAdd("parse-float-no-run-processor",
                                     doc(f("id", "7205"), f("not_in_schema", mixed.values())));
    assertNotNull(d);
    boolean foundLong = false;
    for (Object o : d.getFieldValues("not_in_schema")) {
      if (longVal == o) {
        foundLong = true;
      } else {
        assertThat(o, IS_STRING);
      }
      mixed.values().remove(o);
    }
    assertTrue(foundLong);
    assertTrue(mixed.isEmpty());
  }

  public void testParseDoubleRoundTrip() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("double1_d")); // should match dynamic field "*_d"
    assertNotNull(schema.getFieldOrNull("double2_d")); // should match dynamic field "*_d"
    double value = 10898.83491;
    String doubleString1 = "10898.83491";
    String doubleString2 = "10,898.83491";
    SolrInputDocument d = processAdd("parse-double",
        doc(f("id", "128"), f("double1_d", doubleString1), f("double2_d", doubleString2)));
    assertNotNull(d);
    assertThat(d.getFieldValue("double1_d"), IS_DOUBLE);
    assertEquals(value, (Double)d.getFieldValue("double1_d"), EPSILON);
    assertThat(d.getFieldValue("double2_d"), IS_DOUBLE);
    assertEquals(value, (Double)d.getFieldValue("double2_d"), EPSILON);

    assertU(commit());
    assertQ(req("id:128")
        ,"//double[@name='double1_d'][.='" + value + "']"
        ,"//double[@name='double2_d'][.='" + value + "']");
  }

  public void testParseDoubleNonRootLocale() throws Exception {
    final DecimalFormatSymbols fr_FR = DecimalFormatSymbols.getInstance(new Locale("fr","FR"));
    final char groupChar = fr_FR.getGroupingSeparator();
    final char decimalChar = fr_FR.getDecimalSeparator();

    double value = 10898.83491D;
    String doubleString1 = "10898"+decimalChar+"83491";
    String doubleString2 = "10"+groupChar+"898"+decimalChar+"83491";
    
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("double_d")); // should match dynamic field "*_d"
    assertNull(schema.getFieldOrNull("not_in_schema"));
    SolrInputDocument d = processAdd("parse-double-french-no-run-processor",
                                     doc(f("id", "140"), f("double_d", doubleString1), 
                                         f("not_in_schema", doubleString2)));
    assertNotNull(d);
    assertThat(d.getFieldValue("double_d"), IS_DOUBLE);
    assertEquals(value, (Double)d.getFieldValue("double_d"), EPSILON);
    assertThat(d.getFieldValue("not_in_schema"), IS_DOUBLE);
    assertEquals(value, (Double)d.getFieldValue("not_in_schema"), EPSILON);
  }

  public void testParseTrieDoubleRoundTrip() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("double1_td")); // should match dynamic field "*_td"
    assertNotNull(schema.getFieldOrNull("double2_td")); // should match dynamic field "*_td"
    double value = 10898.83491;
    String doubleString1 = "10898.83491";
    String doubleString2 = "10,898.83491";
    SolrInputDocument d = processAdd("parse-double",
        doc(f("id", "728"), f("double1_td", doubleString1), f("double2_td", doubleString2)));
    assertNotNull(d);
    assertThat(d.getFieldValue("double1_td"), IS_DOUBLE);
    assertEquals(value, (Double)d.getFieldValue("double1_td"), EPSILON);
    assertThat(d.getFieldValue("double2_td"), IS_DOUBLE);
    assertEquals(value, (Double)d.getFieldValue("double2_td"), EPSILON);

    assertU(commit());
    assertQ(req("id:728")
        ,"//double[@name='double1_td'][.='" + value + "']"
        ,"//double[@name='double2_td'][.='" + value + "']");
  }

  public void testFailedParseMixedDouble() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNull(schema.getFieldOrNull("not_in_schema"));
    Map<Object,Object> mixed = new HashMap<>();
    Long longVal = 294423L;
    mixed.put(85, "85.0");
    mixed.put(longVal, longVal); // Float-typed field value
    mixed.put(-2894.518, "-2,894.518");
    mixed.put(187947.2193, "187,947.2193");
    SolrInputDocument d = processAdd("parse-double-no-run-processor",
                                     doc(f("id", "7206"), f("not_in_schema", mixed.values())));
    assertNotNull(d);
    boolean foundLong = false;
    for (Object o : d.getFieldValues("not_in_schema")) {
      if (longVal == o) {
        foundLong = true;
      } else {
        assertThat(o, IS_STRING);
      }
      mixed.values().remove(o);
    }
    assertTrue(foundLong);
    assertTrue(mixed.isEmpty());
  }

  public void testParseBooleanRoundTrip() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("boolean1_b")); // should match dynamic field "*_b"
    assertNotNull(schema.getFieldOrNull("boolean2_b")); // should match dynamic field "*_b"
    boolean value1 = true;
    boolean value2 = false;
    SolrInputDocument d = processAdd("parse-boolean",
        doc(f("id", "141"), f("boolean1_b", value1), f("boolean2_b", value2)));
    assertNotNull(d);
    assertThat(d.getFieldValue("boolean1_b"), IS_BOOLEAN);
    assertEquals(value1, d.getFieldValue("boolean1_b"));
    assertThat(d.getFieldValue("boolean2_b"), IS_BOOLEAN);
    assertEquals(value2, d.getFieldValue("boolean2_b"));

    assertU(commit());
    assertQ(req("id:141")
        ,"//bool[@name='boolean1_b'][.='" + value1 + "']"
        ,"//bool[@name='boolean2_b'][.='" + value2 + "']");
  }
  
  public void testParseAlternateValueBooleans() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("boolean1_b")); // should match dynamic field "*_b"
    assertNotNull(schema.getFieldOrNull("boolean2_b")); // should match dynamic field "*_b"
    assertNotNull(schema.getFieldOrNull("boolean3_b")); // should match dynamic field "*_b"
    assertNotNull(schema.getFieldOrNull("boolean4_b")); // should match dynamic field "*_b"
    assertNotNull(schema.getFieldOrNull("boolean5_b")); // should match dynamic field "*_b"
    assertNull(schema.getFieldOrNull("not_in_schema"));
    boolean[] values      = { true, true, true, false, false, false };
    String[] stringValues = { "on", "yes", "True", "Off", "no", "FALSE" };
    String[] fieldNames   = { "boolean1_b", "boolean2_b", "boolean3_b", "boolean4_b", "boolean5_b", "not_in_schema" };
    SolrInputDocument d = doc(f("id", "55"));
    for (int i = 0 ; i < values.length ; ++i) {
      d.addField(fieldNames[i], stringValues[i]);
    }
    d = processAdd("parse-boolean-alternate-values-no-run-processor", d);
    assertNotNull(d);

    for (int i = 0 ; i < values.length ; ++i) {
      assertThat(d.getFieldValue(fieldNames[i]), IS_BOOLEAN);
      assertEquals(values[i], d.getFieldValue(fieldNames[i]));
    }
  }

  public void testParseAlternateSingleValuesBooleans() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("boolean1_b")); // should match dynamic field "*_b"
    assertNotNull(schema.getFieldOrNull("boolean2_b")); // should match dynamic field "*_b"
    boolean[] values      = { true, false };
    String[] stringValues = { "yup", "nope" };
    String[] fieldNames   = { "boolean1_b", "boolean2_b" };
    SolrInputDocument d = doc(f("id", "59"));
    for (int i = 0 ; i < values.length ; ++i) {
      d.addField(fieldNames[i], stringValues[i]);
    }
    d = processAdd("parse-boolean-alternate-single-values-no-run-processor", d);
    assertNotNull(d);

    for (int i = 0 ; i < values.length ; ++i) {
      assertThat(d.getFieldValue(fieldNames[i]), IS_BOOLEAN);
      assertEquals(values[i], d.getFieldValue(fieldNames[i]));
    }

    // Standard boolean values should not be mutated, since they're not configured
    stringValues = new String[] { "true", "false" };
    d = doc(f("id", "593"));
    for (int i = 0 ; i < values.length ; ++i) {
      d.addField(fieldNames[i], stringValues[i]);
    }
    d = processAdd("parse-boolean-alternate-single-values-no-run-processor", d);
    assertNotNull(d);

    for (int i = 0 ; i < values.length ; ++i) {
      assertThat(d.getFieldValue(fieldNames[i]), IS_STRING);
    }
  }

  public void testFailedParseMixedBoolean() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNull(schema.getFieldOrNull("not_in_schema"));
    Map<Object,Object> mixed = new HashMap<>();
    Long longVal = 294423L;
    mixed.put(true, "true");
    mixed.put(longVal, longVal); // Float-typed field value
    mixed.put(false, "false");
    mixed.put(true, "true");
    SolrInputDocument d = processAdd("parse-boolean-no-run-processor",
                                     doc(f("id", "7207"), f("not_in_schema", mixed.values())));
    assertNotNull(d);
    boolean foundLong = false;
    for (Object o : d.getFieldValues("not_in_schema")) {
      if (longVal == o) {
        foundLong = true;
      } else {
        assertThat(o, IS_STRING);
      }
      mixed.values().remove(o);
    }
    assertTrue(foundLong);
    assertTrue(mixed.isEmpty());
  }

  public void testCascadingParsers() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    final String fieldName = "not_in_schema";
    assertNull(schema.getFieldOrNull(fieldName));
    SolrInputDocument d = null;
    String chain = "cascading-parsers-no-run-processor";
    
    Map<Boolean,String> booleans = new HashMap<>();
    booleans.put(true, "truE");
    booleans.put(false, "False");
    d = processAdd(chain, doc(f("id", "341"), f(fieldName, booleans.values())));
    assertNotNull(d);
    for (Object o : d.getFieldValues(fieldName)) {
      assertThat(o, IS_BOOLEAN);
      booleans.remove(o);
    }
    assertTrue(booleans.isEmpty());

    Map<Integer,String> ints = new HashMap<>();
    ints.put(2, "2");
    ints.put(50928, "50928");
    ints.put(86942008, "86,942,008");
    d = processAdd(chain, doc(f("id", "333"), f(fieldName, ints.values())));
    assertNotNull(d);
    for (Object o : d.getFieldValues(fieldName)) {
      assertThat(o, IS_INTEGER);
      ints.remove(o);
    }
    assertTrue(ints.isEmpty());

    Map<Long,String> longs = new HashMap<>();
    longs.put(2L, "2");
    longs.put(50928L, "50928");
    longs.put(86942008987654L, "86,942,008,987,654");
    d = processAdd(chain, doc(f("id", "342"), f(fieldName, longs.values())));
    assertNotNull(d);
    for (Object o : d.getFieldValues(fieldName)) {
      assertThat(o, IS_LONG);
      longs.remove(o);
    }
    assertTrue(longs.isEmpty());
    
    /*
    // Disabling this test because unlike Integer/Long, Float parsing can perform
    // rounding to make values fit.  See 
    Map<Float,String> floats = new HashMap<Float,String>();
    floats.put(2.0, "2.");
    floats.put(509.28, "509.28");
    floats.put(86942.008, "86,942.008");
    d = processAdd(chain, doc(f("id", "342"), f(fieldName, floats.values())));
    assertNotNull(d);
    for (Object o : d.getFieldValues(fieldName)) {
      assertTrue(o instanceof float);
      longs.remove(o);
    }
    */

    Map<Double,String> doubles = new HashMap<>();
    doubles.put(2.0, "2.");
    doubles.put(509.28, "509.28");
    doubles.put(86942.008, "86,942.008");
    d = processAdd(chain, doc(f("id", "342"), f(fieldName, doubles.values())));
    assertNotNull(d);
    for (Object o : d.getFieldValues(fieldName)) {
      assertThat(o, IS_DOUBLE);
      longs.remove(o);
    }

    Map<Date,String> dates = new HashMap<>();
    String[] dateStrings = { "2020-05-13T18:47", "1989-12-14", "1682-07-22T18:33:00.000Z" };
    for (String dateString : dateStrings) {
      dates.put(parse(isoDateOptionalTimeFormatter, dateString), dateString);
    }
    d = processAdd(chain, doc(f("id", "343"), f(fieldName, dates.values())));
    assertNotNull(d);
    for (Object o : d.getFieldValues(fieldName)) {
      assertThat(o, IS_DATE);
      dates.remove(o);
    }
    assertTrue(dates.isEmpty());
    
    Map<Double,String> mixedLongsAndDoubles = new LinkedHashMap<>(); // preserve order
    mixedLongsAndDoubles.put(85.0, "85");
    mixedLongsAndDoubles.put(2.94423E-9, "2.94423E-9");
    mixedLongsAndDoubles.put(2894518.0, "2,894,518");
    mixedLongsAndDoubles.put(48794721.937, "48,794,721.937");
    d = processAdd(chain, doc(f("id", "344"), f(fieldName, mixedLongsAndDoubles.values())));
    assertNotNull(d);
    for (Object o : d.getFieldValues(fieldName)) {
      assertThat(o, IS_DOUBLE);
      mixedLongsAndDoubles.remove(o);
    }
    assertTrue(mixedLongsAndDoubles.isEmpty());
    
    Set<String> mixed = new HashSet<>();
    mixed.add("true");
    mixed.add("1682-07-22T18:33:00.000Z");
    mixed.add("2,894,518");
    mixed.add("308,393,131,379,900");
    mixed.add("48,794,721.937");
    d = processAdd(chain, doc(f("id", "345"), f(fieldName, mixed)));
    assertNotNull(d);
    for (Object o : d.getFieldValues(fieldName)) {
      assertThat(o, IS_STRING);
    }

    Map<Double,Object> mixedDoubles = new LinkedHashMap<>(); // preserve order
    mixedDoubles.put(85.0, "85");
    mixedDoubles.put(2.94423E-9, 2.94423E-9); // Double-typed field value
    mixedDoubles.put(2894518.0, "2,894,518");
    mixedDoubles.put(48794721.937, "48,794,721.937");
    d = processAdd(chain, doc(f("id", "3391"), f(fieldName, mixedDoubles.values())));
    assertNotNull(d);
    for (Object o : d.getFieldValues(fieldName)) {
      assertThat(o, IS_DOUBLE);
      mixedDoubles.remove(o);
    }
    assertTrue(mixedDoubles.isEmpty());

    Map<Integer,Object> mixedInts = new LinkedHashMap<>(); // preserve order
    mixedInts.put(85, "85");
    mixedInts.put(294423, 294423); // Integer-typed field value
    mixedInts.put(-2894518, "-2,894,518");
    mixedInts.put(1879472193, "1,879,472,193");
    d = processAdd(chain, doc(f("id", "3392"), f(fieldName, mixedInts.values())));
    assertNotNull(d);
    for (Object o : d.getFieldValues(fieldName)) {
      assertThat(o, IS_INTEGER);
      mixedInts.remove(o);
    }
    assertTrue(mixedInts.isEmpty());

    Map<Long,Object> mixedLongs = new LinkedHashMap<>(); // preserve order
    mixedLongs.put(85L, "85");
    mixedLongs.put(42944233L, 42944233L); // Long-typed field value
    mixedLongs.put(2894518L, "2,894,518");
    mixedLongs.put(48794721937L, "48,794,721,937");
    d = processAdd(chain, doc(f("id", "3393"), f(fieldName, mixedLongs.values())));
    assertNotNull(d);
    for (Object o : d.getFieldValues(fieldName)) {
      assertThat(o, IS_LONG);
      mixedLongs.remove(o);
    }
    assertTrue(mixedLongs.isEmpty());

    Map<Boolean,Object> mixedBooleans = new LinkedHashMap<>(); // preserve order
    mixedBooleans.put(true, "true");
    mixedBooleans.put(false, false); // Boolean-typed field value
    mixedBooleans.put(false, "false");
    mixedBooleans.put(true, "true");
    d = processAdd(chain, doc(f("id", "3394"), f(fieldName, mixedBooleans.values())));
    assertNotNull(d);
    for (Object o : d.getFieldValues(fieldName)) {
      assertThat(o, IS_BOOLEAN);
      mixedBooleans.remove(o);
    }
    assertTrue(mixedBooleans.isEmpty());

    Map<Date,Object> mixedDates = new HashMap<>();
    dateStrings = new String[] { "2020-05-13T18:47", "1989-12-14", "1682-07-22T18:33:00.000Z" };
    for (String dateString : dateStrings) {
      mixedDates.put(parse(isoDateOptionalTimeFormatter, dateString), dateString);
    }
    Date extraDate = parse(isoDateOptionalTimeFormatter, "2003-04-24");
    mixedDates.put(extraDate, extraDate); // Date-typed field value
    d = processAdd(chain, doc(f("id", "3395"), f(fieldName, mixedDates.values())));
    assertNotNull(d);
    for (Object o : d.getFieldValues(fieldName)) {
      assertThat(o, IS_DATE);
      mixedDates.remove(o);
    }
    assertTrue(mixedDates.isEmpty());
  }

  // tests that mimic the tests that were in TestExtractionDateUtil
  public void testISO8601() throws IOException {
    // dates with atypical years
    // This test tries to mimic TestExtractionDateUtil#testISO8601

    String[] dateStrings = {
        "0001-01-01T01:01:01Z", "+12021-12-01T03:03:03Z",
        "0000-04-04T04:04:04Z", "-0005-05-05T05:05:05Z",
        "-2021-12-01T04:04:04Z", "-12021-12-01T02:02:02Z"
    };

    int id = 1;

    // ensure strings are parsed
    for(String notInFormatDateString: dateStrings) {
      IndexSchema schema = h.getCore().getLatestSchema();
      assertNotNull(schema.getFieldOrNull("date_dt")); // should match "*_dt" dynamic field
      SolrInputDocument d = processAdd("parse-date-patterns-default-config", doc(f("id", id), f("date_dt", notInFormatDateString)));
      assertNotNull(d);
      assertThat("Date string: " + notInFormatDateString + " was not parsed as a date", d.getFieldValue("date_dt"), IS_DATE);
      assertEquals(notInFormatDateString, ((Date) d.getField("date_dt").getFirstValue()).toInstant().toString());
      assertU(commit());
      assertQ(req("id:" + id), "//date[@name='date_dt'][.='" + notInFormatDateString + "']");
      ++id;
    }

    // odd values are date strings, even values are expected strings
    String[] lenientDateStrings = {
        "10995-12-31T23:59:59.990Z", "+10995-12-31T23:59:59.990Z",
        "995-1-2T3:4:5Z", "0995-01-02T03:04:05Z",
        "2021-01-01t03:04:05", "2021-01-01T03:04:05Z",
        "2021-12-01 04:04:04", "2021-12-01T04:04:04Z"
    };

    // ensure sure strings that should be parsed using lenient resolver are properly parsed
    for(int i = 0; i < lenientDateStrings.length; ++i) {
      String lenientDateString = lenientDateStrings[i];
      String expectedString = lenientDateStrings[++i];
      IndexSchema schema = h.getCore().getLatestSchema();
      assertNotNull(schema.getFieldOrNull("date_dt")); // should match "*_dt" dynamic field
      SolrInputDocument d = processAdd("parse-date-patterns-default-config", doc(f("id", id), f("date_dt", lenientDateString)));
      assertNotNull(d);
      assertThat("Date string: " + lenientDateString + " was not parsed as a date",
          d.getFieldValue("date_dt"), IS_DATE);
      assertEquals(expectedString, ((Date) d.getField("date_dt").getFirstValue()).toInstant().toString());
      ++id;
    }
  }

  // this test has had problems when the JDK timezone is Americas/Metlakatla
  public void testAKSTZone() throws IOException {
    final String dateFormat = "EEE MMM d HH:mm:ss z yyyy";
    final String inputString = "Thu Nov 13 04:35:51 AKST 2008"; // asctime + timezone1

    final long expectTs = 1226583351000L;
    assertEquals(expectTs,
        DateTimeFormatter.ofPattern(dateFormat, Locale.ENGLISH)
            .withZone(ZoneId.of("UTC")).parse(inputString, Instant::from).toEpochMilli());

    // ensure english locale and root locale return the same date
    assertEquals(expectTs, DateTimeFormatter.ofPattern(dateFormat, Locale.ENGLISH)
            .withZone(ZoneId.of("UTC")).parse(inputString, Instant::from).toEpochMilli(),
        DateTimeFormatter.ofPattern(dateFormat, Locale.ENGLISH)
            .withZone(ZoneId.of("UTC")).parse(inputString, Instant::from).toEpochMilli());

    assertParsedDate(inputString, Date.from(Instant.ofEpochMilli(expectTs)), "parse-date-patterns-default-config");

    // A bug in Java 9 (not in 8) causes this to fail!  (not fixed yet?!)
    // see https://bugs.openjdk.java.net/browse/JDK-8189784
    if (System.getProperty("java.version").startsWith("1.8.")) {
      // with daylight savings time timezone
      assertParsedDate("Fri Oct 7 05:14:15 AKDT 2005", Date.from(inst20051007131415()), "parse-date-patterns-default-config");
    } else {
      System.err.println("Didn't test AKDT because only Java 1.8 does this right!  This Java version is: " + System.getProperty("java.version"));
    }
  }

  public void testEDTZone() throws IOException {
    //EDT is GMT-4
    assertParsedDate("Fri Oct 7 09:14:15 EDT 2005", // asctime + timezone
        Date.from(inst20051007131415()), "parse-date-patterns-default-config");
  }

  public void testNoTime() throws IOException {
    Instant instant = instant(2005, 10, 7, 0, 0, 0);
    String inputString = "2005-10-07";
    assertParsedDate(inputString, Date.from(instant), "parse-date-patterns-default-config");
  }

  public void testRfc1123() throws IOException {
    assertParsedDate("Fri, 07 Oct 2005 13:14:15 GMT", Date.from(inst20051007131415()), "parse-date-patterns-default-config");
  }

  public void testRfc1036() throws IOException {
    assertParsedDate("Friday, 07-Oct-05 13:14:15 GMT", Date.from(inst20051007131415()), "parse-date-patterns-default-config");
  }

  public void testAsctime() throws Exception {
    assertParsedDate("Fri Oct 7 13:14:15 2005" , Date.from(inst20051007131415()), "parse-date-patterns-default-config");
    // also see testEDTZone
  }

  public void testAsctimeLeniency() throws Exception {
    // test double digit day
    assertParsedDate("Fri Oct 07 13:14:15 2005" , Date.from(inst20051007131415()), "parse-date-patterns-default-config");
    // test 2 spaces left of a single digit day
    assertParsedDate("Fri Oct  7 13:14:15 2005" , Date.from(inst20051007131415()), "parse-date-patterns-default-config");
    // longer day of week
    assertParsedDate("Friday Oct 7 13:14:15 2005", Date.from(inst20051007131415()), "parse-date-patterns-default-config");
  }

  public void testParseQuotedDate() throws IOException {
    // also using 2 digit day
    assertParsedDate("'Fri, 14 Oct 2005 13:14:15 GMT'",
        Date.from(instant(2005, 10, 14, 13, 14, 15)), "parse-date-patterns-default-config");
  }

  private static Instant instant(final int year, final int month, final int day, int hour, int minute, int second) {
    return LocalDate.of(year, month, day).atTime(hour, minute, second).toInstant(ZoneOffset.UTC);
  }

  private Instant inst20051007131415() {
    return instant(2005, 10, 7, 13, 14, 15);
  }

  private void assertParsedDate(String inputDateString, Date expectedDate, String chain) throws IOException {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull("date_dt")); // should match "*_dt" dynamic field
    SolrInputDocument d = processAdd(chain, doc(f("id", "1"), f("date_dt", inputDateString)));
    assertNotNull(d);
    assertThat("Date string: " + inputDateString + " was not parsed as a date",
        d.getFieldValue("date_dt"), IS_DATE);
    assertEquals(expectedDate, d.getField("date_dt").getFirstValue());
  }

  private static Date parse(DateTimeFormatter dateTimeFormatter, String dateString) {
    final TemporalAccessor temporalAccessor = dateTimeFormatter.parseBest(dateString, OffsetDateTime::from,
        ZonedDateTime::from, LocalDateTime::from, LocalDate::from, Instant::from);
    return temporalToDate(temporalAccessor, dateTimeFormatter.getZone());
  }

  private static Date temporalToDate(TemporalAccessor in, ZoneId timeZoneId) {
    if (in instanceof OffsetDateTime) {
      return Date.from(((OffsetDateTime) in).toInstant());
    } else if (in instanceof ZonedDateTime) {
      return Date.from(((ZonedDateTime) in).toInstant());
    } else if (in instanceof LocalDateTime) {
      return Date.from(((LocalDateTime) in).atZone(timeZoneId).toInstant());
    } else if (in instanceof Instant) {
      return Date.from((Instant) in);
    } else {
      return Date.from(((LocalDate) in).atStartOfDay(timeZoneId).toInstant());
    }
  }
}
