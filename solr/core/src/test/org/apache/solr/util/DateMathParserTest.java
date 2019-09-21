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

import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.solr.SolrTestCaseJ4;

import static org.apache.solr.util.DateMathParser.UTC;

/**
 * Tests that the functions in DateMathParser
 */
public class DateMathParserTest extends SolrTestCaseJ4 {

  /**
   * A formatter for specifying every last nuance of a Date for easy
   * reference in assertion statements
   */
  private DateTimeFormatter fmt;

  /**
   * A parser for reading in explicit dates that are convenient to type
   * in a test
   */
  private DateTimeFormatter parser;

  public DateMathParserTest() {
    fmt = DateTimeFormatter.ofPattern("G yyyyy MM ww W D dd F E a HH hh mm ss SSS z Z", Locale.ROOT)
        .withZone(ZoneOffset.UTC);

    parser = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneOffset.UTC); // basically without the 'Z'
  }

  /** MACRO: Round: parses s, rounds with u, fmts */
  protected String r(String s, String u) throws Exception {
    Date dt = DateMathParser.parseMath(null, s + "Z/" + u);
    return fmt.format(dt.toInstant());
  }
  
  /** MACRO: Add: parses s, adds v u, fmts */
  protected String a(String s, int v, String u) throws Exception {
    char sign = v >= 0 ? '+' : '-';
    Date dt = DateMathParser.parseMath(null, s + 'Z' + sign + Math.abs(v) + u);
    return fmt.format(dt.toInstant());
  }

  /** MACRO: Expected: parses s, fmts */
  protected String e(String s) throws Exception {
    return fmt.format(parser.parse(s, Instant::from));
  }

  protected void assertRound(String e, String i, String u) throws Exception {
    String ee = e(e);
    String rr = r(i,u);
    assertEquals(ee + " != " + rr + " round:" + i + ":" + u, ee, rr);
  }

  protected void assertAdd(String e, String i, int v, String u)
    throws Exception {
    
    String ee = e(e);
    String aa = a(i,v,u);
    assertEquals(ee + " != " + aa + " add:" + i + "+" + v + ":" + u, ee, aa);
  }

  protected void assertMath(String e, DateMathParser p, String i)
    throws Exception {
    
    String ee = e(e);
    String aa = fmt.format(p.parseMath(i).toInstant());
    assertEquals(ee + " != " + aa + " math:" +
                 parser.format(p.getNow().toInstant()) + ":" + i, ee, aa);
  }

  private void setNow(DateMathParser p, String text) {
    p.setNow(Date.from(parser.parse(text, Instant::from)));
  }
  
  public void testCalendarUnitsConsistency() throws Exception {
    String input = "1234-07-04T12:08:56.235";
    for (String u : DateMathParser.CALENDAR_UNITS.keySet()) {
      try {
        r(input, u);
      } catch (IllegalStateException e) {
        assertNotNull("no logic for rounding: " + u, e);
      }
      try {
        a(input, 1, u);
      } catch (IllegalStateException e) {
        assertNotNull("no logic for rounding: " + u, e);
      }
    }
  }
  
  public void testRound() throws Exception {
    
    String input = "1234-07-04T12:08:56.235";
    
    assertRound("1234-07-04T12:08:56.000", input, "SECOND");
    assertRound("1234-07-04T12:08:00.000", input, "MINUTE");
    assertRound("1234-07-04T12:00:00.000", input, "HOUR");
    assertRound("1234-07-04T00:00:00.000", input, "DAY");
    assertRound("1234-07-01T00:00:00.000", input, "MONTH");
    assertRound("1234-01-01T00:00:00.000", input, "YEAR");

  }

  public void testAddZero() throws Exception {
    
    String input = "1234-07-04T12:08:56.235";
    
    for (String u : DateMathParser.CALENDAR_UNITS.keySet()) {
      assertAdd(input, input, 0, u);
    }
  }

  
  public void testAdd() throws Exception {
    
    String input = "1234-07-04T12:08:56.235";
    
    assertAdd("1234-07-04T12:08:56.236", input, 1, "MILLISECOND");
    assertAdd("1234-07-04T12:08:57.235", input, 1, "SECOND");
    assertAdd("1234-07-04T12:09:56.235", input, 1, "MINUTE");
    assertAdd("1234-07-04T13:08:56.235", input, 1, "HOUR");
    assertAdd("1234-07-05T12:08:56.235", input, 1, "DAY");
    assertAdd("1234-08-04T12:08:56.235", input, 1, "MONTH");
    assertAdd("1235-07-04T12:08:56.235", input, 1, "YEAR");
    
  }
  
  public void testParseStatelessness() throws Exception {

    DateMathParser p = new DateMathParser(UTC);
    setNow(p, "1234-07-04T12:08:56.235");

    String e = fmt.format(p.parseMath("").toInstant());
    
    Date trash = p.parseMath("+7YEARS");
    trash = p.parseMath("/MONTH");
    trash = p.parseMath("-5DAYS+20MINUTES");
    Thread.currentThread();
    Thread.sleep(5);
    
    String a =fmt.format(p.parseMath("").toInstant());
    assertEquals("State of DateMathParser changed", e, a);
  }

  public void testParseMath() throws Exception {

    DateMathParser p = new DateMathParser(UTC);
    setNow(p, "1234-07-04T12:08:56.235");

    // No-Op
    assertMath("1234-07-04T12:08:56.235", p, "");
    
    // simple round
    assertMath("1234-07-04T12:08:56.235", p, "/MILLIS"); // no change
    assertMath("1234-07-04T12:08:56.000", p, "/SECOND");
    assertMath("1234-07-04T12:08:00.000", p, "/MINUTE");
    assertMath("1234-07-04T12:00:00.000", p, "/HOUR");
    assertMath("1234-07-04T00:00:00.000", p, "/DAY");
    assertMath("1234-07-01T00:00:00.000", p, "/MONTH");
    assertMath("1234-01-01T00:00:00.000", p, "/YEAR");

    // simple addition
    assertMath("1234-07-04T12:08:56.236", p, "+1MILLISECOND");
    assertMath("1234-07-04T12:08:57.235", p, "+1SECOND");
    assertMath("1234-07-04T12:09:56.235", p, "+1MINUTE");
    assertMath("1234-07-04T13:08:56.235", p, "+1HOUR");
    assertMath("1234-07-05T12:08:56.235", p, "+1DAY");
    assertMath("1234-08-04T12:08:56.235", p, "+1MONTH");
    assertMath("1235-07-04T12:08:56.235", p, "+1YEAR");

    // simple subtraction
    assertMath("1234-07-04T12:08:56.234", p, "-1MILLISECOND");
    assertMath("1234-07-04T12:08:55.235", p, "-1SECOND");
    assertMath("1234-07-04T12:07:56.235", p, "-1MINUTE");
    assertMath("1234-07-04T11:08:56.235", p, "-1HOUR");
    assertMath("1234-07-03T12:08:56.235", p, "-1DAY");
    assertMath("1234-06-04T12:08:56.235", p, "-1MONTH");
    assertMath("1233-07-04T12:08:56.235", p, "-1YEAR");

    // simple '+/-'
    assertMath("1234-07-04T12:08:56.235", p, "+1MILLISECOND-1MILLISECOND");
    assertMath("1234-07-04T12:08:56.235", p, "+1SECOND-1SECOND");
    assertMath("1234-07-04T12:08:56.235", p, "+1MINUTE-1MINUTE");
    assertMath("1234-07-04T12:08:56.235", p, "+1HOUR-1HOUR");
    assertMath("1234-07-04T12:08:56.235", p, "+1DAY-1DAY");
    assertMath("1234-07-04T12:08:56.235", p, "+1MONTH-1MONTH");
    assertMath("1234-07-04T12:08:56.235", p, "+1YEAR-1YEAR");

    // simple '-/+'
    assertMath("1234-07-04T12:08:56.235", p, "-1MILLISECOND+1MILLISECOND");
    assertMath("1234-07-04T12:08:56.235", p, "-1SECOND+1SECOND");
    assertMath("1234-07-04T12:08:56.235", p, "-1MINUTE+1MINUTE");
    assertMath("1234-07-04T12:08:56.235", p, "-1HOUR+1HOUR");
    assertMath("1234-07-04T12:08:56.235", p, "-1DAY+1DAY");
    assertMath("1234-07-04T12:08:56.235", p, "-1MONTH+1MONTH");
    assertMath("1234-07-04T12:08:56.235", p, "-1YEAR+1YEAR");

    // more complex stuff
    assertMath("1233-07-04T12:08:56.236", p, "+1MILLISECOND-1YEAR");
    assertMath("1233-07-04T12:08:57.235", p, "+1SECOND-1YEAR");
    assertMath("1233-07-04T12:09:56.235", p, "+1MINUTE-1YEAR");
    assertMath("1233-07-04T13:08:56.235", p, "+1HOUR-1YEAR");
    assertMath("1233-07-05T12:08:56.235", p, "+1DAY-1YEAR");
    assertMath("1233-08-04T12:08:56.235", p, "+1MONTH-1YEAR");
    assertMath("1233-07-04T12:08:56.236", p, "-1YEAR+1MILLISECOND");
    assertMath("1233-07-04T12:08:57.235", p, "-1YEAR+1SECOND");
    assertMath("1233-07-04T12:09:56.235", p, "-1YEAR+1MINUTE");
    assertMath("1233-07-04T13:08:56.235", p, "-1YEAR+1HOUR");
    assertMath("1233-07-05T12:08:56.235", p, "-1YEAR+1DAY");
    assertMath("1233-08-04T12:08:56.235", p, "-1YEAR+1MONTH");
    assertMath("1233-07-01T00:00:00.000", p, "-1YEAR+1MILLISECOND/MONTH");
    assertMath("1233-07-04T00:00:00.000", p, "-1YEAR+1SECOND/DAY");
    assertMath("1233-07-04T00:00:00.000", p, "-1YEAR+1MINUTE/DAY");
    assertMath("1233-07-04T13:00:00.000", p, "-1YEAR+1HOUR/HOUR");
    assertMath("1233-07-05T12:08:56.000", p, "-1YEAR+1DAY/SECOND");
    assertMath("1233-08-04T12:08:56.000", p, "-1YEAR+1MONTH/SECOND");

    // "tricky" cases
    setNow(p, "2006-01-31T17:09:59.999");
    assertMath("2006-02-28T17:09:59.999", p, "+1MONTH");
    assertMath("2008-02-29T17:09:59.999", p, "+25MONTH");
    assertMath("2006-02-01T00:00:00.000", p, "/MONTH+35DAYS/MONTH");
    assertMath("2006-01-31T17:10:00.000", p, "+3MILLIS/MINUTE");
  }

  public void testParseMathTz() throws Exception {

    final String PLUS_TZS = "America/Los_Angeles";
    final String NEG_TZS = "Europe/Paris";
    
    assumeTrue("Test requires JVM to know about about TZ: " + PLUS_TZS,
               TimeZoneUtils.KNOWN_TIMEZONE_IDS.contains(PLUS_TZS)); 
    assumeTrue("Test requires JVM to know about about TZ: " + NEG_TZS,
               TimeZoneUtils.KNOWN_TIMEZONE_IDS.contains(NEG_TZS)); 

    // US, Positive Offset with DST

    TimeZone tz = TimeZone.getTimeZone(PLUS_TZS);
    DateMathParser p = new DateMathParser(tz);

    setNow(p, "2001-07-04T12:08:56.235");

    // No-Op
    assertMath("2001-07-04T12:08:56.235", p, "");
    assertMath("2001-07-04T12:08:56.235", p, "/MILLIS");

    assertMath("2001-07-04T12:08:56.000", p, "/SECOND");
    assertMath("2001-07-04T12:08:00.000", p, "/MINUTE");
    assertMath("2001-07-04T12:00:00.000", p, "/HOUR");
    assertMath("2001-07-04T07:00:00.000", p, "/DAY");
    assertMath("2001-07-01T07:00:00.000", p, "/MONTH");
    // no DST in jan
    assertMath("2001-01-01T08:00:00.000", p, "/YEAR");
    // no DST in nov 2001
    assertMath("2001-11-04T08:00:00.000", p, "+4MONTH/DAY");
    // yes DST in nov 2010
    assertMath("2010-11-04T07:00:00.000", p, "+9YEAR+4MONTH/DAY");

    // France, Negative Offset with DST

    tz = TimeZone.getTimeZone(NEG_TZS);
    p = new DateMathParser(tz);
    setNow(p, "2001-07-04T12:08:56.235");

    assertMath("2001-07-04T12:08:56.000", p, "/SECOND");
    assertMath("2001-07-04T12:08:00.000", p, "/MINUTE");
    assertMath("2001-07-04T12:00:00.000", p, "/HOUR");
    assertMath("2001-07-03T22:00:00.000", p, "/DAY");
    assertMath("2001-06-30T22:00:00.000", p, "/MONTH");
    // no DST in dec
    assertMath("2000-12-31T23:00:00.000", p, "/YEAR");
    // no DST in nov
    assertMath("2001-11-03T23:00:00.000", p, "+4MONTH/DAY");

  } 
 
  public void testParseMathExceptions() throws Exception {
    
    DateMathParser p = new DateMathParser(UTC);
    setNow(p, "1234-07-04T12:08:56.235");
    
    Map<String,Integer> badCommands = new HashMap<>();
    badCommands.put("/", 1);
    badCommands.put("+", 1);
    badCommands.put("-", 1);
    badCommands.put("/BOB", 1);
    badCommands.put("+SECOND", 1);
    badCommands.put("-2MILLI/", 4);
    badCommands.put(" +BOB", 0);
    badCommands.put("+2SECONDS ", 3);
    badCommands.put("/4", 1);
    badCommands.put("?SECONDS", 0);

    for (String command : badCommands.keySet()) {
      ParseException e = expectThrows(ParseException.class, () -> p.parseMath(command));
      assertEquals("Wrong pos for: " + command + " => " + e.getMessage(),
          badCommands.get(command).intValue(), e.getErrorOffset());
    }
    
  }

  /*
  PARSING / FORMATTING (without date math)  Formerly in DateFieldTest.
   */


  public void testFormatter() {
    assertFormat("1995-12-31T23:59:59.999Z", 820454399999l);
    assertFormat("1995-12-31T23:59:59.990Z", 820454399990l);
    assertFormat("1995-12-31T23:59:59.900Z", 820454399900l);
    assertFormat("1995-12-31T23:59:59Z", 820454399000l);

    // just after epoch
    assertFormat("1970-01-01T00:00:00.005Z", 5L);
    assertFormat("1970-01-01T00:00:00Z",     0L);
    assertFormat("1970-01-01T00:00:00.370Z",  370L);
    assertFormat("1970-01-01T00:00:00.900Z",   900L);

    // well after epoch
    assertFormat("1999-12-31T23:59:59.005Z", 946684799005L);
    assertFormat("1999-12-31T23:59:59Z",     946684799000L);
    assertFormat("1999-12-31T23:59:59.370Z",  946684799370L);
    assertFormat("1999-12-31T23:59:59.900Z",   946684799900L);

    // waaaay after epoch  ('+' is required for more than 4 digits in a year)
    assertFormat("+12345-12-31T23:59:59.005Z", 327434918399005L);
    assertFormat("+12345-12-31T23:59:59Z",     327434918399000L);
    assertFormat("+12345-12-31T23:59:59.370Z",  327434918399370L);
    assertFormat("+12345-12-31T23:59:59.900Z",   327434918399900L);

    // well before epoch
    assertFormat("0299-12-31T23:59:59Z",     -52700112001000L);
    assertFormat("0299-12-31T23:59:59.123Z", -52700112000877L);
    assertFormat("0299-12-31T23:59:59.090Z",  -52700112000910L);

    // BC (negative years)
    assertFormat("-12021-12-01T02:02:02Z", Instant.parse("-12021-12-01T02:02:02Z").toEpochMilli());
  }

  private void assertFormat(final String expected, final long millis) {
    assertEquals(expected, Instant.ofEpochMilli(millis).toString()); // assert same as ISO_INSTANT
    assertEquals(millis, DateMathParser.parseMath(null, expected).getTime()); // assert DMP has same result
  }

  /**
   * Using dates in the canonical format, verify that parsing+formatting
   * is an identify function
   */
  public void testRoundTrip() throws Exception {
    // NOTE: the 2nd arg is what the round trip result looks like (may be null if same as input)

    assertParseFormatEquals("1995-12-31T23:59:59.999666Z",  "1995-12-31T23:59:59.999Z"); // beyond millis is truncated
    assertParseFormatEquals("1995-12-31T23:59:59.999Z",     "1995-12-31T23:59:59.999Z");
    assertParseFormatEquals("1995-12-31T23:59:59.99Z",      "1995-12-31T23:59:59.990Z");
    assertParseFormatEquals("1995-12-31T23:59:59.9Z",       "1995-12-31T23:59:59.900Z");
    assertParseFormatEquals("1995-12-31T23:59:59Z",         "1995-12-31T23:59:59Z");

    // here the input isn't in the canonical form, but we should be forgiving
    assertParseFormatEquals("1995-12-31T23:59:59.990Z", "1995-12-31T23:59:59.990Z");
    assertParseFormatEquals("1995-12-31T23:59:59.900Z", "1995-12-31T23:59:59.900Z");
    assertParseFormatEquals("1995-12-31T23:59:59.90Z",  "1995-12-31T23:59:59.900Z");
    assertParseFormatEquals("1995-12-31T23:59:59.000Z", "1995-12-31T23:59:59Z");
    assertParseFormatEquals("1995-12-31T23:59:59.00Z",  "1995-12-31T23:59:59Z");
    assertParseFormatEquals("1995-12-31T23:59:59.0Z",   "1995-12-31T23:59:59Z");

    // kind of kludgy, but we have other tests for the actual date math
    //assertParseFormatEquals("NOW/DAY", p.parseMath("/DAY").toInstant().toString());

    // as of Solr 1.3
    assertParseFormatEquals("1995-12-31T23:59:59Z/DAY", "1995-12-31T00:00:00Z");
    assertParseFormatEquals("1995-12-31T23:59:59.123Z/DAY", "1995-12-31T00:00:00Z");
    assertParseFormatEquals("1995-12-31T23:59:59.123999Z/DAY", "1995-12-31T00:00:00Z");

    // typical dates, various precision  (0,1,2,3 digits of millis)
    assertParseFormatEquals("1995-12-31T23:59:59.987Z", null);
    assertParseFormatEquals("1995-12-31T23:59:59.98Z", "1995-12-31T23:59:59.980Z");//add 0 ms
    assertParseFormatEquals("1995-12-31T23:59:59.9Z",  "1995-12-31T23:59:59.900Z");//add 00 ms
    assertParseFormatEquals("1995-12-31T23:59:59Z", null);
    assertParseFormatEquals("1976-03-06T03:06:00Z", null);
    assertParseFormatEquals("1995-12-31T23:59:59.987654Z", "1995-12-31T23:59:59.987Z");//truncate nanoseconds off

    // dates with atypical years
    assertParseFormatEquals("0001-01-01T01:01:01Z", null);
    assertParseFormatEquals("+12021-12-01T03:03:03Z", null);

    assertParseFormatEquals("0000-04-04T04:04:04Z", null); // note: 0 AD is also known as 1 BC

    // dates with negative years (BC)
    assertParseFormatEquals("-0005-05-05T05:05:05Z", null);
    assertParseFormatEquals("-2021-12-01T04:04:04Z", null);
    assertParseFormatEquals("-12021-12-01T02:02:02Z", null);
  }

  public void testParseLenient() throws Exception {
    // dates that only parse thanks to lenient mode of DateTimeFormatter
    assertParseFormatEquals("10995-12-31T23:59:59.990Z", "+10995-12-31T23:59:59.990Z"); // missing '+' 5 digit year
    assertParseFormatEquals("995-1-2T3:4:5Z", "0995-01-02T03:04:05Z"); // wasn't 0 padded
  }

  private void assertParseFormatEquals(String inputStr, String expectedStr) {
    if (expectedStr == null) {
      expectedStr = inputStr;
    }
    Date inputDate = DateMathParser.parseMath(null, inputStr);
    String resultStr = inputDate.toInstant().toString();
    assertEquals("d:" + inputDate.getTime(), expectedStr, resultStr);
  }
}

