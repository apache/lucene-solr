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

package org.apache.solr.util;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.util.DateMathParser;

import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.Locale;

import java.util.Map;
import java.util.HashMap;
import java.text.ParseException;

/**
 * Tests that the functions in DateMathParser
 */
public class DateMathParserTest extends LuceneTestCase {

  public static TimeZone UTC = TimeZone.getTimeZone("UTC");
  
  /**
   * A formatter for specifying every last nuance of a Date for easy
   * refernece in assertion statements
   */
  private DateFormat fmt;
  /**
   * A parser for reading in explicit dates that are convinient to type
   * in a test
   */
  private DateFormat parser;

  public DateMathParserTest() {
    super();
    fmt = new SimpleDateFormat
      ("G yyyyy MM ww WW DD dd F E aa HH hh mm ss SSS z Z",Locale.US);
    fmt.setTimeZone(UTC);

    parser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS",Locale.US);
    parser.setTimeZone(UTC);
  }

  /** MACRO: Round: parses s, rounds with u, fmts */
  protected String r(String s, String u) throws Exception {
    Date d = parser.parse(s);
    Calendar c = Calendar.getInstance(UTC, Locale.US);
    c.setTime(d);
    DateMathParser.round(c, u);
    return fmt.format(c.getTime());
  }
  
  /** MACRO: Add: parses s, adds v u, fmts */
  protected String a(String s, int v, String u) throws Exception {
    Date d = parser.parse(s);
    Calendar c = Calendar.getInstance(UTC, Locale.US);
    c.setTime(d);
    DateMathParser.add(c, v, u);
    return fmt.format(c.getTime());
  }

  /** MACRO: Expected: parses s, fmts */
  protected String e(String s) throws Exception {
    return fmt.format(parser.parse(s));
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
    String aa = fmt.format(p.parseMath(i));
    assertEquals(ee + " != " + aa + " math:" +
                 parser.format(p.getNow()) + ":" + i, ee, aa);
  }
  
  public void testCalendarUnitsConsistency() throws Exception {
    String input = "2001-07-04T12:08:56.235";
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
    
    String input = "2001-07-04T12:08:56.235";
    
    assertRound("2001-07-04T12:08:56.000", input, "SECOND");
    assertRound("2001-07-04T12:08:00.000", input, "MINUTE");
    assertRound("2001-07-04T12:00:00.000", input, "HOUR");
    assertRound("2001-07-04T00:00:00.000", input, "DAY");
    assertRound("2001-07-01T00:00:00.000", input, "MONTH");
    assertRound("2001-01-01T00:00:00.000", input, "YEAR");

  }

  public void testAddZero() throws Exception {
    
    String input = "2001-07-04T12:08:56.235";
    
    for (String u : DateMathParser.CALENDAR_UNITS.keySet()) {
      assertAdd(input, input, 0, u);
    }
  }

  
  public void testAdd() throws Exception {
    
    String input = "2001-07-04T12:08:56.235";
    
    assertAdd("2001-07-04T12:08:56.236", input, 1, "MILLISECOND");
    assertAdd("2001-07-04T12:08:57.235", input, 1, "SECOND");
    assertAdd("2001-07-04T12:09:56.235", input, 1, "MINUTE");
    assertAdd("2001-07-04T13:08:56.235", input, 1, "HOUR");
    assertAdd("2001-07-05T12:08:56.235", input, 1, "DAY");
    assertAdd("2001-08-04T12:08:56.235", input, 1, "MONTH");
    assertAdd("2002-07-04T12:08:56.235", input, 1, "YEAR");
    
  }
  
  public void testParseStatelessness() throws Exception {

    DateMathParser p = new DateMathParser(UTC, Locale.US);
    p.setNow(parser.parse("2001-07-04T12:08:56.235"));

    String e = fmt.format(p.parseMath(""));
    
    Date trash = p.parseMath("+7YEARS");
    trash = p.parseMath("/MONTH");
    trash = p.parseMath("-5DAYS+20MINUTES");
    Thread.currentThread().sleep(5);
    
    String a = fmt.format(p.parseMath(""));
    assertEquals("State of DateMathParser changed", e, a);
  }
    
  public void testParseMath() throws Exception {

    DateMathParser p = new DateMathParser(UTC, Locale.US);
    p.setNow(parser.parse("2001-07-04T12:08:56.235"));

    // No-Op
    assertMath("2001-07-04T12:08:56.235", p, "");
    
    // simple round
    assertMath("2001-07-04T12:08:56.000", p, "/SECOND");
    assertMath("2001-07-04T12:08:00.000", p, "/MINUTE");
    assertMath("2001-07-04T12:00:00.000", p, "/HOUR");
    assertMath("2001-07-04T00:00:00.000", p, "/DAY");
    assertMath("2001-07-01T00:00:00.000", p, "/MONTH");
    assertMath("2001-01-01T00:00:00.000", p, "/YEAR");

    // simple addition
    assertMath("2001-07-04T12:08:56.236", p, "+1MILLISECOND");
    assertMath("2001-07-04T12:08:57.235", p, "+1SECOND");
    assertMath("2001-07-04T12:09:56.235", p, "+1MINUTE");
    assertMath("2001-07-04T13:08:56.235", p, "+1HOUR");
    assertMath("2001-07-05T12:08:56.235", p, "+1DAY");
    assertMath("2001-08-04T12:08:56.235", p, "+1MONTH");
    assertMath("2002-07-04T12:08:56.235", p, "+1YEAR");

    // simple subtraction
    assertMath("2001-07-04T12:08:56.234", p, "-1MILLISECOND");
    assertMath("2001-07-04T12:08:55.235", p, "-1SECOND");
    assertMath("2001-07-04T12:07:56.235", p, "-1MINUTE");
    assertMath("2001-07-04T11:08:56.235", p, "-1HOUR");
    assertMath("2001-07-03T12:08:56.235", p, "-1DAY");
    assertMath("2001-06-04T12:08:56.235", p, "-1MONTH");
    assertMath("2000-07-04T12:08:56.235", p, "-1YEAR");

    // simple '+/-'
    assertMath("2001-07-04T12:08:56.235", p, "+1MILLISECOND-1MILLISECOND");
    assertMath("2001-07-04T12:08:56.235", p, "+1SECOND-1SECOND");
    assertMath("2001-07-04T12:08:56.235", p, "+1MINUTE-1MINUTE");
    assertMath("2001-07-04T12:08:56.235", p, "+1HOUR-1HOUR");
    assertMath("2001-07-04T12:08:56.235", p, "+1DAY-1DAY");
    assertMath("2001-07-04T12:08:56.235", p, "+1MONTH-1MONTH");
    assertMath("2001-07-04T12:08:56.235", p, "+1YEAR-1YEAR");

    // simple '-/+'
    assertMath("2001-07-04T12:08:56.235", p, "-1MILLISECOND+1MILLISECOND");
    assertMath("2001-07-04T12:08:56.235", p, "-1SECOND+1SECOND");
    assertMath("2001-07-04T12:08:56.235", p, "-1MINUTE+1MINUTE");
    assertMath("2001-07-04T12:08:56.235", p, "-1HOUR+1HOUR");
    assertMath("2001-07-04T12:08:56.235", p, "-1DAY+1DAY");
    assertMath("2001-07-04T12:08:56.235", p, "-1MONTH+1MONTH");
    assertMath("2001-07-04T12:08:56.235", p, "-1YEAR+1YEAR");

    // more complex stuff
    assertMath("2000-07-04T12:08:56.236", p, "+1MILLISECOND-1YEAR");
    assertMath("2000-07-04T12:08:57.235", p, "+1SECOND-1YEAR");
    assertMath("2000-07-04T12:09:56.235", p, "+1MINUTE-1YEAR");
    assertMath("2000-07-04T13:08:56.235", p, "+1HOUR-1YEAR");
    assertMath("2000-07-05T12:08:56.235", p, "+1DAY-1YEAR");
    assertMath("2000-08-04T12:08:56.235", p, "+1MONTH-1YEAR");
    assertMath("2000-07-04T12:08:56.236", p, "-1YEAR+1MILLISECOND");
    assertMath("2000-07-04T12:08:57.235", p, "-1YEAR+1SECOND");
    assertMath("2000-07-04T12:09:56.235", p, "-1YEAR+1MINUTE");
    assertMath("2000-07-04T13:08:56.235", p, "-1YEAR+1HOUR");
    assertMath("2000-07-05T12:08:56.235", p, "-1YEAR+1DAY");
    assertMath("2000-08-04T12:08:56.235", p, "-1YEAR+1MONTH");
    assertMath("2000-07-01T00:00:00.000", p, "-1YEAR+1MILLISECOND/MONTH");
    assertMath("2000-07-04T00:00:00.000", p, "-1YEAR+1SECOND/DAY");
    assertMath("2000-07-04T00:00:00.000", p, "-1YEAR+1MINUTE/DAY");
    assertMath("2000-07-04T13:00:00.000", p, "-1YEAR+1HOUR/HOUR");
    assertMath("2000-07-05T12:08:56.000", p, "-1YEAR+1DAY/SECOND");
    assertMath("2000-08-04T12:08:56.000", p, "-1YEAR+1MONTH/SECOND");

    // "tricky" cases
    p.setNow(parser.parse("2006-01-31T17:09:59.999"));
    assertMath("2006-02-28T17:09:59.999", p, "+1MONTH");
    assertMath("2008-02-29T17:09:59.999", p, "+25MONTH");
    assertMath("2006-02-01T00:00:00.000", p, "/MONTH+35DAYS/MONTH");
    assertMath("2006-01-31T17:10:00.000", p, "+3MILLIS/MINUTE");

    
  }
  
  public void testParseMathExceptions() throws Exception {
    
    DateMathParser p = new DateMathParser(UTC, Locale.US);
    p.setNow(parser.parse("2001-07-04T12:08:56.235"));
    
    Map<String,Integer> badCommands = new HashMap<String,Integer>();
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
      try {
        Date out = p.parseMath(command);
        fail("Didn't generate ParseException for: " + command);
      } catch (ParseException e) {
        assertEquals("Wrong pos for: " + command + " => " + e.getMessage(),
                     badCommands.get(command).intValue(), e.getErrorOffset());

      }
    }
    
  }
    
}

