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
package org.apache.solr.handler.extraction;

import java.text.ParseException;
import java.util.Date;
import java.util.Locale;

import org.apache.lucene.util.LuceneTestCase;

public class TestExtractionDateUtil extends LuceneTestCase {

  public void testISO8601() throws Exception {
    // dates with atypical years
    assertParseFormatEquals("0001-01-01T01:01:01Z", null);
    assertParseFormatEquals("+12021-12-01T03:03:03Z", null);

    assertParseFormatEquals("0000-04-04T04:04:04Z", null); // note: 0 AD is also known as 1 BC

    // dates with negative years (BC)
    assertParseFormatEquals("-0005-05-05T05:05:05Z", null);
    assertParseFormatEquals("-2021-12-01T04:04:04Z", null);
    assertParseFormatEquals("-12021-12-01T02:02:02Z", null);

    // dates that only parse thanks to lenient mode of DateTimeFormatter
    assertParseFormatEquals("10995-12-31T23:59:59.990Z", "+10995-12-31T23:59:59.990Z"); // missing '+' 5 digit year
    assertParseFormatEquals("995-1-2T3:4:5Z", "0995-01-02T03:04:05Z"); // wasn't 0 padded
  }

  private static void assertParseFormatEquals(String inputStr, String expectedStr) throws ParseException {
    if (expectedStr == null) {
      expectedStr = inputStr;
    }
    Date inputDate = ExtractionDateUtil.parseDate(inputStr);
    String resultStr = inputDate.toInstant().toString();
    assertEquals("d:" + inputDate.getTime(), expectedStr, resultStr);
  }

  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-12593")
  public void testParseDate() throws ParseException {
    assertParsedDate(1226583351000L, "Thu Nov 13 04:35:51 AKST 2008");
  }

  private static void assertParsedDate(long ts, String dateStr) throws ParseException {
    long parsed = ExtractionDateUtil.parseDate(dateStr).getTime();
    assertTrue(String.format(Locale.ENGLISH, "Incorrect parsed timestamp: %d != %d (%s)", ts, parsed, dateStr), Math.abs(ts - parsed) <= 1000L);
  }
}
