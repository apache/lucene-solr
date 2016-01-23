package org.apache.solr.common.util;

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

import java.text.ParseException;
import java.util.Locale;

import org.apache.lucene.util.LuceneTestCase;

public class TestDateUtil extends LuceneTestCase {

  public void testParseDate() throws ParseException {
    assertParsedDate(1226583351000L, "Thu Nov 13 04:35:51 AKST 2008");
  }
    
  private static void assertParsedDate(long ts, String dateStr) throws ParseException {
    long parsed = DateUtil.parseDate(dateStr).getTime();
    assertTrue(String.format(Locale.ENGLISH, "Incorrect parsed timestamp: %d != %d (%s)", ts, parsed, dateStr), Math.abs(ts - parsed) <= 1000L);
  }

}
