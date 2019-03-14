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

import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCase;

import java.util.Set;
import java.util.HashSet;
import java.util.Random;
import java.util.TimeZone;
import java.util.Locale;

public class TimeZoneUtilsTest extends SolrTestCase {

  private static void assertSameRules(final String label,
                                      final TimeZone expected,
                                      final TimeZone actual) {
    
    if (null == expected && null == actual) return;

    assertNotNull(label + ": expected is null", expected);
    assertNotNull(label + ": actual is null", actual);

    final boolean same = expected.hasSameRules(actual);

    assertTrue(label + ": " + expected.toString() + " [[NOT SAME RULES]] " + 
               actual.toString(),
               same);
  }

  public void testValidIds() throws Exception {

    final Set<String> idsTested = new HashSet<>();

    // brain dead: anything the JVM supports, should work
    for (String validId : TimeZone.getAvailableIDs()) {
      assertTrue(validId + " not found in list of known ids",
                 TimeZoneUtils.KNOWN_TIMEZONE_IDS.contains(validId));

      final TimeZone expected = TimeZone.getTimeZone(validId);
      final TimeZone actual = TimeZoneUtils.getTimeZone(validId);
      assertSameRules(validId, expected, actual);

      idsTested.add(validId);
    }
    
    assertEquals("TimeZone.getAvailableIDs vs TimeZoneUtils.KNOWN_TIMEZONE_IDS",
                 TimeZoneUtils.KNOWN_TIMEZONE_IDS.size(), idsTested.size());
  }

  public void testCustom() throws Exception {

    for (String input : new String[] {"GMT-00", "GMT+00", "GMT-0", "GMT+0", 
                                      "GMT+08","GMT+8", "GMT-08","GMT-8",
                                      "GMT+0800","GMT+08:00",
                                      "GMT-0800","GMT-08:00",
                                      "GMT+23", "GMT+2300",
                                      "GMT-23", "GMT-2300"}) {
      assertSameRules(input, 
                      TimeZone.getTimeZone(input),
                      TimeZoneUtils.getTimeZone(input));
    }
  }

  public void testStupidIKnowButIDontTrustTheJVM() throws Exception {

    for (String input : new String[] {"GMT-00", "GMT+00", "GMT-0", "GMT+0", 
                                      "GMT+08","GMT+8", "GMT-08","GMT-8",
                                      "GMT+0800","GMT+08:00",
                                      "GMT-0800","GMT-08:00",
                                      "GMT+23", "GMT+2300",
                                      "GMT-23", "GMT-2300"}) {
      assertSameRules(input, 
                      TimeZone.getTimeZone(input),
                      TimeZone.getTimeZone(input));
    }
  }

  public void testInvalidInput() throws Exception {

    final String giberish = "giberish";
    assumeFalse("This test assumes that " + giberish + " is not a valid tz id",
                TimeZoneUtils.KNOWN_TIMEZONE_IDS.contains(giberish));
    assertNull(giberish, TimeZoneUtils.getTimeZone(giberish));


    for (String malformed : new String[] {"GMT+72", "GMT0800", 
                                          "GMT+2400" , "GMT+24:00",
                                          "GMT+11-30" , "GMT+11:-30",
                                          "GMT+0080" , "GMT+00:80"}) {
      assertNull(malformed, TimeZoneUtils.getTimeZone(malformed));
    }
  }



  public void testRandom() throws Exception {
    final String ONE_DIGIT = "%1d";
    final String TWO_DIGIT = "%02d";

    final Random r = random();
    final int iters = atLeast(r, 50);
    for (int i = 0; i <= iters; i++) {
      int hour = TestUtil.nextInt(r, 0, 23);
      int min = TestUtil.nextInt(r, 0, 59);

      String hours = String.format(Locale.ROOT, 
                                   (r.nextBoolean() ? ONE_DIGIT : TWO_DIGIT),
                                   hour);
      String mins = String.format(Locale.ROOT, TWO_DIGIT, min);
      String input = "GMT" + (r.nextBoolean()?"+":"-") 
        + hours + (r.nextBoolean() ? "" : ((r.nextBoolean()?":":"") + mins));
      assertSameRules(input,  
                      TimeZone.getTimeZone(input),
                      TimeZoneUtils.getTimeZone(input));
    }
  }
}

