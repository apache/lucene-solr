package org.apache.lucene.spatial.geometry;

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

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

/**
 * Tests for {@link org.apache.lucene.spatial.geometry.DistanceUnits}
 */
public class TestDistanceUnits extends LuceneTestCase {

  /**
   * Pass condition: When finding the DistanceUnit for "km", KILOMETRES is found.  When finding the DistanceUnit for
   * "miles", MILES is found.
   */
  @Test
  public void testFindDistanceUnit() {
    assertEquals(DistanceUnits.KILOMETERS, DistanceUnits.findDistanceUnit("km"));
    assertEquals(DistanceUnits.MILES, DistanceUnits.findDistanceUnit("miles"));
  }

  /**
   * Pass condition: Searching for the DistanceUnit of an unknown unit "mls" should throw an IllegalArgumentException.
   */
  @Test
  public void testFindDistanceUnit_unknownUnit() {
    try {
      DistanceUnits.findDistanceUnit("mls");
      assertTrue("IllegalArgumentException should have been thrown", false);
    } catch (IllegalArgumentException iae) {
      // Expected
    }
  }

  /**
   * Pass condition: Converting between the same units should not change the value.  Converting from MILES to KILOMETRES
   * involves multiplying the distance by the ratio, and converting from KILOMETRES to MILES involves dividing by the ratio
   */
  @Test
  public void testConvert() {
    assertEquals(10.5, DistanceUnits.MILES.convert(10.5, DistanceUnits.MILES), 0D);
    assertEquals(10.5, DistanceUnits.KILOMETERS.convert(10.5, DistanceUnits.KILOMETERS), 0D);
    assertEquals(10.5 * 1.609344, DistanceUnits.KILOMETERS.convert(10.5, DistanceUnits.MILES), 0D);
    assertEquals(10.5 / 1.609344, DistanceUnits.MILES.convert(10.5, DistanceUnits.KILOMETERS), 0D);
  }
}
