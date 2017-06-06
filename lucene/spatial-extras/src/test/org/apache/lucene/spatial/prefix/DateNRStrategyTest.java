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
package org.apache.lucene.spatial.prefix;

import java.io.IOException;
import java.util.Calendar;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.apache.lucene.spatial.prefix.tree.DateRangePrefixTree;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.UnitNRShape;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.spatial4j.shape.Shape;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomInt;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;

public class DateNRStrategyTest extends RandomSpatialOpStrategyTestCase {

  static final int ITERATIONS = 10;

  DateRangePrefixTree tree;

  long randomCalWindowMs;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    tree = DateRangePrefixTree.INSTANCE;
    strategy = new NumberRangePrefixTreeStrategy(tree, "dateRange");
    ((NumberRangePrefixTreeStrategy)strategy).setPointsOnly(randomInt() % 5 == 0);
    Calendar tmpCal = tree.newCal();
    int randomCalWindowField = randomIntBetween(Calendar.YEAR, Calendar.MILLISECOND);
    tmpCal.add(randomCalWindowField, 2_000);
    randomCalWindowMs = Math.max(2000L, tmpCal.getTimeInMillis());
  }

  @Test
  @Repeat(iterations = ITERATIONS)
  public void testIntersects() throws IOException {
    testOperationRandomShapes(SpatialOperation.Intersects);
  }

  @Test
  @Repeat(iterations = ITERATIONS)
  public void testWithin() throws IOException {
    testOperationRandomShapes(SpatialOperation.IsWithin);
  }

  @Test
  @Repeat(iterations = ITERATIONS)
  public void testContains() throws IOException {
    testOperationRandomShapes(SpatialOperation.Contains);
  }

  @Test
  public void testWithinSame() throws IOException {
    Shape shape = randomIndexedShape();
    testOperation(
        shape,
        SpatialOperation.IsWithin,
        shape, true);//is within itself
  }

  @Test
  public void testWorld() throws IOException {
    ((NumberRangePrefixTreeStrategy)strategy).setPointsOnly(false);
    testOperation(
        tree.toShape(tree.newCal()),//world matches everything
        SpatialOperation.Contains,
        tree.toShape(randomCalendar()), true);
  }

  @Test
  public void testBugInitIterOptimization() throws Exception {
    ((NumberRangePrefixTreeStrategy)strategy).setPointsOnly(false);
    //bug due to fast path initIter() optimization
    testOperation(
        tree.parseShape("[2014-03-27T23 TO 2014-04-01T01]"),
        SpatialOperation.Intersects,
        tree.parseShape("[2014-04 TO 2014-04-01T02]"), true);
  }

  @Test
  public void testLastMillionYearPeriod() throws Exception {
    testOperation(
        tree.parseShape("+292220922-05-17T18:01:57.572"), // a year in the last million year period (>=292M)
        SpatialOperation.Intersects,
        tree.parseShape("[1970 TO *]"), true
    );
  }

  @Override
  protected Shape randomIndexedShape() {
    if (((NumberRangePrefixTreeStrategy)strategy).isPointsOnly()) {
      Calendar cal = tree.newCal();
      cal.setTimeInMillis(random().nextLong());
      return tree.toShape(cal);
    } else {
      return randomShape();
    }
  }

  @Override
  protected Shape randomQueryShape() {
    return randomShape();
  }

  private Shape randomShape() {
    Calendar cal1 = randomCalendar();
    UnitNRShape s1 = tree.toShape(cal1);
    if (rarely()) {
      return s1;
    }
    try {
      Calendar cal2 = randomCalendar();
      UnitNRShape s2 = tree.toShape(cal2);
      if (cal1.compareTo(cal2) < 0) {
        return tree.toRangeShape(s1, s2);
      } else {
        return tree.toRangeShape(s2, s1);
      }
    } catch (IllegalArgumentException e) {
      assert e.getMessage().startsWith("Differing precision");
      return s1;
    }
  }

  private Calendar randomCalendar() {
    Calendar cal = tree.newCal();
    cal.setTimeInMillis(random().nextLong() % randomCalWindowMs);
    try {
      tree.clearFieldsAfter(cal, random().nextInt(Calendar.FIELD_COUNT+1)-1);
    } catch (AssertionError e) {
      if (!e.getMessage().equals("Calendar underflow"))
        throw e;
    }
    return cal;
  }
}
