package org.apache.lucene.spatial.prefix;

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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.spatial.NumberRangePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.DateRangePrefixTree;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Calendar;

public class DateNRStrategyTest extends RandomSpatialOpStrategyTestCase {

  static final int ITERATIONS = 10;

  DateRangePrefixTree tree;

  int era;
  int year;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    tree = DateRangePrefixTree.INSTANCE;
    strategy = new NumberRangePrefixTreeStrategy(tree, "dateRange");
    era = random().nextBoolean() ? 0 : 1;
    year = 1 + random().nextInt(2_000_000);
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

  @Test @Ignore("see LUCENE-5692")
  @Repeat(iterations = ITERATIONS)
  public void testDisjoint() throws IOException {
    testOperationRandomShapes(SpatialOperation.IsDisjointTo);
  }

  @Test
  public void testWithinSame() throws IOException {
    final Calendar cal = tree.newCal();
    cal.set(Calendar.ERA, era);
    cal.set(Calendar.YEAR, year);

    testOperation(
        tree.toShape(cal),
        SpatialOperation.IsWithin,
        tree.toShape(cal), true);//is within itself
  }

  @Test
  public void testWorld() throws IOException {
    testOperation(
        tree.toShape(tree.newCal()),//world matches everything
        SpatialOperation.Contains,
        tree.toShape(randomCalendar()), true);
  }

  @Test
  public void testBugInitIterOptimization() throws Exception {
    //bug due to fast path initIter() optimization
    testOperation(
        tree.parseShape("[2014-03-27T23 TO 2014-04-01T01]"),
        SpatialOperation.Intersects,
        tree.parseShape("[2014-04 TO 2014-04-01T02]"), true);
  }

  @Override
  protected Shape randomIndexedShape() {
    Calendar cal1 = randomCalendar();
    Shape s1 = tree.toShape(cal1);
    try {
      Calendar cal2 = randomCalendar();
      Shape s2 = tree.toShape(cal2);
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
    cal.setTimeInMillis(random().nextLong());
    cal.set(Calendar.ERA, era);
    cal.set(Calendar.YEAR, year);
    try {
      tree.clearFieldsAfter(cal, random().nextInt(Calendar.FIELD_COUNT+1)-1);
    } catch (AssertionError e) {
      if (!e.getMessage().equals("Calendar underflow"))
        throw e;
    }
    return cal;
  }

  @Override
  protected Shape randomQueryShape() {
    return randomIndexedShape();
  }
}
