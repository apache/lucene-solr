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
package org.apache.lucene.spatial.prefix.tree;

import java.text.ParseException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.UnitNRShape;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

public class DateRangePrefixTreeTest extends LuceneTestCase {

  @ParametersFactory(argumentFormatting = "calendar=%s")
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(new Object[][]{
        {"default", DateRangePrefixTree.DEFAULT_CAL},
        {"compat", DateRangePrefixTree.JAVA_UTIL_TIME_COMPAT_CAL}
    });
  }

  private final DateRangePrefixTree tree;

  public DateRangePrefixTreeTest(String suiteName, Calendar templateCal) {
    tree = new DateRangePrefixTree(templateCal);
  }

  public void testRoundTrip() throws Exception {
    Calendar cal = tree.newCal();

    assertEquals("*", tree.toString(cal));

    //test no underflow
    assertTrue(tree.toShape(new int[]{0}, 1).toString().startsWith("-"));

    //Some arbitrary date
    cal.set(2014, Calendar.MAY, 9);
    roundTrip(cal);
    assertEquals("2014-05-09",tree.toString(cal));

    //Earliest date
    cal.setTimeInMillis(Long.MIN_VALUE);
    roundTrip(cal);

    //Farthest date
    cal.setTimeInMillis(Long.MAX_VALUE);
    roundTrip(cal);

    //1BC is "0000".
    cal.clear();
    cal.set(Calendar.ERA, GregorianCalendar.BC);
    cal.set(Calendar.YEAR, 1);
    roundTrip(cal);
    assertEquals("0000", tree.toString(cal));
    //adding a "+" parses to the same; and a trailing 'Z' is fine too
    assertEquals(cal, tree.parseCalendar("+0000Z"));

    //2BC is "-0001"
    cal.clear();
    cal.set(Calendar.ERA, GregorianCalendar.BC);
    cal.set(Calendar.YEAR, 2);
    roundTrip(cal);
    assertEquals("-0001", tree.toString(cal));

    //1AD is "0001"
    cal.clear();
    cal.set(Calendar.YEAR, 1);
    roundTrip(cal);
    assertEquals("0001", tree.toString(cal));

    //test random
    cal.setTimeInMillis(random().nextLong());
    roundTrip(cal);
  }

  public void testToStringISO8601() {
    Calendar cal = tree.newCal();
    cal.setTimeInMillis(random().nextLong());
    //  create ZonedDateTime from the calendar, then get toInstant.toString which is the ISO8601 we emulate
    //   note: we don't simply init off of millisEpoch because of possible GregorianChangeDate discrepancy.
    int year = cal.get(Calendar.YEAR);
    if (cal.get(Calendar.ERA) == 0) { // BC
      year = -year + 1;
    }
    String expectedISO8601 =
        ZonedDateTime.of(year, cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH),
          cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND),
          cal.get(Calendar.MILLISECOND) * 1_000_000, ZoneOffset.UTC)
            .toInstant().toString();
    String resultToString = tree.toString(cal) + 'Z';
    assertEquals(expectedISO8601, resultToString);
  }

  //copies from DateRangePrefixTree
  private static final int[] CAL_FIELDS = {
      Calendar.YEAR, Calendar.MONTH, Calendar.DAY_OF_MONTH,
      Calendar.HOUR_OF_DAY, Calendar.MINUTE, Calendar.SECOND, Calendar.MILLISECOND};

  private void roundTrip(Calendar calOrig) throws ParseException {
    Calendar cal = (Calendar) calOrig.clone();
    String lastString = null;
    while (true) {
      String calString;
      {
        Calendar preToStringCalClone = (Calendar) cal.clone();
        calString = tree.toString(cal);
        assertEquals(preToStringCalClone, cal);//ensure toString doesn't modify cal state
      }

      //test parseCalendar
      assertEquals(cal, tree.parseCalendar(calString));

      //to Shape and back to Cal
      UnitNRShape shape = tree.toShape(cal);
      Calendar cal2 = tree.toCalendar(shape);
      assertEquals(calString, tree.toString(cal2));

      if (!calString.equals("*")) {//not world cell
        //to Term and back to Cell
        Cell cell = (Cell) shape;
        BytesRef term = cell.getTokenBytesNoLeaf(null);
        Cell cell2 = tree.readCell(BytesRef.deepCopyOf(term), null);
        assertEquals(calString, cell, cell2);
        Calendar cal3 = tree.toCalendar((UnitNRShape) cell2.getShape());
        assertEquals(calString, tree.toString(cal3));

        // setLeaf comparison
        cell2.setLeaf();
        BytesRef termLeaf = cell2.getTokenBytesWithLeaf(null);
        assertTrue(term.compareTo(termLeaf) < 0);
        assertEquals(termLeaf.length, term.length + 1);
        assertEquals(0, termLeaf.bytes[termLeaf.offset + termLeaf.length - 1]);
        assertTrue(cell.isPrefixOf(cell2));
      }

      //end of loop; decide if should loop again with lower precision
      final int calPrecField = tree.getCalPrecisionField(cal);
      if (calPrecField == -1)
        break;
      int fieldIdx = Arrays.binarySearch(CAL_FIELDS, calPrecField);
      assert fieldIdx >= 0;
      int prevPrecField = (fieldIdx == 0 ? -1 : CAL_FIELDS[--fieldIdx]);
      try {
        tree.clearFieldsAfter(cal, prevPrecField);
      } catch (AssertionError e) {
        if (e.getMessage().equals("Calendar underflow"))
          return;
        throw e;
      }
      lastString = calString;
    }
  }

  public void testShapeRelations() throws ParseException {
    //note: left range is 264000 at the thousand year level whereas right value is exact year
    assertEquals(SpatialRelation.WITHIN,
        tree.parseShape("[-264000 TO -264000-11-20]").relate(tree.parseShape("-264000")));

    Shape shapeA = tree.parseShape("[3122-01-23 TO 3122-11-27]");
    Shape shapeB = tree.parseShape("[3122-08 TO 3122-11]");
    assertEquals(SpatialRelation.INTERSECTS, shapeA.relate(shapeB));

    shapeA = tree.parseShape("3122");
    shapeB = tree.parseShape("[* TO 3122-10-31]");
    assertEquals(SpatialRelation.INTERSECTS, shapeA.relate(shapeB));

    shapeA = tree.parseShape("[3122-05-28 TO 3122-06-29]");
    shapeB = tree.parseShape("[3122 TO 3122-04]");
    assertEquals(SpatialRelation.DISJOINT, shapeA.relate(shapeB));
  }

  public void testShapeRangeOptimizer() throws ParseException {
    assertEquals("[2014-08 TO 2014-09]", tree.parseShape("[2014-08-01 TO 2014-09-30]").toString());

    assertEquals("2014", tree.parseShape("[2014-01-01 TO 2014-12-31]").toString());

    assertEquals("2014",    tree.parseShape("[2014-01 TO 2014]").toString());
    assertEquals("2014-01", tree.parseShape("[2014 TO 2014-01]").toString());
    assertEquals("2014-12", tree.parseShape("[2014-12 TO 2014]").toString());

    assertEquals("[2014 TO 2014-04-06]", tree.parseShape("[2014-01 TO 2014-04-06]").toString());

    assertEquals("*", tree.parseShape("[* TO *]").toString());

    assertEquals("2014-08-01", tree.parseShape("[2014-08-01 TO 2014-08-01]").toString());

    assertEquals("[2014 TO 2014-09-15]", tree.parseShape("[2014 TO 2014-09-15]").toString());

    assertEquals("[* TO 2014-09-15]", tree.parseShape("[* TO 2014-09-15]").toString());
  }

}