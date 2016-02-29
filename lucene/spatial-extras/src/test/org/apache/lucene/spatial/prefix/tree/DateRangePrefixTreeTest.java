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
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;

import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.UnitNRShape;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class DateRangePrefixTreeTest extends LuceneTestCase {

  private DateRangePrefixTree tree = DateRangePrefixTree.INSTANCE;

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

  //copies from DateRangePrefixTree
  private static final int[] CAL_FIELDS = {
      Calendar.YEAR, Calendar.MONTH, Calendar.DAY_OF_MONTH,
      Calendar.HOUR_OF_DAY, Calendar.MINUTE, Calendar.SECOND, Calendar.MILLISECOND};

  private void roundTrip(Calendar calOrig) throws ParseException {
    Calendar cal = (Calendar) calOrig.clone();
    String lastString = null;
    while (true) {
      String calString = tree.toString(cal);
      assert lastString == null || calString.length() < lastString.length();
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