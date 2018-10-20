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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.spatial.StrategyTestCase;
import org.apache.lucene.spatial.prefix.NumberRangePrefixTreeStrategy.Facets;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.CellIterator;
import org.apache.lucene.spatial.prefix.tree.DateRangePrefixTree;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.UnitNRShape;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.spatial4j.shape.Shape;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomInt;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;

public class NumberRangeFacetsTest extends StrategyTestCase {

  DateRangePrefixTree tree;

  int randomCalWindowField;
  long randomCalWindowMs;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    tree = DateRangePrefixTree.INSTANCE;
    strategy = new NumberRangePrefixTreeStrategy(tree, "dateRange");
    Calendar tmpCal = tree.newCal();
    randomCalWindowField = randomIntBetween(1, Calendar.ZONE_OFFSET - 1);//we're not allowed to add zone offset
    tmpCal.add(randomCalWindowField, 2_000);
    randomCalWindowMs = Math.max(2000L, tmpCal.getTimeInMillis());
  }

  @Repeat(iterations = 20)
  @Test
  public void test() throws IOException {
    //generate test data
    List<Shape> indexedShapes = new ArrayList<>();
    final int numIndexedShapes = random().nextInt(15);
    for (int i = 0; i < numIndexedShapes; i++) {
      indexedShapes.add(randomShape());
    }

    //Main index loop:
    for (int i = 0; i < indexedShapes.size(); i++) {
      Shape shape = indexedShapes.get(i);
      adoc(""+i, shape);

      if (random().nextInt(10) == 0)
        commit();//intermediate commit, produces extra segments
    }

    //delete some documents randomly
    for (int id = 0; id < indexedShapes.size(); id++) {
      if (random().nextInt(10) == 0) {
        deleteDoc(""+id);
        indexedShapes.set(id, null);
      }
    }

    commit();

    //Main query loop:
    for (int queryIdx = 0; queryIdx < 10; queryIdx++) {
      preQueryHavoc();

      // We need to have a facet range window to do the facets between (a start time & end time). We randomly
      // pick a date, decide the level we want to facet on, and then pick a right end time that is up to 2 thousand
      // values later.
      int calFieldFacet = randomCalWindowField - 1;
      if (calFieldFacet > 1 && rarely()) {
        calFieldFacet--;
      }
      final Calendar leftCal = randomCalendar();
      leftCal.add(calFieldFacet, -1 * randomInt(1000));
      Calendar rightCal = (Calendar) leftCal.clone();
      rightCal.add(calFieldFacet, randomInt(2000));
      // Pick facet detail level based on cal field.
      int detailLevel = tree.getTreeLevelForCalendarField(calFieldFacet);
      if (detailLevel < 0) {//no exact match
        detailLevel = -1 * detailLevel;
      }

      //Randomly pick a filter/acceptDocs
      Bits topAcceptDocs = null;
      List<Integer> acceptFieldIds = new ArrayList<>();
      if (usually()) {
        //get all possible IDs into a list, random shuffle it, then randomly choose how many of the first we use to
        // replace the list.
        for (int i = 0; i < indexedShapes.size(); i++) {
          if (indexedShapes.get(i) == null) { // we deleted this one
            continue;
          }
          acceptFieldIds.add(i);
        }
        Collections.shuffle(acceptFieldIds, random());
        acceptFieldIds = acceptFieldIds.subList(0, randomInt(acceptFieldIds.size()));
        if (!acceptFieldIds.isEmpty()) {
          List<BytesRef> terms = new ArrayList<>();
          for (Integer acceptDocId : acceptFieldIds) {
            terms.add(new BytesRef(acceptDocId.toString()));
          }

          topAcceptDocs = searchForDocBits(new TermInSetQuery("id", terms));
        }
      }

      //Lets do it!
      NumberRangePrefixTree.NRShape facetRange = tree.toRangeShape(tree.toShape(leftCal), tree.toShape(rightCal));
      Facets facets = ((NumberRangePrefixTreeStrategy) strategy)
          .calcFacets(indexSearcher.getTopReaderContext(), topAcceptDocs, facetRange, detailLevel);

      //System.out.println("Q: " + queryIdx + " " + facets);

      //Verify results. We do it by looping over indexed shapes and reducing the facet counts.
      Shape facetShapeRounded = facetRange.roundToLevel(detailLevel);
      for (int indexedShapeId = 0; indexedShapeId < indexedShapes.size(); indexedShapeId++) {
        if (topAcceptDocs != null && !acceptFieldIds.contains(indexedShapeId)) {
          continue;// this doc was filtered out via acceptDocs
        }
        Shape indexedShape = indexedShapes.get(indexedShapeId);
        if (indexedShape == null) {//was deleted
          continue;
        }
        Shape indexedShapeRounded = ((NumberRangePrefixTree.NRShape) indexedShape).roundToLevel(detailLevel);
        if (!indexedShapeRounded.relate(facetShapeRounded).intersects()) { // no intersection at all
          continue;
        }
        // walk the cells
        final CellIterator cellIterator = tree.getTreeCellIterator(indexedShape, detailLevel);
        while (cellIterator.hasNext()) {
          Cell cell = cellIterator.next();
          if (!cell.getShape().relate(facetShapeRounded).intersects()) {
            cellIterator.remove();//no intersection; prune
            continue;
          }
          assert cell.getLevel() <= detailLevel;

          if (cell.getLevel() == detailLevel) {
            //count it
            UnitNRShape shape = (UnitNRShape) cell.getShape();
            final UnitNRShape parentShape = shape.getShapeAtLevel(detailLevel - 1);//get parent
            final Facets.FacetParentVal facetParentVal = facets.parents.get(parentShape);
            assertNotNull(facetParentVal);
            int index = shape.getValAtLevel(shape.getLevel());
            assertNotNull(facetParentVal.childCounts);
            assert facetParentVal.childCounts[index] > 0;
            facetParentVal.childCounts[index]--;

          } else if (cell.isLeaf()) {
            //count it, and remove/prune.
            if (cell.getLevel() < detailLevel - 1) {
              assert facets.topLeaves > 0;
              facets.topLeaves--;
            } else {
              UnitNRShape shape = (UnitNRShape) cell.getShape();
              final UnitNRShape parentShape = shape.getShapeAtLevel(detailLevel - 1);//get parent
              final Facets.FacetParentVal facetParentVal = facets.parents.get(parentShape);
              assertNotNull(facetParentVal);
              assert facetParentVal.parentLeaves > 0;
              facetParentVal.parentLeaves--;
            }

            cellIterator.remove();
          }
        }
      }
      // At this point; all counts should be down to zero.
      assertTrue(facets.topLeaves == 0);
      for (Facets.FacetParentVal facetParentVal : facets.parents.values()) {
        assertTrue(facetParentVal.parentLeaves == 0);
        if (facetParentVal.childCounts != null) {
          for (int childCount : facetParentVal.childCounts) {
            assertTrue(childCount == 0);
          }
        }
      }

    }
  }

  private Bits searchForDocBits(Query query) throws IOException {
    FixedBitSet bitSet = new FixedBitSet(indexSearcher.getIndexReader().maxDoc());
    indexSearcher.search(query,
        new SimpleCollector() {
          int leafDocBase;
          @Override
          public void collect(int doc) throws IOException {
            bitSet.set(leafDocBase + doc);
          }

          @Override
          protected void doSetNextReader(LeafReaderContext context) throws IOException {
            leafDocBase = context.docBase;
          }

          @Override
          public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
          }
        });
    return bitSet;
  }

  private void preQueryHavoc() {
    if (strategy instanceof RecursivePrefixTreeStrategy) {
      RecursivePrefixTreeStrategy rpts = (RecursivePrefixTreeStrategy) strategy;
      int scanLevel = randomInt(rpts.getGrid().getMaxLevels());
      rpts.setPrefixGridScanLevel(scanLevel);
    }
  }

  protected Shape randomShape() {
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
