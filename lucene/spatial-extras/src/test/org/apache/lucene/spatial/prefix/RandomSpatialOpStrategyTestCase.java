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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.locationtech.spatial4j.shape.Shape;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.StrategyTestCase;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomInt;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;

/** Base test harness, ideally for SpatialStrategy impls that have exact results
 * (not grid approximated), hence "not fuzzy".
 */
public abstract class RandomSpatialOpStrategyTestCase extends StrategyTestCase {

  //Note: this is partially redundant with StrategyTestCase.runTestQuery & testOperation

  protected void testOperationRandomShapes(final SpatialOperation operation) throws IOException {

    final int numIndexedShapes = randomIntBetween(1, 6);
    List<Shape> indexedShapes = new ArrayList<>(numIndexedShapes);
    for (int i = 0; i < numIndexedShapes; i++) {
      indexedShapes.add(randomIndexedShape());
    }

    final int numQueryShapes = atLeast(20);
    List<Shape> queryShapes = new ArrayList<>(numQueryShapes);
    for (int i = 0; i < numQueryShapes; i++) {
      queryShapes.add(randomQueryShape());
    }

    testOperation(operation, indexedShapes, queryShapes, true/*havoc*/);
  }

  protected void testOperation(final SpatialOperation operation,
                               List<Shape> indexedShapes, List<Shape> queryShapes, boolean havoc) throws IOException {
    //first show that when there's no data, a query will result in no results
    {
      Query query = strategy.makeQuery(new SpatialArgs(operation, randomQueryShape()));
      SearchResults searchResults = executeQuery(query, 1);
      assertEquals(0, searchResults.numFound);
    }

    //Main index loop:
    for (int i = 0; i < indexedShapes.size(); i++) {
      Shape shape = indexedShapes.get(i);
      adoc(""+i, shape);

      if (havoc && random().nextInt(10) == 0)
        commit();//intermediate commit, produces extra segments
    }
    if (havoc) {
      //delete some documents randomly
      for (int id = 0; id < indexedShapes.size(); id++) {
        if (random().nextInt(10) == 0) {
          deleteDoc(""+id);
          indexedShapes.set(id, null);
        }
      }
    }

    commit();

    //Main query loop:
    for (int queryIdx = 0; queryIdx < queryShapes.size(); queryIdx++) {
      final Shape queryShape = queryShapes.get(queryIdx);

      if (havoc)
        preQueryHavoc();

      //Generate truth via brute force:
      // We ensure true-positive matches (if the predicate on the raw shapes match
      //  then the search should find those same matches).
      Set<String> expectedIds = new LinkedHashSet<>();//true-positives
      for (int id = 0; id < indexedShapes.size(); id++) {
        Shape indexedShape = indexedShapes.get(id);
        if (indexedShape == null)
          continue;
        if (operation.evaluate(indexedShape, queryShape)) {
          expectedIds.add(""+id);
        }
      }

      //Search and verify results
      SpatialArgs args = new SpatialArgs(operation, queryShape);
      Query query = strategy.makeQuery(args);
      SearchResults got = executeQuery(query, 100);
      Set<String> remainingExpectedIds = new LinkedHashSet<>(expectedIds);
      for (SearchResult result : got.results) {
        String id = result.getId();
        if (!remainingExpectedIds.remove(id)) {
          fail("qIdx:" + queryIdx + " Shouldn't match", id, indexedShapes, queryShape, operation);
        }
      }
      if (!remainingExpectedIds.isEmpty()) {
        String id = remainingExpectedIds.iterator().next();
        fail("qIdx:" + queryIdx + " Should have matched", id, indexedShapes, queryShape, operation);
      }
    }
  }

  private void fail(String label, String id, List<Shape> indexedShapes, Shape queryShape, SpatialOperation operation) {
    fail("[" + operation + "] " + label
        + " I#" + id + ":" + indexedShapes.get(Integer.parseInt(id)) + " Q:" + queryShape);
  }

  protected void preQueryHavoc() {
    if (strategy instanceof RecursivePrefixTreeStrategy) {
      RecursivePrefixTreeStrategy rpts = (RecursivePrefixTreeStrategy) strategy;
      int scanLevel = randomInt(rpts.getGrid().getMaxLevels());
      rpts.setPrefixGridScanLevel(scanLevel);
    }
  }

  protected abstract Shape randomIndexedShape();

  protected abstract Shape randomQueryShape();
}
