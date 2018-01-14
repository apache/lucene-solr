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
package org.apache.lucene.spatial.spatial4j.performance;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialTestCase;
import org.apache.lucene.spatial.composite.CompositeSpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.S2PrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.serialized.SerializedDVStrategy;
import org.apache.lucene.spatial.spatial4j.Geo3dSpatialContextFactory;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.spatial3d.geom.RandomGeo3dShapeGenerator;
import org.junit.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;

/**
 * Base class to test performance between geohash and s2.
 */
public abstract class Geo3dPerformanceRptTest extends SpatialTestCase {

  protected PlanetModel planetModel;
  protected RandomGeo3dShapeGenerator shapeGenerator;
  SpatialContext ctx;

  protected RecursivePrefixTreeStrategy s2RecursiveStrategy1;
  protected RecursivePrefixTreeStrategy s2RecursiveStrategy2;
  protected RecursivePrefixTreeStrategy s2RecursiveStrategy3;
  protected RecursivePrefixTreeStrategy geohashRecursiveStrategy;

  protected CompositeSpatialStrategy geohashStrategy;
  protected CompositeSpatialStrategy s2Strategy1;
  protected CompositeSpatialStrategy s2Strategy2;
  protected CompositeSpatialStrategy s2Strategy3;
  List<Shape> indexedShapes;
  List<Shape> queryShapes;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    shapeGenerator = new RandomGeo3dShapeGenerator();
    planetModel = shapeGenerator.randomPlanetModel();
    Geo3dSpatialContextFactory factory = new Geo3dSpatialContextFactory();
    factory.planetModel = planetModel;
    ctx = factory.newSpatialContext();

    int arity = 1;
    SpatialPrefixTree helper = new S2PrefixTree(ctx, S2PrefixTree.getMaxLevels(arity), arity);
    SpatialPrefixTree grid = new S2PrefixTree(ctx, helper.getLevelForDistance(precision()), arity);
    s2RecursiveStrategy1 = new RecursivePrefixTreeStrategy(grid,
        getClass().getSimpleName() + "_rpt");
    s2RecursiveStrategy1.setDistErrPct(distErrPct());
    s2RecursiveStrategy1.setPointsOnly(pointsOnly());
    s2RecursiveStrategy1.setPruneLeafyBranches(false);
    SerializedDVStrategy serializedDVStrategy = new SerializedDVStrategy(ctx, getClass().getSimpleName() + "_sdv");
    this.s2Strategy1 = new CompositeSpatialStrategy("composite_" + getClass().getSimpleName(),
        s2RecursiveStrategy1, serializedDVStrategy);

    arity = 2;
    SpatialPrefixTree helper2 = new S2PrefixTree(ctx, S2PrefixTree.getMaxLevels(arity), arity);
    SpatialPrefixTree grid2 = new S2PrefixTree(ctx, helper2.getLevelForDistance(precision()), arity);
    s2RecursiveStrategy2 = new RecursivePrefixTreeStrategy(grid2,
        getClass().getSimpleName() + "_rpt");
    s2RecursiveStrategy2.setDistErrPct(distErrPct());
    s2RecursiveStrategy2.setPointsOnly(pointsOnly());
    s2RecursiveStrategy2.setPruneLeafyBranches(false);
    SerializedDVStrategy serializedDVStrategy2 = new SerializedDVStrategy(ctx, getClass().getSimpleName() + "_sdv");
    this.s2Strategy2 = new CompositeSpatialStrategy("composite_" + getClass().getSimpleName(),
        s2RecursiveStrategy2, serializedDVStrategy2);

    arity = 3;
    SpatialPrefixTree helper3 = new S2PrefixTree(ctx, S2PrefixTree.getMaxLevels(arity), arity);
    SpatialPrefixTree grid3 = new S2PrefixTree(ctx, helper3.getLevelForDistance(precision()), arity);
    s2RecursiveStrategy3 = new RecursivePrefixTreeStrategy(grid3,
        getClass().getSimpleName() + "_rpt");
    s2RecursiveStrategy3.setDistErrPct(distErrPct());
    s2RecursiveStrategy3.setPointsOnly(pointsOnly());
    s2RecursiveStrategy3.setPruneLeafyBranches(false);
    SerializedDVStrategy serializedDVStrategy3 = new SerializedDVStrategy(ctx, getClass().getSimpleName() + "_sdv");
    this.s2Strategy3 = new CompositeSpatialStrategy("composite_" + getClass().getSimpleName(),
        s2RecursiveStrategy3, serializedDVStrategy3);

    helper = new GeohashPrefixTree(ctx, GeohashPrefixTree.getMaxLevelsPossible());
    grid = new GeohashPrefixTree(ctx, helper.getLevelForDistance(precision()));
    geohashRecursiveStrategy = new RecursivePrefixTreeStrategy(grid,
        getClass().getSimpleName() + "_rpt");
    geohashRecursiveStrategy.setDistErrPct(distErrPct());
    geohashRecursiveStrategy.setPointsOnly(pointsOnly());
    serializedDVStrategy = new SerializedDVStrategy(ctx, getClass().getSimpleName() + "_sdv");
    this.geohashStrategy = new CompositeSpatialStrategy("composite_" + getClass().getSimpleName(),
        geohashRecursiveStrategy, serializedDVStrategy);

    indexedShapes = new ArrayList<>(numberIndexedshapes());
    for (int i = 0; i< numberIndexedshapes(); i++) {
      indexedShapes.add(randomIndexedShape());
    }

    queryShapes = new ArrayList<>(numberQueryShapes());
    for (int i = 0; i< numberQueryShapes(); i++) {
      queryShapes.add(randomQueryShape());
    }
  }

  @Test
  @Repeat(iterations = 3)
  public void testOperations() throws IOException {

    testLoad("geohash", geohashStrategy);
    long geoHashHits = executeQueries("geohash", geohashStrategy);

    indexWriter.deleteAll();
    indexWriter.forceMergeDeletes(true);

    testLoad("s2 arity 1", s2Strategy1);
    long s2Hits1 = executeQueries("s2 arity 1", s2Strategy1);

    indexWriter.deleteAll();
    indexWriter.forceMergeDeletes(true);

    testLoad("s2 arity 2", s2Strategy2);
    long s2Hits2 = executeQueries("s2 arity 2", s2Strategy2);

    indexWriter.deleteAll();
    indexWriter.forceMergeDeletes(true);


    testLoad("s2 arity 3", s2Strategy3);
    long s2Hits3 = executeQueries("s2 arity 3", s2Strategy3);

    indexWriter.deleteAll();
    indexWriter.forceMergeDeletes(true);

    assertEquals(geoHashHits, s2Hits1);
    assertEquals(geoHashHits, s2Hits2);
    assertEquals(geoHashHits, s2Hits3);
  }

  protected void testLoad(String tree, CompositeSpatialStrategy strategy) throws IOException {

    //Main index loop:
    long start = System.currentTimeMillis();
    for (int i = 0; i < indexedShapes.size(); i++) {
      Shape shape = indexedShapes.get(i);
      Document aDoc = newDoc( ""+ i, shape, strategy);
      addDocument(aDoc);
    }
    commit();
    long end = System.currentTimeMillis();
    double shapesPerSecond = 1000.0 * numberIndexedshapes()/(end -start) ;
    DecimalFormat df = new DecimalFormat("#.00");
    System.out.println("load " + tree + " : " + df.format(shapesPerSecond) + " indexed shapes per second");
  }

  protected long executeQueries(String tree ,CompositeSpatialStrategy strategy) {
    long start = System.currentTimeMillis();
    long totalHits = 0;
    for (Shape shape : queryShapes) {
      Query query = strategy.makeQuery(new SpatialArgs(SpatialOperation.Intersects, shape));
      totalHits += executeQuery(query, 100, indexSearcher);
    }
    long end = System.currentTimeMillis();
    double queriesPerSecond = 1000.0 * numberQueryShapes() / (end -start);
    DecimalFormat df = new DecimalFormat("#.00");
    System.out.println("query " + tree + " : " + df.format(queriesPerSecond) + " queries per second");
    return totalHits;
  }

  protected long executeQuery(Query query, int numDocs, IndexSearcher indexSearcher) {
    try {
      return indexSearcher.search(query, numDocs).totalHits;
    } catch (IOException ioe) {
      throw new RuntimeException("IOException thrown while executing query", ioe);
    }
  }

  protected Document newDoc(String id, Shape shape, CompositeSpatialStrategy strategy) {
    Document doc = new Document();
    doc.add(new StringField("id", id, Field.Store.NO));
    if (shape != null) {
      for (Field f : strategy.createIndexableFields(shape)) {
        doc.add(f);
      }
    }
    return doc;
  }

  protected abstract int numberIndexedshapes();

  protected abstract Shape randomIndexedShape();

  protected abstract int numberQueryShapes();

  protected abstract Shape randomQueryShape();

  protected abstract boolean pointsOnly();

  protected abstract double precision();

  protected abstract double distErrPct();

}
