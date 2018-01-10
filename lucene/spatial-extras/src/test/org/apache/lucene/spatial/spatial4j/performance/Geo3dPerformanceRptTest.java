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
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.google.common.geometry.S2;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.spatial.SpatialTestCase;
import org.apache.lucene.spatial.StrategyTestCase;
import org.apache.lucene.spatial.composite.CompositeSpatialStrategy;
import org.apache.lucene.spatial.prefix.RandomSpatialOpStrategyTestCase;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.S2PrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.serialized.SerializedDVStrategy;
import org.apache.lucene.spatial.spatial4j.Geo3dPointShape;
import org.apache.lucene.spatial.spatial4j.Geo3dShape;
import org.apache.lucene.spatial.spatial4j.Geo3dSpatialContextFactory;
import org.apache.lucene.spatial3d.geom.GeoAreaShape;
import org.apache.lucene.spatial3d.geom.GeoPointShape;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.spatial3d.geom.RandomGeo3dShapeGenerator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;

public abstract class Geo3dPerformanceRptTest extends SpatialTestCase {

  protected PlanetModel planetModel;
  protected RandomGeo3dShapeGenerator shapeGenerator;
  SpatialContext ctx;

  protected RecursivePrefixTreeStrategy s2RecursiveStrategy;
  protected RecursivePrefixTreeStrategy geohashRecursiveStrategy;

  protected CompositeSpatialStrategy geohashStrategy;
  protected CompositeSpatialStrategy s2Strategy;
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

    SpatialPrefixTree helper = new S2PrefixTree(ctx, S2PrefixTree.MAX_LEVELS);
    SpatialPrefixTree grid = new S2PrefixTree(ctx, helper.getLevelForDistance(precision()));
    s2RecursiveStrategy = new RecursivePrefixTreeStrategy(grid,
        getClass().getSimpleName() + "_rpt");
    s2RecursiveStrategy.setDistErrPct(distErrPct());
    s2RecursiveStrategy.setPointsOnly(pointsOnly());
    s2RecursiveStrategy.setPruneLeafyBranches(false);
    SerializedDVStrategy serializedDVStrategy = new SerializedDVStrategy(ctx, getClass().getSimpleName() + "_sdv");
    this.s2Strategy = new CompositeSpatialStrategy("composite_" + getClass().getSimpleName(),
        s2RecursiveStrategy, serializedDVStrategy);

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

    testLoad("s2", s2Strategy);
    long s2Hits = executeQueries("s2", s2Strategy);

    indexWriter.deleteAll();
    indexWriter.forceMergeDeletes(true);

    assertEquals(geoHashHits, s2Hits);
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
    printTime("load " + tree, start, end );
  }

  protected long executeQueries(String tree ,CompositeSpatialStrategy strategy) {
    long start = System.currentTimeMillis();
    long totalHits = 0;
    for (Shape shape : queryShapes) {
      Query query = strategy.makeQuery(new SpatialArgs(SpatialOperation.Intersects, shape));
      totalHits += executeQuery(query, 100, indexSearcher);
    }
    long end = System.currentTimeMillis();
    printTime("query " + tree, start, end );
    return totalHits;
  }

  protected long executeQuery(Query query, int numDocs, IndexSearcher indexSearcher) {
    try {
      return indexSearcher.search(query, numDocs).totalHits;
    } catch (IOException ioe) {
      throw new RuntimeException("IOException thrown while executing query", ioe);
    }
  }

  private void printTime(String process, long start, long end) {
    System.out.println(process + " took " + (end - start) + " ms");
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
