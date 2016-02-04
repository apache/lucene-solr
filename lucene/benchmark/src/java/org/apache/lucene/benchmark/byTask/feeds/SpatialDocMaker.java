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
package org.apache.lucene.benchmark.byTask.feeds;


import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.SpatialContextFactory;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.composite.CompositeSpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.PackedQuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTreeFactory;
import org.apache.lucene.spatial.serialized.SerializedDVStrategy;

/**
 * Indexes spatial data according to a configured {@link SpatialStrategy} with optional
 * shape transformation via a configured {@link ShapeConverter}. The converter can turn points into
 * circles and bounding boxes, in order to vary the type of indexing performance tests.
 * Unless it's subclass-ed to do otherwise, this class configures a {@link SpatialContext},
 * {@link SpatialPrefixTree}, and {@link RecursivePrefixTreeStrategy}. The Strategy is made
 * available to a query maker via the static method {@link #getSpatialStrategy(int)}.
 * See spatial.alg for a listing of spatial parameters, in particular those starting with "spatial."
 * and "doc.spatial".
 */
public class SpatialDocMaker extends DocMaker {

  public static final String SPATIAL_FIELD = "spatial";

  //cache spatialStrategy by round number
  private static Map<Integer,SpatialStrategy> spatialStrategyCache = new HashMap<>();

  private SpatialStrategy strategy;
  private ShapeConverter shapeConverter;

  /**
   * Looks up the SpatialStrategy from the given round --
   * {@link org.apache.lucene.benchmark.byTask.utils.Config#getRoundNumber()}. It's an error
   * if it wasn't created already for this round -- when SpatialDocMaker is initialized.
   */
  public static SpatialStrategy getSpatialStrategy(int roundNumber) {
    SpatialStrategy result = spatialStrategyCache.get(roundNumber);
    if (result == null) {
      throw new IllegalStateException("Strategy should have been init'ed by SpatialDocMaker by now");
    }
    return result;
  }

  /**
   * Builds a SpatialStrategy from configuration options.
   */
  protected SpatialStrategy makeSpatialStrategy(final Config config) {
    //A Map view of Config that prefixes keys with "spatial."
    Map<String, String> configMap = new AbstractMap<String, String>() {
      @Override
      public Set<Entry<String, String>> entrySet() {
        throw new UnsupportedOperationException();
      }

      @Override
      public String get(Object key) {
        return config.get("spatial." + key, null);
      }
    };

    SpatialContext ctx = SpatialContextFactory.makeSpatialContext(configMap, null);

    return makeSpatialStrategy(config, configMap, ctx);
  }

  protected SpatialStrategy makeSpatialStrategy(final Config config, Map<String, String> configMap,
                                                SpatialContext ctx) {
    //TODO once strategies have factories, we could use them here.
    final String strategyName = config.get("spatial.strategy", "rpt");
    switch (strategyName) {
      case "rpt": return makeRPTStrategy(SPATIAL_FIELD, config, configMap, ctx);
      case "composite": return makeCompositeStrategy(config, configMap, ctx);
      //TODO add more as-needed
      default: throw new IllegalStateException("Unknown spatial.strategy: " + strategyName);
    }
  }

  protected RecursivePrefixTreeStrategy makeRPTStrategy(String spatialField, Config config,
                                                        Map<String, String> configMap, SpatialContext ctx) {
    //A factory for the prefix tree grid
    SpatialPrefixTree grid = SpatialPrefixTreeFactory.makeSPT(configMap, null, ctx);

    RecursivePrefixTreeStrategy strategy = new RecursivePrefixTreeStrategy(grid, spatialField);
    strategy.setPointsOnly(config.get("spatial.docPointsOnly", false));
    final boolean pruneLeafyBranches = config.get("spatial.pruneLeafyBranches", true);
    if (grid instanceof PackedQuadPrefixTree) {
      ((PackedQuadPrefixTree) grid).setPruneLeafyBranches(pruneLeafyBranches);
      strategy.setPruneLeafyBranches(false);//always leave it to packed grid, even though it isn't the same
    } else {
      strategy.setPruneLeafyBranches(pruneLeafyBranches);
    }

    int prefixGridScanLevel = config.get("query.spatial.prefixGridScanLevel", -4);
    if (prefixGridScanLevel < 0)
      prefixGridScanLevel = grid.getMaxLevels() + prefixGridScanLevel;
    strategy.setPrefixGridScanLevel(prefixGridScanLevel);

    double distErrPct = config.get("spatial.distErrPct", .025);//doc & query; a default
    strategy.setDistErrPct(distErrPct);
    return strategy;
  }

  protected SerializedDVStrategy makeSerializedDVStrategy(String spatialField, Config config,
                                                          Map<String, String> configMap, SpatialContext ctx) {
    return new SerializedDVStrategy(ctx, spatialField);
  }

  protected SpatialStrategy makeCompositeStrategy(Config config, Map<String, String> configMap, SpatialContext ctx) {
    final CompositeSpatialStrategy strategy = new CompositeSpatialStrategy(
        SPATIAL_FIELD, makeRPTStrategy(SPATIAL_FIELD + "_rpt", config, configMap, ctx),
        makeSerializedDVStrategy(SPATIAL_FIELD + "_sdv", config, configMap, ctx)
    );
    strategy.setOptimizePredicates(config.get("query.spatial.composite.optimizePredicates", true));
    return strategy;
  }

  @Override
  public void setConfig(Config config, ContentSource source) {
    super.setConfig(config, source);
    SpatialStrategy existing = spatialStrategyCache.get(config.getRoundNumber());
    if (existing == null) {
      //new round; we need to re-initialize
      strategy = makeSpatialStrategy(config);
      spatialStrategyCache.put(config.getRoundNumber(), strategy);
      //TODO remove previous round config?
      shapeConverter = makeShapeConverter(strategy, config, "doc.spatial.");
      System.out.println("Spatial Strategy: " + strategy);
    }
  }

  /**
   * Optionally converts points to circles, and optionally bbox'es result.
   */
  public static ShapeConverter makeShapeConverter(final SpatialStrategy spatialStrategy,
                                                  Config config, String configKeyPrefix) {
    //by default does no conversion
    final double radiusDegrees = config.get(configKeyPrefix+"radiusDegrees", 0.0);
    final double plusMinus = config.get(configKeyPrefix+"radiusDegreesRandPlusMinus", 0.0);
    final boolean bbox = config.get(configKeyPrefix + "bbox", false);

    return new ShapeConverter() {
      @Override
      public Shape convert(Shape shape) {
        if (shape instanceof Point && (radiusDegrees != 0.0 || plusMinus != 0.0)) {
          Point point = (Point)shape;
          double radius = radiusDegrees;
          if (plusMinus > 0.0) {
            Random random = new Random(point.hashCode());//use hashCode so it's reproducibly random
            radius += random.nextDouble() * 2 * plusMinus - plusMinus;
            radius = Math.abs(radius);//can happen if configured plusMinus > radiusDegrees
          }
          shape = spatialStrategy.getSpatialContext().makeCircle(point, radius);
        }
        if (bbox)
          shape = shape.getBoundingBox();
        return shape;
      }
    };
  }

  /** Converts one shape to another. Created by
   * {@link #makeShapeConverter(org.apache.lucene.spatial.SpatialStrategy, org.apache.lucene.benchmark.byTask.utils.Config, String)} */
  public interface ShapeConverter {
    Shape convert(Shape shape);
  }

  @Override
  public Document makeDocument() throws Exception {

    DocState docState = getDocState();

    Document doc = super.makeDocument();

    // Set SPATIAL_FIELD from body
    DocData docData = docState.docData;
    //   makeDocument() resets docState.getBody() so we can't look there; look in Document
    String shapeStr = doc.getField(DocMaker.BODY_FIELD).stringValue();
    Shape shape = makeShapeFromString(strategy, docData.getName(), shapeStr);
    if (shape != null) {
      shape = shapeConverter.convert(shape);
      //index
      for (Field f : strategy.createIndexableFields(shape)) {
        doc.add(f);
      }
    }

    return doc;
  }

  public static Shape makeShapeFromString(SpatialStrategy strategy, String name, String shapeStr) {
    if (shapeStr != null && shapeStr.length() > 0) {
      try {
        return strategy.getSpatialContext().readShapeFromWkt(shapeStr);
      } catch (Exception e) {//InvalidShapeException TODO
        System.err.println("Shape "+name+" wasn't parseable: "+e+"  (skipping it)");
        return null;
      }
    }
    return null;
  }

  @Override
  public Document makeDocument(int size) throws Exception {
    //TODO consider abusing the 'size' notion to number of shapes per document
    throw new UnsupportedOperationException();
  }
}
