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


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.locationtech.spatial4j.shape.Shape;

/**
 * Reads spatial data from the body field docs from an internally created {@link LineDocSource}.
 * It's parsed by {@link org.locationtech.spatial4j.context.SpatialContext#readShapeFromWkt(String)} (String)} and then
 * further manipulated via a configurable {@link SpatialDocMaker.ShapeConverter}. When using point
 * data, it's likely you'll want to configure the shape converter so that the query shapes actually
 * cover a region. The queries are all created and cached in advance. This query maker works in
 * conjunction with {@link SpatialDocMaker}.  See spatial.alg for a listing of options, in
 * particular the options starting with "query.".
 */
public class SpatialFileQueryMaker extends AbstractQueryMaker {
  protected SpatialStrategy strategy;
  protected double distErrPct;//NaN if not set
  protected SpatialOperation operation;
  protected boolean score;

  protected SpatialDocMaker.ShapeConverter shapeConverter;

  @Override
  public void setConfig(Config config) throws Exception {
    strategy = SpatialDocMaker.getSpatialStrategy(config.getRoundNumber());
    shapeConverter = SpatialDocMaker.makeShapeConverter(strategy, config, "query.spatial.");

    distErrPct = config.get("query.spatial.distErrPct", Double.NaN);
    operation = SpatialOperation.get(config.get("query.spatial.predicate", "Intersects"));
    score = config.get("query.spatial.score", false);

    super.setConfig(config);//call last, will call prepareQueries()
  }

  @Override
  protected Query[] prepareQueries() throws Exception {
    final int maxQueries = config.get("query.file.maxQueries", 1000);
    Config srcConfig = new Config(new Properties());
    srcConfig.set("docs.file", config.get("query.file", null));
    srcConfig.set("line.parser", config.get("query.file.line.parser", null));
    srcConfig.set("content.source.forever", "false");

    List<Query> queries = new ArrayList<>();
    LineDocSource src = new LineDocSource();
    try {
      src.setConfig(srcConfig);
      src.resetInputs();
      DocData docData = new DocData();
      for (int i = 0; i < maxQueries; i++) {
        docData = src.getNextDocData(docData);
        Shape shape = SpatialDocMaker.makeShapeFromString(strategy, docData.getName(), docData.getBody());
        if (shape != null) {
          shape = shapeConverter.convert(shape);
          queries.add(makeQueryFromShape(shape));
        } else {
          i--;//skip
        }
      }
    } catch (NoMoreDataException e) {
      //all-done
    } finally {
      src.close();
    }
    return queries.toArray(new Query[queries.size()]);
  }


  protected Query makeQueryFromShape(Shape shape) {
    SpatialArgs args = new SpatialArgs(operation, shape);
    if (!Double.isNaN(distErrPct))
      args.setDistErrPct(distErrPct);

    Query filterQuery = strategy.makeQuery(args);
    if (score) {
      //wrap with distance computing query
      DoubleValuesSource valueSource = strategy.makeDistanceValueSource(shape.getCenter());
      return new FunctionScoreQuery(filterQuery, valueSource);
    } else {
      return filterQuery; // assume constant scoring
    }
  }

}
