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

package org.apache.lucene.spatial.benchmark;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.tasks.PerfTask;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.spatial.base.query.SpatialArgs;
import org.apache.lucene.spatial.base.query.SpatialArgsParser;
import org.apache.lucene.spatial.strategy.SpatialFieldInfo;


public abstract class QueryShapeTask<T extends SpatialFieldInfo> extends PerfTask implements StrategyAware<T> {

  private SpatialArgs spatialArgs;

  public QueryShapeTask(PerfRunData runData) {
    super(runData);
  }

  @Override
  public void setup() {
    Config config = getRunData().getConfig();
    String rawQuery = config.get("query.shapequery", ""); // TODO (cmale) - Come up with default query
    this.spatialArgs = new SpatialArgsParser().parse(rawQuery, getSpatialContext());
  }

  @Override
  public int doLogic() throws Exception {
    Query query = createSpatialStrategy().makeQuery(spatialArgs, createFieldInfo());
    TopDocs topDocs = getRunData().getIndexSearcher().search(query, 10);
    System.out.println("Numfound: " + topDocs.totalHits);
    return 1;
  }
}
