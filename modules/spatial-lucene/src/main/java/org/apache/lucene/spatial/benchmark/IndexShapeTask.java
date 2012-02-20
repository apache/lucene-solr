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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.spatial.base.shape.Shape;
import org.apache.lucene.spatial.strategy.SpatialFieldInfo;
import org.apache.lucene.spatial.strategy.SpatialStrategy;

import java.util.UUID;

public abstract class IndexShapeTask<T extends SpatialFieldInfo> extends PerfTask implements StrategyAware<T> {

  private ShapeGenerator shapeGenerator;
  private int numShapes;

  public IndexShapeTask(PerfRunData runData) {
    super(runData);
  }

  @Override
  public void setup() throws Exception {
    Config config = getRunData().getConfig();
    String shapeGeneratorName = config.get("index.shapegenerator", ""); // TODO (cmale) - Setup default shape generator
    shapeGenerator = (ShapeGenerator) Class.forName(shapeGeneratorName)
            .getConstructor(Config.class)
            .newInstance(config);
    numShapes = config.get("index.numshapes", 1);
  }

  @Override
  public int doLogic() throws Exception {
    SpatialStrategy<T> spatialStrategy = createSpatialStrategy();
    T fieldInfo = createFieldInfo();
    for (int i = 0; i < numShapes; i++) {
      Shape shape = shapeGenerator.generate();
      IndexableField[] fields = spatialStrategy.createFields(fieldInfo, shape, true, true);
      if (fields == null) {
        continue;
      }
      Document document = new Document();
      document.add(new Field("id",UUID.randomUUID().toString(),StringField.TYPE_STORED));
      for (IndexableField field : fields) {
        document.add(field);
      }
      getRunData().getIndexWriter().addDocument(document);
    }
    return 1;
  }
}
