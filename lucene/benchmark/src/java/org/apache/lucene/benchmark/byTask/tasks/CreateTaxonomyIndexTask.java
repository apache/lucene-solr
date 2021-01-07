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
package org.apache.lucene.benchmark.byTask.tasks;

import java.io.IOException;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;

/**
 * Create a taxonomy index. <br>
 * Other side effects: taxonomy writer object in perfRunData is set.
 */
public class CreateTaxonomyIndexTask extends PerfTask {

  public CreateTaxonomyIndexTask(PerfRunData runData) {
    super(runData);
  }

  @Override
  public int doLogic() throws IOException {
    PerfRunData runData = getRunData();
    runData.setTaxonomyWriter(
        new DirectoryTaxonomyWriter(runData.getTaxonomyDir(), OpenMode.CREATE));
    return 1;
  }
}
