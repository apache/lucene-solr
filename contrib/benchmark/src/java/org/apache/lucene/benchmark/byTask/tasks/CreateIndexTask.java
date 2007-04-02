package org.apache.lucene.benchmark.byTask.tasks;

/**
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

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.utils.Config;


/**
 * Create an index.
 * <br>Other side effects: index writer object in perfRunData is set.
 * <br>Relevant properties: <code>merge.factor , max.buffered</code>.
 */
public class CreateIndexTask extends PerfTask {

  public CreateIndexTask(PerfRunData runData) {
    super(runData);
  }

  public int doLogic() throws IOException {
    Directory dir = getRunData().getDirectory();
    Analyzer analyzer = getRunData().getAnalyzer();
    
    IndexWriter iw = new IndexWriter(dir, analyzer, true);
    
    Config config = getRunData().getConfig();
    
    boolean cmpnd = config.get("compound",true);
    int mrgf = config.get("merge.factor",OpenIndexTask.DEFAULT_MERGE_PFACTOR);
    int mxbf = config.get("max.buffered",OpenIndexTask.DEFAULT_MAX_BUFFERED);

    iw.setUseCompoundFile(cmpnd);
    iw.setMergeFactor(mrgf);
    iw.setMaxBufferedDocs(mxbf);

    getRunData().setIndexWriter(iw);
    return 1;
  }

}
