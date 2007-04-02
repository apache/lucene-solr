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
 * Open an index writer.
 * <br>Other side effects: index writer object in perfRunData is set.
 * <br>Relevant properties: <code>merge.factor , max.buffered</code>.
 */
public class OpenIndexTask extends PerfTask {

  public static final int DEFAULT_MAX_BUFFERED = 10;
  public static final int DEFAULT_MERGE_PFACTOR = 10;

  public OpenIndexTask(PerfRunData runData) {
    super(runData);
  }

  public int doLogic() throws IOException {
    Directory dir = getRunData().getDirectory();
    Analyzer analyzer = getRunData().getAnalyzer();
    IndexWriter writer = new IndexWriter(dir, analyzer, false);
    
    Config config = getRunData().getConfig();
    
    boolean cmpnd = config.get("compound",true);
    int mrgf = config.get("merge.factor",DEFAULT_MERGE_PFACTOR);
    int mxbf = config.get("max.buffered",DEFAULT_MAX_BUFFERED);

    // must update params for newly opened writer
    writer.setMaxBufferedDocs(mxbf);
    writer.setMergeFactor(mrgf);
    writer.setUseCompoundFile(cmpnd); // this one redundant?
    
    getRunData().setIndexWriter(writer);
    return 1;
  }

}
