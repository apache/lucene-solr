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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;

import java.io.IOException;


/**
 * Create an index.
 * <br>Other side effects: index writer object in perfRunData is set.
 * <br>Relevant properties: <code>merge.factor, max.buffered,
 *  max.field.length, ram.flush.mb [default 0], autocommit
 *  [default true]</code>.
 */
public class CreateIndexTask extends PerfTask {

  public CreateIndexTask(PerfRunData runData) {
    super(runData);
  }

  public int doLogic() throws IOException {
    Directory dir = getRunData().getDirectory();
    Analyzer analyzer = getRunData().getAnalyzer();
    
    Config config = getRunData().getConfig();
    
    boolean cmpnd = config.get("compound",true);
    int mrgf = config.get("merge.factor",OpenIndexTask.DEFAULT_MERGE_PFACTOR);
    int mxbf = config.get("max.buffered",OpenIndexTask.DEFAULT_MAX_BUFFERED);
    int mxfl = config.get("max.field.length",OpenIndexTask.DEFAULT_MAX_FIELD_LENGTH);
    double flushAtRAMUsage = config.get("ram.flush.mb",OpenIndexTask.DEFAULT_RAM_FLUSH_MB);
    boolean autoCommit = config.get("autocommit", OpenIndexTask.DEFAULT_AUTO_COMMIT);

    IndexWriter iw = new IndexWriter(dir, autoCommit, analyzer, true);
    
    iw.setUseCompoundFile(cmpnd);
    iw.setMergeFactor(mrgf);
    iw.setMaxFieldLength(mxfl);
    if (flushAtRAMUsage > 0)
      iw.setRAMBufferSizeMB(flushAtRAMUsage);
    else if (mxbf != 0)
      iw.setMaxBufferedDocs(mxbf);
    else
      throw new RuntimeException("either max.buffered or ram.flush.mb must be non-zero");
    getRunData().setIndexWriter(iw);
    return 1;
  }

}
