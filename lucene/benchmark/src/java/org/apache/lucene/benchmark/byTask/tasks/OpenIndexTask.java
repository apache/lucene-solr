package org.apache.lucene.benchmark.byTask.tasks;

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

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import java.io.IOException;

/**
 * Open an index writer.
 * <br>Other side effects: index writer object in perfRunData is set.
 * <br>Relevant properties: <code>merge.factor, max.buffered,
 * max.field.length, ram.flush.mb [default 0]</code>.
 *
 * <p> Accepts a param specifying the commit point as
 * previously saved with CommitIndexTask.  If you specify
 * this, it rolls the index back to that commit on opening
 * the IndexWriter.
 */
public class OpenIndexTask extends PerfTask {

  public static final int DEFAULT_MAX_BUFFERED = IndexWriterConfig.DEFAULT_MAX_BUFFERED_DOCS;
  public static final int DEFAULT_MERGE_PFACTOR = LogMergePolicy.DEFAULT_MERGE_FACTOR;
  public static final double DEFAULT_RAM_FLUSH_MB = (int) IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB;
  private String commitUserData;

  public OpenIndexTask(PerfRunData runData) {
    super(runData);
  }

  @Override
  public int doLogic() throws IOException {
    PerfRunData runData = getRunData();
    Config config = runData.getConfig();
    final IndexCommit ic;
    if (commitUserData != null) {
      ic = OpenReaderTask.findIndexCommit(runData.getDirectory(), commitUserData);
    } else {
      ic = null;
    }
    
    final IndexWriter writer = CreateIndexTask.configureWriter(config, runData, OpenMode.APPEND, ic);
    runData.setIndexWriter(writer);
    return 1;
  }

  @Override
  public void setParams(String params) {
    super.setParams(params);
    if (params != null) {
      // specifies which commit point to open
      commitUserData = params;
    }
  }

  @Override
  public boolean supportsParams() {
    return true;
  }
}
