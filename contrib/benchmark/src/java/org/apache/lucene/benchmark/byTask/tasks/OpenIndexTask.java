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

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogMergePolicy;

import java.io.IOException;


/**
 * Open an index writer.
 * <br>Other side effects: index writer object in perfRunData is set.
 * <br>Relevant properties: <code>merge.factor, max.buffered,
 * max.field.length, ram.flush.mb [default 0], autocommit
 * [default true]</code>.
 */
public class OpenIndexTask extends PerfTask {

  public static final int DEFAULT_MAX_BUFFERED = IndexWriter.DEFAULT_MAX_BUFFERED_DOCS;
  public static final int DEFAULT_MAX_FIELD_LENGTH = IndexWriter.DEFAULT_MAX_FIELD_LENGTH;
  public static final int DEFAULT_MERGE_PFACTOR = LogMergePolicy.DEFAULT_MERGE_FACTOR;
  public static final double DEFAULT_RAM_FLUSH_MB = (int) IndexWriter.DEFAULT_RAM_BUFFER_SIZE_MB;
  public static final boolean DEFAULT_AUTO_COMMIT = true;

  public OpenIndexTask(PerfRunData runData) {
    super(runData);
  }

  public int doLogic() throws IOException {
    PerfRunData runData = getRunData();
    Config config = runData.getConfig();
    IndexWriter writer = new IndexWriter(runData.getDirectory(),
                                         config.get("autocommit", DEFAULT_AUTO_COMMIT),
                                         runData.getAnalyzer(),
                                         false);
    CreateIndexTask.setIndexWriterConfig(writer, config);
    runData.setIndexWriter(writer);
    return 1;
  }
}
