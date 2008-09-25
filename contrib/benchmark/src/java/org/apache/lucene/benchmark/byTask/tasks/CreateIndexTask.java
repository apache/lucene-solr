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
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergePolicy;

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

  public static void setIndexWriterConfig(IndexWriter writer, Config config) throws IOException {

    final String mergeScheduler = config.get("merge.scheduler",
                                             "org.apache.lucene.index.ConcurrentMergeScheduler");
    RuntimeException err = null;
    try {
      writer.setMergeScheduler((MergeScheduler) Class.forName(mergeScheduler).newInstance());
    } catch (IllegalAccessException iae) {
      err = new RuntimeException("unable to instantiate class '" + mergeScheduler + "' as merge scheduler");
      err.initCause(iae);
    } catch (InstantiationException ie) {
      err = new RuntimeException("unable to instantiate class '" + mergeScheduler + "' as merge scheduler");
      err.initCause(ie);
    } catch (ClassNotFoundException cnfe) {
      err = new RuntimeException("unable to load class '" + mergeScheduler + "' as merge scheduler");
      err.initCause(cnfe);
    }
    if (err != null)
      throw err;

    final String mergePolicy = config.get("merge.policy",
                                          "org.apache.lucene.index.LogByteSizeMergePolicy");
    err = null;
    try {
      writer.setMergePolicy((MergePolicy) Class.forName(mergePolicy).newInstance());
    } catch (IllegalAccessException iae) {
      err = new RuntimeException("unable to instantiate class '" + mergePolicy + "' as merge policy");
      err.initCause(iae);
    } catch (InstantiationException ie) {
      err = new RuntimeException("unable to instantiate class '" + mergePolicy + "' as merge policy");
      err.initCause(ie);
    } catch (ClassNotFoundException cnfe) {
      err = new RuntimeException("unable to load class '" + mergePolicy + "' as merge policy");
      err.initCause(cnfe);
    }
    if (err != null)
      throw err;

    writer.setUseCompoundFile(config.get("compound",true));
    writer.setMergeFactor(config.get("merge.factor",OpenIndexTask.DEFAULT_MERGE_PFACTOR));
    writer.setMaxFieldLength(config.get("max.field.length",OpenIndexTask.DEFAULT_MAX_FIELD_LENGTH));

    final double ramBuffer = config.get("ram.flush.mb",OpenIndexTask.DEFAULT_RAM_FLUSH_MB);
    final int maxBuffered = config.get("max.buffered",OpenIndexTask.DEFAULT_MAX_BUFFERED);
    if (maxBuffered == IndexWriter.DISABLE_AUTO_FLUSH) {
      writer.setRAMBufferSizeMB(ramBuffer);
      writer.setMaxBufferedDocs(maxBuffered);
    } else {
      writer.setMaxBufferedDocs(maxBuffered);
      writer.setRAMBufferSizeMB(ramBuffer);
    }
  }

  public int doLogic() throws IOException {
    PerfRunData runData = getRunData();
    Config config = runData.getConfig();
    IndexWriter writer = new IndexWriter(runData.getDirectory(),
                                         runData.getConfig().get("autocommit", OpenIndexTask.DEFAULT_AUTO_COMMIT),
                                         runData.getAnalyzer(),
                                         true);
    CreateIndexTask.setIndexWriterConfig(writer, config);
    runData.setIndexWriter(writer);
    return 1;
  }
}
