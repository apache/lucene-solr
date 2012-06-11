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

import java.io.IOException;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.InfoStream;

/**
 * Close index writer.
 * <br>Other side effects: index writer object in perfRunData is nullified.
 * <br>Takes optional param "doWait": if false, then close(false) is called.
 */
public class CloseIndexTask extends PerfTask {

  public CloseIndexTask(PerfRunData runData) {
    super(runData);
  }

  boolean doWait = true;

  @Override
  public int doLogic() throws IOException {
    IndexWriter iw = getRunData().getIndexWriter();
    if (iw != null) {
      // If infoStream was set to output to a file, close it.
      InfoStream infoStream = iw.getConfig().getInfoStream();
      if (infoStream != null) {
        infoStream.close();
      }
      iw.close(doWait);
      getRunData().setIndexWriter(null);
    }
    return 1;
  }

  @Override
  public void setParams(String params) {
    super.setParams(params);
    doWait = Boolean.valueOf(params).booleanValue();
  }

  @Override
  public boolean supportsParams() {
    return true;
  }
}
