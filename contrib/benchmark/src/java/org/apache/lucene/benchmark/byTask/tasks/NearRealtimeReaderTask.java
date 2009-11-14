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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;

/**
 * Spawns a BG thread that periodically (defaults to 3.0
 * seconds, but accepts param in seconds) wakes up and asks
 * IndexWriter for a near real-time reader.  Then runs a
 * single query (body: 1) sorted by docdate, and prints
 * time to reopen and time to run the search.
 *
 * <b>NOTE</b>: this is very experimental at this point, and
 * subject to change.  It's also not generally usable, eg
 * you cannot change which query is executed.
 */
public class NearRealtimeReaderTask extends PerfTask {

  long pauseMSec = 3000L;

  public NearRealtimeReaderTask(PerfRunData runData) {
    super(runData);
  }

  @Override
  public int doLogic() throws Exception {

    final PerfRunData runData = getRunData();

    // Get initial reader
    IndexWriter w = runData.getIndexWriter();
    if (w == null) {
      throw new RuntimeException("please open the writer before invoking NearRealtimeReader");
    }

    if (runData.getIndexReader() != null) {
      throw new RuntimeException("please close the existing reader before invoking NearRealtimeReader");
    }
    
    long t = System.currentTimeMillis();
    IndexReader r = w.getReader();
    runData.setIndexReader(r);
    // Transfer our reference to runData
    r.decRef();

    // TODO: gather basic metrics for reporting -- eg mean,
    // stddev, min/max reopen latencies

    // Parent sequence sets stopNow
    int reopenCount = 0;
    while(!stopNow) {
      long waitForMsec = (long) (pauseMSec - (System.currentTimeMillis() - t));
      if (waitForMsec > 0) {
        Thread.sleep(waitForMsec);
      }

      t = System.currentTimeMillis();
      final IndexReader newReader = r.reopen();
      if (r != newReader) {
        // TODO: somehow we need to enable warming, here
        runData.setIndexReader(newReader);
        // Transfer our reference to runData
        newReader.decRef();
        r = newReader;
        reopenCount++;
      }
    }

    return reopenCount;
  }

  @Override
  public void setParams(String params) {
    super.setParams(params);
    pauseMSec = (long) (1000.0*Float.parseFloat(params));
  }

  @Override
  public boolean supportsParams() {
    return true;
  }
}
