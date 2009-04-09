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

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.index.Term;

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

  ReopenThread t;
  float pauseSec = 3.0f;

  private static class ReopenThread extends Thread {

    final IndexWriter writer;
    final int pauseMsec;

    public volatile boolean done;

    ReopenThread(IndexWriter writer, float pauseSec) {
      this.writer = writer;
      this.pauseMsec = (int) (1000*pauseSec);
      setDaemon(true);
    }

    public void run() {

      IndexReader reader = null;

      final Query query = new TermQuery(new Term("body", "1"));
      final SortField sf = new SortField("docdate", SortField.LONG);
      final Sort sort = new Sort(sf);

      try {
        while(!done) {
          final long t0 = System.currentTimeMillis();
          if (reader == null) {
            reader = writer.getReader();
          } else {
            final IndexReader newReader = reader.reopen();
            if (reader != newReader) {
              reader.close();
              reader = newReader;
            }
          }

          final long t1 = System.currentTimeMillis();
          final TopFieldDocs hits = new IndexSearcher(reader).search(query, null, 10, sort);
          final long t2 = System.currentTimeMillis();
          System.out.println("nrt: open " + (t1-t0) + " msec; search " + (t2-t1) + " msec, " + hits.totalHits +
                             " results; " + reader.numDocs() + " docs");

          final long t4 = System.currentTimeMillis();
          final int delay = (int) (pauseMsec - (t4-t0));
          if (delay > 0) {
            try {
              Thread.sleep(delay);
            } catch (InterruptedException ie) {
              throw new RuntimeException(ie);
            }
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public NearRealtimeReaderTask(PerfRunData runData) {
    super(runData);
  }

  public int doLogic() throws IOException {
    if (t == null) {
      IndexWriter w = getRunData().getIndexWriter();
      t = new ReopenThread(w, pauseSec);
      t.start();
    }
    return 1;
  }

  public void setParams(String params) {
    super.setParams(params);
    pauseSec = Float.parseFloat(params);
  }

  public boolean supportsParams() {
    return true;
  }

  // Close the thread
  public void close() throws InterruptedException {
    if (t != null) {
      t.done = true;
      t.join();
    }
  }
}
