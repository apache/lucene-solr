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
package org.apache.solr.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexReader.ReaderContext;
import org.apache.lucene.queries.function.DocValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.ReaderUtil;
import org.apache.noggit.ObjectBuilder;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TestRealTimeGet extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema12.xml");
  }

  /***
  @Test
  public void testGetRealtime() throws Exception {
    SolrQueryRequest sr1 = req("q","foo");
    IndexReader r1 = sr1.getCore().getRealtimeReader();

    assertU(adoc("id","1"));

    IndexReader r2 = sr1.getCore().getRealtimeReader();
    assertNotSame(r1, r2);
    int refcount = r2.getRefCount();

    // make sure a new reader wasn't opened
    IndexReader r3 = sr1.getCore().getRealtimeReader();
    assertSame(r2, r3);
    assertEquals(refcount+1, r3.getRefCount());

    assertU(commit());

    // this is not critical, but currently a commit does not refresh the reader
    // if nothing has changed
    IndexReader r4 = sr1.getCore().getRealtimeReader();
    assertEquals(refcount+2, r4.getRefCount());


    r1.decRef();
    r2.decRef();
    r3.decRef();
    r4.decRef();
    sr1.close();
  }
  ***/


  final ConcurrentHashMap<Integer,Long> model = new ConcurrentHashMap<Integer,Long>();
  Map<Integer,Long> committedModel = new HashMap<Integer,Long>();
  long snapshotCount;
  long committedModelClock;
  volatile int lastId;
  final String field = "val_l";

  @Test
  public void testStressGetRealtime() throws Exception {
    // update variables
    final int commitPercent = 10;
    final int softCommitPercent = 50; // what percent of the commits are soft
    final int deletePercent = 8;
    final int deleteByQueryPercent = 4;
    final int ndocs = 100;
    int nWriteThreads = 10;
    final int maxConcurrentCommits = 2;   // number of committers at a time... needed if we want to avoid commit errors due to exceeding the max

    // query variables
    final int percentRealtimeQuery = 0;   // realtime get is not implemented yet
    final AtomicLong operations = new AtomicLong(10000);  // number of query operations to perform in total
    int nReadThreads = 10;


    for (int i=0; i<ndocs; i++) {
      model.put(i, -1L);
    }
    committedModel.putAll(model);

    final AtomicInteger numCommitting = new AtomicInteger();

    List<Thread> threads = new ArrayList<Thread>();

    for (int i=0; i<nWriteThreads; i++) {
      Thread thread = new Thread("WRITER"+i) {
        Random rand = new Random(random.nextInt());

        @Override
        public void run() {
          while (operations.get() > 0) {
            int oper = rand.nextInt(100);
            int id = rand.nextInt(ndocs);
            Long val = model.get(id);
            long nextVal = Math.abs(val)+1;

            // set the lastId before we actually change it sometimes to try and
            // uncover more race conditions between writing and reading
            boolean before = random.nextBoolean();
            if (before) {
              lastId = id;
            }

            if (oper < commitPercent) {
              if (numCommitting.incrementAndGet() <= maxConcurrentCommits) {
                Map<Integer,Long> newCommittedModel;
                long version;

                synchronized(TestRealTimeGet.this) {
                  newCommittedModel = new HashMap<Integer,Long>(model);  // take a snapshot
                  version = snapshotCount++;
                }

                if (rand.nextInt(100) < softCommitPercent)
                  assertU(h.commit("softCommit","true"));
                else
                  assertU(commit());

                synchronized(TestRealTimeGet.this) {
                  // install this snapshot only if it's newer than the current one
                  if (version >= committedModelClock) {
                    committedModel = newCommittedModel;
                    committedModelClock = version;
                  }
                }
              }
              numCommitting.decrementAndGet();
            } else if (oper < commitPercent + deletePercent) {
              assertU("<delete><id>" + id + "</id></delete>");
              model.put(id, -nextVal);
            } else if (oper < commitPercent + deletePercent + deleteByQueryPercent) {
              assertU("<delete><query>id:" + id + "</query></delete>");
              model.put(id, -nextVal);
            } else {
              assertU(adoc("id",Integer.toString(id), field, Long.toString(nextVal)));
            }

            if (!before) {
              lastId = id;
            }
          }
        }
      };

      threads.add(thread);
    }


    for (int i=0; i<nReadThreads; i++) {
      Thread thread = new Thread("READER"+i) {
        Random rand = new Random(random.nextInt());

        @Override
        public void run() {
          while (operations.decrementAndGet() >= 0) {
            int oper = rand.nextInt(100);
            // bias toward a recently changed doc
            int id = rand.nextInt(100) < 25 ? lastId : rand.nextInt(ndocs);

            // when indexing, we update the index, then the model
            // so when querying, we should first check the model, and then the index

            boolean realTime = rand.nextInt(100) < percentRealtimeQuery;
            long val;

            if (realTime) {
              val = model.get(id);
            } else {
              synchronized(TestRealTimeGet.this) {
                val = committedModel.get(id);
              }
            }

            SolrQueryRequest sreq;
            if (realTime) {
              sreq = req("wt","json", "qt","/get", "ids",Integer.toString(id));
            } else {
              sreq = req("wt","json", "q","id:"+Integer.toString(id), "omitHeader","true");
            }

            try {
              String response = h.query(sreq);
              Map rsp = (Map)ObjectBuilder.fromJSON(response);
              List doclist = (List)(((Map)rsp.get("response")).get("docs"));
              if (doclist.size() == 0) {
                // there's no info we can get back with a delete, so not much we can check without further synchronization
              } else {
                assertEquals(1, doclist.size());
                long foundVal = (Long)(((Map)doclist.get(0)).get(field));
                assertTrue(foundVal >= Math.abs(val));
              }
            } catch (Exception e) {
              fail(e.toString());
            }
          }
        }
      };

      threads.add(thread);
    }


    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }
  }

}
