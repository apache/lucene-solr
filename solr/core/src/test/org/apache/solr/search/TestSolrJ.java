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
package org.apache.solr.search;


import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.RTimer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;


public class TestSolrJ extends SolrTestCaseJ4 {

  public void testSolrJ() throws Exception {
                          // docs, producers, connections, sleep_time
    //  main(new String[] {"1000000","4", "1", "0"});

    // doCommitPerf();
  }

  public static SolrClient client;
  public static String idField = "id";
  public static Exception ex;

  public static void main(String[] args) throws Exception {
    // String addr = "http://odin.local:80/solr";
    // String addr = "http://odin.local:8983/solr";
    String addr = "http://127.0.0.1:8983/solr";

    int i = 0;
    final int nDocs = Integer.parseInt(args[i++]);
    final int nProducers = Integer.parseInt(args[i++]);
    final int nConnections = Integer.parseInt(args[i++]);
    final int maxSleep = Integer.parseInt(args[i++]);

    ConcurrentUpdateSolrClient concurrentClient = null;

    // server = concurrentClient = new ConcurrentUpdateSolrServer(addr,32,8);
    client = concurrentClient = getConcurrentUpdateSolrClient(addr,64,nConnections);

    client.deleteByQuery("*:*");
    client.commit();

    final RTimer timer = new RTimer();

    final int docsPerThread = nDocs / nProducers;

    Thread[] threads = new Thread[nProducers];

    for (int threadNum = 0; threadNum<nProducers; threadNum++) {
      final int base = threadNum * docsPerThread;

      threads[threadNum] = new Thread("add-thread"+i) {
        @Override
        public void run(){
          try {
            indexDocs(base, docsPerThread, maxSleep);
          } catch (Exception e) {
            System.out.println("###############################CAUGHT EXCEPTION");
            e.printStackTrace();
            ex = e;
          }
        }
      };
      threads[threadNum].start();
    }

    // optional: wait for commit?

    for (int threadNum = 0; threadNum<nProducers; threadNum++) {
      threads[threadNum].join();
    }

    if (concurrentClient != null) {
      concurrentClient.blockUntilFinished();
    }

    double elapsed = timer.getTime();
    System.out.println("time="+elapsed + " throughput="+(nDocs*1000/elapsed) + " Exception="+ex);

    // should server threads be marked as daemon?
    // need a server.close()!!!
  }

  @SuppressWarnings({"unchecked"})
  public static SolrInputDocument getDocument(int docnum) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField(idField, docnum );
    doc.setField("cat", Integer.toString(docnum&0x0f) );
    doc.setField("name", "my name is " + Integer.toString(docnum&0xff) );
    doc.setField("foo_t", "now is the time for all good men to come to the aid of their country" );
    doc.setField("foo_i", Integer.toString(docnum&0x0f) );
    doc.setField("foo_s", Integer.toString(docnum&0xff) );
    doc.setField("foo_b", Boolean.toString( (docnum&0x01) == 1) );
    doc.setField("parent_s", Integer.toString(docnum-1) );
    doc.setField("price", Integer.toString(docnum >> 4));

    int golden = (int)2654435761L;
    int h = docnum * golden;
    int n = (h & 0xff) + 1;
    @SuppressWarnings({"rawtypes"})
    List lst = new ArrayList(n);
    for (int i=0; i<n; i++) {
      h = (h+i) * golden;
      lst.add(h & 0xfff);
    }

    doc.setField("num_is", lst);
    return doc;
  }

  public static void indexDocs(int base, int count, int maxSleep) throws IOException, SolrServerException {
    Random r = new Random(base);

    for (int i=base; i<count+base; i++) {
      if ((i & 0xfffff) == 0) {
        System.out.print("\n% " + new Date()+ "\t" + i + "\t");
        System.out.flush();
      }

      if ((i & 0xffff) == 0) {
        System.out.print(".");
        System.out.flush();
      }

      SolrInputDocument doc = getDocument(i);
      client.add(doc);

      if (maxSleep > 0) {
        int sleep = r.nextInt(maxSleep);
        try {
          Thread.sleep(sleep);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }

    }
  }


  public void doCommitPerf() throws Exception {

    try (HttpSolrClient client = getHttpSolrClient("http://127.0.0.1:8983/solr")) {

      final RTimer timer = new RTimer();

      for (int i = 0; i < 10000; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", Integer.toString(i % 13));
        client.add(doc);
        client.commit(true, true, true);
      }

      System.out.println("TIME: " + timer.getTime());
    }

  }

}
