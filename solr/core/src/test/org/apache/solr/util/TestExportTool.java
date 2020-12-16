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

package org.apache.solr.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.common.util.JsonRecordReader;

@SolrTestCaseJ4.SuppressSSL
public class TestExportTool extends SolrCloudTestCase {

  public void testBasic() throws Exception {
    String COLLECTION_NAME = "globalLoaderColl";
    configureCluster(4)
        .addConfig("conf", configset("cloud-dynamic"))
        .configure();

    try {
      CollectionAdminRequest
          .createCollection(COLLECTION_NAME, "conf", 2, 1)
          .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
          .setMaxShardsPerNode(100)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLLECTION_NAME, 2, 2);

      String tmpFileLoc = new File(cluster.getBaseDir().toFile().getAbsolutePath() +
          File.separator).getPath();

      UpdateRequest ur = new UpdateRequest();
      ur.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
      int docCount = 1000;

      for (int i = 0; i < docCount; i++) {
        ur.add("id", String.valueOf(i),
            "desc_s", TestUtil.randomSimpleString(random(), 10, 50) ,
            "a_dt", "2019-09-30T05:58:03Z");
      }
      cluster.getSolrClient().request(ur, COLLECTION_NAME);

      QueryResponse qr = cluster.getSolrClient().query(COLLECTION_NAME, new SolrQuery("*:*").setRows(0));
      assertEquals(docCount, qr.getResults().getNumFound());

      String url = cluster.getRandomJetty(random()).getBaseUrl() + "/" + COLLECTION_NAME;


      ExportTool.Info info = new ExportTool.MultiThreadedRunner(url);
      String absolutePath = tmpFileLoc + COLLECTION_NAME + random().nextInt(100000) + ".json";
      info.setOutFormat(absolutePath, "jsonl");
      info.setLimit("200");
      info.fields = "id,desc_s,a_dt";
      info.exportDocs();

      assertJsonDocsCount(info, 200, record -> "2019-09-30T05:58:03Z".equals(record.get("a_dt")));

      info = new ExportTool.MultiThreadedRunner(url);
      absolutePath = tmpFileLoc + COLLECTION_NAME + random().nextInt(100000) + ".json";
      info.setOutFormat(absolutePath, "jsonl");
      info.setLimit("-1");
      info.fields = "id,desc_s";
      info.exportDocs();

      assertJsonDocsCount(info, 1000,null);

      info = new ExportTool.MultiThreadedRunner(url);
      absolutePath = tmpFileLoc + COLLECTION_NAME + random().nextInt(100000) + ".javabin";
      info.setOutFormat(absolutePath, "javabin");
      info.setLimit("200");
      info.fields = "id,desc_s";
      info.exportDocs();

      assertJavabinDocsCount(info, 200);

      info = new ExportTool.MultiThreadedRunner(url);
      absolutePath = tmpFileLoc + COLLECTION_NAME + random().nextInt(100000) + ".javabin";
      info.setOutFormat(absolutePath, "javabin");
      info.setLimit("-1");
      info.fields = "id,desc_s";
      info.exportDocs();
      assertJavabinDocsCount(info, 1000);

    } finally {
      cluster.shutdown();

    }
  }

  @Nightly
  public void testVeryLargeCluster() throws Exception {
    String COLLECTION_NAME = "veryLargeColl";
    configureCluster(4)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();

    try {
      CollectionAdminRequest
          .createCollection(COLLECTION_NAME, "conf", 8, 1)
          .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
          .setMaxShardsPerNode(10)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLLECTION_NAME, 8, 8);

      String tmpFileLoc = new File(cluster.getBaseDir().toFile().getAbsolutePath() +
          File.separator).getPath();
      String url = cluster.getRandomJetty(random()).getBaseUrl() + "/" + COLLECTION_NAME;


      int docCount = 0;

      for (int j = 0; j < 4; j++) {
        int bsz = 10000;
        UpdateRequest ur = new UpdateRequest();
        ur.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
        for (int i = 0; i < bsz; i++) {
          ur.add("id", String.valueOf((j * bsz) + i), "desc_s", TestUtil.randomSimpleString(random(), 10, 50));
        }
        cluster.getSolrClient().request(ur, COLLECTION_NAME);
        docCount += bsz;
      }

      QueryResponse qr = cluster.getSolrClient().query(COLLECTION_NAME, new SolrQuery("*:*").setRows(0));
      assertEquals(docCount, qr.getResults().getNumFound());

      DocCollection coll = cluster.getSolrClient().getClusterStateProvider().getCollection(COLLECTION_NAME);
      HashMap<String, Long> docCounts = new HashMap<>();
      long totalDocsFromCores = 0;
      for (Slice slice : coll.getSlices()) {
        Replica replica = slice.getLeader();
        try (HttpSolrClient client = new HttpSolrClient.Builder(replica.getBaseUrl()).build()) {
          long count = ExportTool.getDocCount(replica.getCoreName(), client);
          docCounts.put(replica.getCoreName(), count);
          totalDocsFromCores += count;
        }
      }
      assertEquals(docCount, totalDocsFromCores);

      ExportTool.MultiThreadedRunner info = null;
      String absolutePath = null;

      info = new ExportTool.MultiThreadedRunner(url);
      info.output = System.out;
      absolutePath = tmpFileLoc + COLLECTION_NAME + random().nextInt(100000) + ".javabin";
      info.setOutFormat(absolutePath, "javabin");
      info.setLimit("-1");
      info.exportDocs();
      assertJavabinDocsCount(info, docCount);
      for (Map.Entry<String, Long> e : docCounts.entrySet()) {
        assertEquals(e.getValue().longValue(), info.corehandlers.get(e.getKey()).receivedDocs.get());
      }
      info = new ExportTool.MultiThreadedRunner(url);
      info.output = System.out;
      absolutePath = tmpFileLoc + COLLECTION_NAME + random().nextInt(100000) + ".json";
      info.setOutFormat(absolutePath, "jsonl");
      info.fields = "id,desc_s";
      info.setLimit("-1");
      info.exportDocs();
      long actual = ((ExportTool.JsonSink) info.sink).info.docsWritten.get();
      assertTrue("docs written :" + actual + "docs produced : " + info.docsWritten.get(), actual >= docCount);
      assertJsonDocsCount(info, docCount,null);
    } finally {
      cluster.shutdown();

    }
  }


  private void assertJavabinDocsCount(ExportTool.Info info, int expected) throws IOException {
    assertTrue("" + info.docsWritten.get() + " expected " + expected, info.docsWritten.get() >= expected);
    FileInputStream fis = new FileInputStream(info.out);
    try {
      int[] count = new int[]{0};
      FastInputStream in = FastInputStream.wrap(fis);
      new JavaBinUpdateRequestCodec()
          .unmarshal(in, (document, req, commitWithin, override) -> {
            assertEquals(2, document.size());
            count[0]++;
          });
      assertTrue(count[0] >= expected);
    } finally {
      fis.close();
    }
  }

    private void assertJsonDocsCount(ExportTool.Info info, int expected, Predicate<Map<String,Object>> predicate) throws IOException {
    assertTrue("" + info.docsWritten.get() + " expected " + expected, info.docsWritten.get() >= expected);

    JsonRecordReader jsonReader;
    Reader rdr;
    jsonReader = JsonRecordReader.getInst("/", Arrays.asList("$FQN:/**"));
    rdr = new InputStreamReader(new FileInputStream(info.out), StandardCharsets.UTF_8);
    try {
      int[] count = new int[]{0};
      jsonReader.streamRecords(rdr, (record, path) -> {
        if(predicate != null){
          assertTrue(predicate.test(record));
        }
        count[0]++;
      });
      assertTrue(count[0] >= expected);
    } finally {
      rdr.close();
    }
  }
}
