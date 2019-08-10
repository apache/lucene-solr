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
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.common.util.JsonRecordReader;

@SolrTestCaseJ4.SuppressSSL
public class TestExportTool extends SolrCloudTestCase {

  public void testBasic() throws Exception {
    String COLLECTION_NAME = "globalLoaderColl";
    MiniSolrCloudCluster cluster = configureCluster(4)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();

    try {
      CollectionAdminRequest
          .createCollection(COLLECTION_NAME, "conf", 2, 1)
          .setMaxShardsPerNode(100)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLLECTION_NAME, 2, 2);

      String tmpFileLoc = new File(cluster.getBaseDir().toFile().getAbsolutePath() +
          File.separator).getPath();

      UpdateRequest ur = new UpdateRequest();
      ur.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
      int docCount = 1000;

      for (int i = 0; i < docCount; i++) {
        ur.add("id", String.valueOf(i), "desc_s", TestUtil.randomSimpleString(random(), 10, 50));
      }
      cluster.getSolrClient().request(ur, COLLECTION_NAME);

      QueryResponse qr = cluster.getSolrClient().query(COLLECTION_NAME, new SolrQuery("*:*").setRows(0));
      assertEquals(docCount, qr.getResults().getNumFound());

      String url = cluster.getRandomJetty(random()).getBaseUrl() + "/" + COLLECTION_NAME;


      ExportTool.Info info = new ExportTool.Info(url);

      String absolutePath = tmpFileLoc + COLLECTION_NAME + random().nextInt(100000) + ".json";
      info.setOutFormat(absolutePath, "jsonl");
      info.setLimit("200");
      info.exportDocsWithCursorMark();

      assertTrue(info.docsWritten >= 200);
      JsonRecordReader jsonReader = JsonRecordReader.getInst("/", Arrays.asList("$FQN:/**"));
      Reader rdr = new InputStreamReader(new FileInputStream( absolutePath), StandardCharsets.UTF_8);
      try {
        int[] count = new int[]{0};
        jsonReader.streamRecords(rdr, (record, path) -> count[0]++);
        assertTrue(count[0] >= 200);
      } finally {
        rdr.close();
      }


      info = new ExportTool.Info(url);
      absolutePath = tmpFileLoc + COLLECTION_NAME + random().nextInt(100000) + ".json";
      info.setOutFormat(absolutePath, "jsonl");
      info.setLimit("-1");
      info.exportDocsWithCursorMark();

      assertTrue(info.docsWritten >= 1000);
      jsonReader = JsonRecordReader.getInst("/", Arrays.asList("$FQN:/**"));
      rdr = new InputStreamReader(new FileInputStream( absolutePath), StandardCharsets.UTF_8);
      try {
        int[] count = new int[]{0};
        jsonReader.streamRecords(rdr, (record, path) -> count[0]++);
        assertTrue(count[0] >= 1000);
      } finally {
        rdr.close();
      }


      info = new ExportTool.Info(url);
      absolutePath = tmpFileLoc + COLLECTION_NAME + random().nextInt(100000) + ".javabin";
      info.setOutFormat(absolutePath, "javabin");
      info.setLimit("200");
      info.exportDocsWithCursorMark();
      assertTrue(info.docsWritten >= 200);

      FileInputStream fis = new FileInputStream(absolutePath);
      try {
        int[] count = new int[]{0};
        FastInputStream in = FastInputStream.wrap(fis);
        new JavaBinUpdateRequestCodec()
            .unmarshal(in, (document, req, commitWithin, override) -> count[0]++);
        assertTrue(count[0] >= 200);
      } finally {
        fis.close();
      }

      info = new ExportTool.Info(url);
      absolutePath = tmpFileLoc + COLLECTION_NAME + random().nextInt(100000) + ".javabin";
      info.setOutFormat(absolutePath, "javabin");
      info.setLimit("-1");
      info.exportDocsWithCursorMark();
      assertTrue(info.docsWritten >= 1000);

      fis = new FileInputStream(absolutePath);
      try {
        int[] count = new int[]{0};
        FastInputStream in = FastInputStream.wrap(fis);
        new JavaBinUpdateRequestCodec()
            .unmarshal(in, (document, req, commitWithin, override) -> count[0]++);
        assertTrue(count[0] >= 1000);
      } finally {
        fis.close();
      }

    } finally {
      cluster.shutdown();

    }
  }
}
