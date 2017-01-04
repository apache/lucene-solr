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
package org.apache.solr.schema;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.beans.Field;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;

import java.io.File;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;

@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class TestBinaryField extends SolrJettyTestBase {

  @BeforeClass
  public static void beforeTest() throws Exception {
    File homeDir = createTempDir().toFile();

    File collDir = new File(homeDir, "collection1");
    File dataDir = new File(collDir, "data");
    File confDir = new File(collDir, "conf");

    homeDir.mkdirs();
    collDir.mkdirs();
    dataDir.mkdirs();
    confDir.mkdirs();

    FileUtils.copyFile(new File(SolrTestCaseJ4.TEST_HOME(), "solr.xml"), new File(homeDir, "solr.xml"));

    String src_dir = TEST_HOME() + "/collection1/conf";
    FileUtils.copyFile(new File(src_dir, "schema-binaryfield.xml"), 
                       new File(confDir, "schema.xml"));
    FileUtils.copyFile(new File(src_dir, "solrconfig-basic.xml"), 
                       new File(confDir, "solrconfig.xml"));
    FileUtils.copyFile(new File(src_dir, "solrconfig.snippet.randomindexconfig.xml"), 
                       new File(confDir, "solrconfig.snippet.randomindexconfig.xml"));

    try (Writer w = new OutputStreamWriter(Files.newOutputStream(collDir.toPath().resolve("core.properties")), StandardCharsets.UTF_8)) {
      Properties coreProps = new Properties();
      coreProps.put("name", "collection1");
      coreProps.store(w, "");
    }

    createJetty(homeDir.getAbsolutePath());
  }


  public void testSimple() throws Exception {
    try (SolrClient client = getSolrClient()) {
      byte[] buf = new byte[10];
      for (int i = 0; i < 10; i++) {
        buf[i] = (byte) i;
      }
      SolrInputDocument doc = null;
      doc = new SolrInputDocument();
      doc.addField("id", 1);
      doc.addField("data", ByteBuffer.wrap(buf, 2, 5));
      client.add(doc);

      doc = new SolrInputDocument();
      doc.addField("id", 2);
      doc.addField("data", ByteBuffer.wrap(buf, 4, 3));
      client.add(doc);

      doc = new SolrInputDocument();
      doc.addField("id", 3);
      doc.addField("data", buf);
      client.add(doc);

      client.commit();

      QueryResponse resp = client.query(new SolrQuery("*:*"));
      SolrDocumentList res = resp.getResults();
      List<Bean> beans = resp.getBeans(Bean.class);
      assertEquals(3, res.size());
      assertEquals(3, beans.size());
      for (SolrDocument d : res) {

        Integer id = (Integer) d.getFieldValue("id");
        byte[] data = (byte[]) d.getFieldValue("data");
        if (id == 1) {
          assertEquals(5, data.length);
          for (int i = 0; i < data.length; i++) {
            byte b = data[i];
            assertEquals((byte) (i + 2), b);
          }

        } else if (id == 2) {
          assertEquals(3, data.length);
          for (int i = 0; i < data.length; i++) {
            byte b = data[i];
            assertEquals((byte) (i + 4), b);
          }


        } else if (id == 3) {
          assertEquals(10, data.length);
          for (int i = 0; i < data.length; i++) {
            byte b = data[i];
            assertEquals((byte) i, b);
          }

        }

      }
      for (Bean d : beans) {
        Integer id = d.id;
        byte[] data = d.data;
        if (id == 1) {
          assertEquals(5, data.length);
          for (int i = 0; i < data.length; i++) {
            byte b = data[i];
            assertEquals((byte) (i + 2), b);
          }

        } else if (id == 2) {
          assertEquals(3, data.length);
          for (int i = 0; i < data.length; i++) {
            byte b = data[i];
            assertEquals((byte) (i + 4), b);
          }


        } else if (id == 3) {
          assertEquals(10, data.length);
          for (int i = 0; i < data.length; i++) {
            byte b = data[i];
            assertEquals((byte) i, b);
          }

        }

      }
    }

  }
  public static class Bean{
    @Field
    int id;
    @Field
    byte [] data;
  }

}
