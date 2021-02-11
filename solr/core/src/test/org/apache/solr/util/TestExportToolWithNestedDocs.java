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
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.solr.SolrTestCaseHS;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.JsonRecordReader;
import org.junit.After;
import org.junit.BeforeClass;

@SolrTestCaseJ4.SuppressSSL
public class TestExportToolWithNestedDocs extends SolrCloudTestCase {
  
  public static final String ANON_KIDS_CONFIG = "anon_kids_configset";
  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
      // when indexing 'anonymous' kids, we need a schema that doesn't use _nest_path_ so
      // that we can use [child] transformer with a parentFilter...
      .addConfig(ANON_KIDS_CONFIG, new File(ExternalPaths.TECHPRODUCTS_CONFIGSET).toPath())
      .configure();
  }

  @After
  public void cleanCollections() throws Exception {
    cluster.deleteAllCollections();
  }

  /**
   * Syntactic sugar so code snippet doesn't refer to test-framework specific method name 
   */
  public static SolrClient getSolrClient() {
    return cluster.getSolrClient();
  }

  /**
   * This test is inspired by the IndexingNestedDocuments.java unit test that 
   * demonstrates creating Anonymous Children docs, and then confirming the
   * export format.
   */
  public void testIndexingAnonKids() throws Exception {
    final String COLLECTION_NAME = "test_anon";
    CollectionAdminRequest.createCollection(COLLECTION_NAME, ANON_KIDS_CONFIG, 1, 1)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .process(cluster.getSolrClient());
    cluster.getSolrClient().setDefaultCollection(COLLECTION_NAME);
    
    final SolrClient client = getSolrClient();

    final SolrInputDocument p1 = new SolrInputDocument();
    p1.setField("id", "P11!prod");
    p1.setField("type_s", "PRODUCT");
    p1.setField("name_s", "Swingline Stapler");
    p1.setField("description_t", "The Cadillac of office staplers ...");
    {
      final SolrInputDocument s1 = new SolrInputDocument();
      s1.setField("id", "P11!S21");
      s1.setField("type_s", "SKU");
      s1.setField("color_s", "RED");
      s1.setField("price_i", 42);
      { 
        final SolrInputDocument m1 = new SolrInputDocument();
        m1.setField("id", "P11!D41");
        m1.setField("type_s", "MANUAL");
        m1.setField("name_s", "Red Swingline Brochure");
        m1.setField("pages_i", 1);
        m1.setField("content_t", "...");

        s1.addChildDocument(m1);
      }

      final SolrInputDocument s2 = new SolrInputDocument();
      s2.setField("id", "P11!S31");
      s2.setField("type_s", "SKU");
      s2.setField("color_s", "BLACK");
      s2.setField("price_i", 3);

      final SolrInputDocument m1 = new SolrInputDocument();
      m1.setField("id", "P11!D51");
      m1.setField("type_s", "MANUAL");
      m1.setField("name_s", "Quick Reference Guide");
      m1.setField("pages_i", 1);
      m1.setField("content_t", "How to use your stapler ...");

      final SolrInputDocument m2 = new SolrInputDocument();
      m2.setField("id", "P11!D61");
      m2.setField("type_s", "MANUAL");
      m2.setField("name_s", "Warranty Details");
      m2.setField("pages_i", 42);
      m2.setField("content_t", "... lifetime guarantee ...");

      p1.addChildDocuments(Arrays.asList(s1, s2, m1, m2));
    }
    
    client.add(p1);
    
    client.commit();

    
    String url = cluster.getRandomJetty(random()).getBaseUrl() + "/" + COLLECTION_NAME;
    
    String tmpFileLoc = new File(cluster.getBaseDir().toFile().getAbsolutePath() +
        File.separator).getPath();
    
    ExportTool.Info info = new ExportTool.MultiThreadedRunner(url);
    String absolutePath = tmpFileLoc + COLLECTION_NAME + random().nextInt(100000) + ".json";
    info.setOutFormat(absolutePath, "json");
    info.setLimit("200");
    info.query = "description_t:Cadillac";
    info.fields = "*,[child parentFilter='type_s:PRODUCT']";
    info.exportDocs();
    
    assertEquals(1, info.docsWritten.get());
    
    assertJsonDocsCount(info, 1, record -> "P11!prod".equals(record.get("id")));
    
    String jsonOutput = Files.readString(new File(info.out).toPath());
    
    
    SolrTestCaseHS.matchJSON(jsonOutput, 
        "//id=='P11!prod'", 
        "//type_s==PRODUCT", 
        "//name_s=='Swingline Stapler'",
        "//id=='P11!prod'/_childDocuments_/[1]/id=='P11!D41'"
        );

  }

  /**
   * This test is inspired by the IndexingNestedDocuments.java unit test that 
   * demonstrates using <code>NestPath</code> related psuedo-fields when indexing hierarchical documents
   * and then confirms the export format.
   */
  public void testIndexingUsingNestPath() throws Exception {
    final String COLLECTION_NAME = "test_anon";
    CollectionAdminRequest.createCollection(COLLECTION_NAME, 1, 1).process(cluster.getSolrClient());
    cluster.getSolrClient().setDefaultCollection(COLLECTION_NAME);
   
    final SolrClient client = getSolrClient();

    final SolrInputDocument p1 = new SolrInputDocument();
    p1.setField("id", "P11!prod");
    p1.setField("name_s", "Swingline Stapler");
    p1.setField("description_t", "The Cadillac of office staplers ...");
    {
      final SolrInputDocument s1 = new SolrInputDocument();
      s1.setField("id", "P11!S21");
      s1.setField("color_s", "RED");
      s1.setField("price_i", 42);
      { 
        final SolrInputDocument m1 = new SolrInputDocument();
        m1.setField("id", "P11!D41");
        m1.setField("name_s", "Red Swingline Brochure");
        m1.setField("pages_i", 1);
        m1.setField("content_t", "...");

        s1.setField("manuals", m1);
      }

      final SolrInputDocument s2 = new SolrInputDocument();
      s2.setField("id", "P11!S31");
      s2.setField("color_s", "BLACK");
      s2.setField("price_i", 3);
      
      p1.setField("skus", Arrays.asList(s1, s2));
    }
    {
      final SolrInputDocument m1 = new SolrInputDocument();
      m1.setField("id", "P11!D51");
      m1.setField("name_s", "Quick Reference Guide");
      m1.setField("pages_i", 1);
      m1.setField("content_t", "How to use your stapler ...");

      final SolrInputDocument m2 = new SolrInputDocument();
      m2.setField("id", "P11!D61");
      m2.setField("name_s", "Warranty Details");
      m2.setField("pages_i", 42);
      m2.setField("content_t", "... lifetime guarantee ...");

      p1.setField("manuals", Arrays.asList(m1, m2));
    }

    final SolrInputDocument p2 = new SolrInputDocument();
    p2.setField("id", "P22!prod");
    p2.setField("name_s", "Mont Blanc Fountain Pen");
    p2.setField("description_t", "The Cadillac of Writing Instruments ...");
    {
      final SolrInputDocument s1 = new SolrInputDocument();
      s1.setField("id", "P22!S22");
      s1.setField("color_s", "RED");
      s1.setField("price_i", 89);
      { 
        final SolrInputDocument m1 = new SolrInputDocument();
        m1.setField("id", "P22!D42");
        m1.setField("name_s", "Red Mont Blanc Brochure");
        m1.setField("pages_i", 1);
        m1.setField("content_t", "...");

        s1.setField("manuals", m1);
      }
      
      final SolrInputDocument s2 = new SolrInputDocument();
      s2.setField("id", "P22!S32");
      s2.setField("color_s", "BLACK");
      s2.setField("price_i", 67);
      
      p2.setField("skus", Arrays.asList(s1, s2));
    }
    {
      final SolrInputDocument m1 = new SolrInputDocument();
      m1.setField("id", "P22!D52");
      m1.setField("name_s", "How To Use A Pen");
      m1.setField("pages_i", 42);
      m1.setField("content_t", "Start by removing the cap ...");

      p2.setField("manuals", m1);
    }
    
    client.add(Arrays.asList(p1, p2));
   
    client.commit();

    
    String url = cluster.getRandomJetty(random()).getBaseUrl() + "/" + COLLECTION_NAME;
    
    String tmpFileLoc = new File(cluster.getBaseDir().toFile().getAbsolutePath() +
        File.separator).getPath();
    
    ExportTool.Info info = new ExportTool.MultiThreadedRunner(url);
    String absolutePath = tmpFileLoc + COLLECTION_NAME + random().nextInt(100000) + ".json";
    info.setOutFormat(absolutePath, "json");
    info.setLimit("-1");
    info.query = "description_t:Cadillac";
    info.fields = "*,[child]";
    info.exportDocs();
    
    assertEquals(2, info.docsWritten.get());
    
    String jsonOutput = Files.readString(new File(info.out).toPath());

    SolrTestCaseHS.matchJSON(jsonOutput, 
        "//id=='P11!prod'", 
        "//name_s=='Swingline Stapler'",
        "//id=='P11!prod'/skus/[1]/id=='P11!S21'",
        "//id=='P11!prod'/skus/[1]/manuals/[1]/id=='P11!D41'",
        "//id=='P11!prod'/skus/[2]/id=='P11!S31'",
        "//id=='P11!prod'/manuals/[1]/id=='P11!D51'"        
        );
    
    info = new ExportTool.MultiThreadedRunner(url);
    absolutePath = tmpFileLoc + COLLECTION_NAME + random().nextInt(100000) + ".jsonl";
    info.setOutFormat(absolutePath, "jsonl");
    info.setLimit("2");
    info.query = "description_t:Cadillac";
    info.fields = "*,[child]";
    info.exportDocs();   
    
    long lines = Files.lines(new File(info.out).toPath()).count();
    assertEquals(2, lines);
    
    
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
