package org.apache.solr.handler.component;

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

import java.io.File;
import java.io.IOException;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.util.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * 
 */
public class DistributedQueryElevationComponentTest extends BaseDistributedSearchTestCase {

  public DistributedQueryElevationComponentTest() {
    fixShardCount = true;
    shardCount = 3;
    stress = 0;

    // TODO: a better way to do this?
    configString = "solrconfig-elevate.xml";
    schemaString = "schema11.xml";
  }
  
  @BeforeClass
  public static void beforeClass() throws IOException {
    System.setProperty("elevate.data.file", "elevate.xml");
    File parent = new File(TEST_HOME(), "conf");
  }
  
  @AfterClass
  public static void afterClass() throws IOException {
    System.clearProperty("elevate.data.file");
  }

  @Override
  public void doTest() throws Exception {
    
    
    del("*:*");
    indexr(id,"1", "int_i", "1", "text", "XXXX XXXX", "field_t", "anything");
    indexr(id,"2", "int_i", "2", "text", "YYYY YYYY", "plow_t", "rake");
    indexr(id,"3", "int_i", "3", "text", "ZZZZ ZZZZ");
    indexr(id,"4", "int_i", "4", "text", "XXXX XXXX");
    indexr(id,"5", "int_i", "5", "text", "ZZZZ ZZZZ ZZZZ");
    indexr(id,"6", "int_i", "6", "text", "ZZZZ");
    
    index_specific(2, id, "7", "int_i", "7", "text", "solr");
    commit();
    
    handle.put("explain", SKIPVAL);
    handle.put("debug", SKIPVAL);
    handle.put("QTime", SKIPVAL);
    handle.put("maxScore", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("score", SKIPVAL);
    handle.put("wt", SKIP);
    handle.put("distrib", SKIP);
    handle.put("shards.qt", SKIP);
    handle.put("shards", SKIP);
    handle.put("q", SKIP);
    handle.put("qt", SKIP);
    query("q", "*:*", "qt", "/elevate", "shards.qt", "/elevate", "rows", "500", "sort", "id desc", CommonParams.FL, "id, score, [elevated]");

    query("q", "ZZZZ", "qt", "/elevate", "shards.qt", "/elevate", "rows", "500", CommonParams.FL, "*, [elevated]", "forceElevation", "true", "sort", "int_i desc");
    
    query("q", "solr", "qt", "/elevate", "shards.qt", "/elevate", "rows", "500", CommonParams.FL, "*, [elevated]", "forceElevation", "true", "sort", "int_i asc");
    
    query("q", "ZZZZ", "qt", "/elevate", "shards.qt", "/elevate", "rows", "500", CommonParams.FL, "*, [elevated]", "forceElevation", "true", "sort", "id desc");
  }
  
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    indexDoc(doc);
  }
  
}
