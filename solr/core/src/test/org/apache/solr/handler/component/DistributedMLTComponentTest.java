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
package org.apache.solr.handler.component;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.MoreLikeThisParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.stats.ExactStatsCache;
import org.apache.solr.search.stats.LRUStatsCache;
import org.apache.solr.search.stats.LocalStatsCache;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for distributed MoreLikeThisComponent's 
 *
 * @since solr 4.1
 *
 * @see org.apache.solr.handler.component.MoreLikeThisComponent
 */
@Slow
public class DistributedMLTComponentTest extends BaseDistributedSearchTestCase {
  
  private String requestHandlerName;

  public DistributedMLTComponentTest()
  {
    stress=0;
  }

  @Override
  public void distribSetUp() throws Exception {
    requestHandlerName = "mltrh";
    super.distribSetUp();
  }

  @BeforeClass
  public static void beforeClass() {
    int statsType = TestUtil.nextInt(random(), 1, 3);
    if (statsType == 1) {
      System.setProperty("solr.statsCache", ExactStatsCache.class.getName());
    } else if (statsType == 2) {
      System.setProperty("solr.statsCache", LRUStatsCache.class.getName());
    } else {
      System.setProperty("solr.statsCache", LocalStatsCache.class.getName());
    }
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("solr.statsCache");
  }
  
  @Test
  @ShardsFixed(num = 3)
  @SuppressWarnings({"unchecked"})
  public void test() throws Exception {
    del("*:*");
    index(id, "1", "lowerfilt", "toyota", "lowerfilt1", "x");
    index(id, "2", "lowerfilt", "chevrolet", "lowerfilt1", "x");
    index(id, "3", "lowerfilt", "suzuki", "lowerfilt1", "x");
    index(id, "4", "lowerfilt", "ford", "lowerfilt1", "x");
    index(id, "5", "lowerfilt", "ferrari", "lowerfilt1", "x");
    index(id, "6", "lowerfilt", "jaguar", "lowerfilt1", "x");
    index(id, "7", "lowerfilt", "mclaren moon or the moon and moon moon shine and the moon but moon was good foxes too", "lowerfilt1", "x");
    index(id, "8", "lowerfilt", "sonata", "lowerfilt1", "x");
    index(id, "9", "lowerfilt", "The quick red fox jumped over the lazy big and large brown dogs.", "lowerfilt1", "x");
    index(id, "10", "lowerfilt", "blue", "lowerfilt1", "x");
    index(id, "12", "lowerfilt", "glue", "lowerfilt1", "x");
    index(id, "13", "lowerfilt", "The quote red fox jumped over the lazy brown dogs.", "lowerfilt1", "y");
    index(id, "14", "lowerfilt", "The quote red fox jumped over the lazy brown dogs.", "lowerfilt1", "y");
    index(id, "15", "lowerfilt", "The fat red fox jumped over the lazy brown dogs.", "lowerfilt1", "y");
    index(id, "16", "lowerfilt", "The slim red fox jumped over the lazy brown dogs.", "lowerfilt1", "y");
    index(id, "17", "lowerfilt", "The quote red fox jumped moon over the lazy brown dogs moon. Of course moon. Foxes and moon come back to the foxes and moon", "lowerfilt1", "y");
    index(id, "18", "lowerfilt", "The quote red fox jumped over the lazy brown dogs.", "lowerfilt1", "y");
    index(id, "19", "lowerfilt", "The hose red fox jumped over the lazy brown dogs.", "lowerfilt1", "y");
    index(id, "20", "lowerfilt", "The quote red fox jumped over the lazy brown dogs.", "lowerfilt1", "y");
    index(id, "21", "lowerfilt", "The court red fox jumped over the lazy brown dogs.", "lowerfilt1", "y");
    index(id, "22", "lowerfilt", "The quote red fox jumped over the lazy brown dogs.", "lowerfilt1", "y");
    index(id, "23", "lowerfilt", "The quote red fox jumped over the lazy brown dogs.", "lowerfilt1", "y");
    index(id, "24", "lowerfilt", "The file red fox jumped over the lazy brown dogs.", "lowerfilt1", "y");
    index(id, "25", "lowerfilt", "rod fix", "lowerfilt1", "y");
    
    commit();

    handle.clear();
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);
    // we care only about the mlt results
    handle.put("response", SKIP);
    
    // currently distrib mlt is sorting by score (even though it's not really comparable across shards)
    // so it may not match the sort of single shard mlt
    handle.put("17", UNORDERED);
    
    query("q", "match_none", "mlt", "true", "mlt.fl", "lowerfilt", "qt", requestHandlerName, "shards.qt", requestHandlerName);
    
    query("q", "lowerfilt:sonata", "mlt", "true", "mlt.fl", "lowerfilt", "qt", requestHandlerName, "shards.qt", requestHandlerName);
    
    handle.put("24", UNORDERED);
    handle.put("23", UNORDERED);
    handle.put("22", UNORDERED);
    handle.put("21", UNORDERED);
    handle.put("20", UNORDERED);
    handle.put("19", UNORDERED);
    handle.put("18", UNORDERED);
    handle.put("17", UNORDERED);
    handle.put("16", UNORDERED);
    handle.put("15", UNORDERED);
    handle.put("14", UNORDERED);
    handle.put("13", UNORDERED);
    handle.put("7", UNORDERED);
    
    // keep in mind that MLT params influence stats that are calulated
    // per shard - because of this, depending on params, distrib and single
    // shard queries will not match.
    
    // because distrib and single node do not currently sort exactly the same,
    // we ask for an mlt.count of 20 to ensure both include all results
    
    query("q", "lowerfilt:moon", "fl", id, MoreLikeThisParams.MIN_TERM_FREQ, 2,
        MoreLikeThisParams.MIN_DOC_FREQ, 1, "sort", "id_i1 desc", "mlt", "true",
        "mlt.fl", "lowerfilt", "qt", requestHandlerName, "shards.qt",
        requestHandlerName, "mlt.count", "20");
    
    query("q", "lowerfilt:fox", "fl", id, MoreLikeThisParams.MIN_TERM_FREQ, 1,
        MoreLikeThisParams.MIN_DOC_FREQ, 1, "sort", "id_i1 desc", "mlt", "true",
        "mlt.fl", "lowerfilt", "qt", requestHandlerName, "shards.qt",
        requestHandlerName, "mlt.count", "20");

    query("q", "lowerfilt:the red fox", "fl", id, MoreLikeThisParams.MIN_TERM_FREQ, 1,
        MoreLikeThisParams.MIN_DOC_FREQ, 1, "sort", "id_i1 desc", "mlt", "true",
        "mlt.fl", "lowerfilt", "qt", requestHandlerName, "shards.qt",
        requestHandlerName, "mlt.count", "20");
    
    query("q", "lowerfilt:blue moon", "fl", id, MoreLikeThisParams.MIN_TERM_FREQ, 1,
        MoreLikeThisParams.MIN_DOC_FREQ, 1, "sort", "id_i1 desc", "mlt", "true",
        "mlt.fl", "lowerfilt", "qt", requestHandlerName, "shards.qt",
        requestHandlerName, "mlt.count", "20");

    // let's query by specifying multiple mlt.fl as comma-separated values
    QueryResponse response = query("q", "lowerfilt:moon", "fl", id, MoreLikeThisParams.MIN_TERM_FREQ, 2,
        MoreLikeThisParams.MIN_DOC_FREQ, 1, "sort", "id_i1 desc", "mlt", "true",
        "mlt.fl", "lowerfilt1,lowerfilt", "qt", requestHandlerName, "shards.qt",
        requestHandlerName, "mlt.count", "20");
    NamedList<Object> moreLikeThis = (NamedList<Object>) response.getResponse().get("moreLikeThis");
    Map<String, Long> idVsMLTCount = new HashMap<>();
    for (Map.Entry<String, Object> entry : moreLikeThis) {
      SolrDocumentList docList = (SolrDocumentList) entry.getValue();
      idVsMLTCount.put(entry.getKey(), docList.getNumFound());
    }

    // let's query by specifying multiple mlt.fl as multiple request parameters
    response = query("q", "lowerfilt:moon", "fl", id, MoreLikeThisParams.MIN_TERM_FREQ, 2,
        MoreLikeThisParams.MIN_DOC_FREQ, 1, "sort", "id_i1 desc", "mlt", "true",
        "mlt.fl", "lowerfilt1", "mlt.fl", "lowerfilt", "qt", requestHandlerName, "shards.qt",
        requestHandlerName, "mlt.count", "20");
    moreLikeThis = (NamedList<Object>) response.getResponse().get("moreLikeThis");
    for (Map.Entry<String, Object> entry : moreLikeThis) {
      String key = entry.getKey();
      Long expected = idVsMLTCount.get(key);
      Long actual = ((SolrDocumentList) entry.getValue()).getNumFound();
      assertEquals("MLT mismatch for id=" + key, expected, actual);
    }
  }
}
