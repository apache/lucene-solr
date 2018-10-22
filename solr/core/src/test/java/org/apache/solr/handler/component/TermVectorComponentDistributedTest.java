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

import org.apache.lucene.util.Constants;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.TermVectorParams;
import org.apache.solr.search.stats.ExactStatsCache;
import org.apache.solr.search.stats.LRUStatsCache;
import org.apache.solr.search.stats.LocalStatsCache;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TermVectorComponentDistributedTest extends BaseDistributedSearchTestCase {
  @BeforeClass
  public static void betterNotBeJ9() {
    assumeFalse("FIXME: SOLR-5792: This test fails under IBM J9", 
                Constants.JAVA_VENDOR.startsWith("IBM"));
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
  public void test() throws Exception {

    handle.clear();
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);
    handle.put("score", SKIPVAL);
    handle.put("[docid]", SKIPVAL);
    handle.put("_version_", SKIPVAL); // not a cloud test, but may use updateLog

    // SOLR-3720: TODO: TVC doesn't "merge" df and idf .. should it?
    handle.put("df", SKIPVAL);
    handle.put("tf-idf", SKIPVAL);

    index("id", "0",
          "test_posofftv", "This is a title and another title",
          "test_basictv", "This is a title and another title",
          "test_notv", "This is a title and another title",
          "test_postv", "This is a title and another title",
          "test_offtv", "This is a title and another title"
          );
    index("id", "1",
          "test_posofftv", "The quick reb fox jumped over the lazy brown dogs.",
          "test_basictv", "The quick reb fox jumped over the lazy brown dogs.",
          "test_notv", "The quick reb fox jumped over the lazy brown dogs.",
          "test_postv", "The quick reb fox jumped over the lazy brown dogs.",
          "test_offtv", "The quick reb fox jumped over the lazy brown dogs."
          );
    
    index("id", "2",
          "test_posofftv", "This is a document",
          "test_basictv", "This is a document",
          "test_notv", "This is a document",
          "test_postv", "This is a document",
          "test_offtv", "This is a document"
          );
    index("id", "3",
          "test_posofftv", "another document",
          "test_basictv", "another document",
          "test_notv", "another document",
          "test_postv", "another document",
          "test_offtv", "another document"
          );
    //bunch of docs that are variants on blue
    index("id", "4",
          "test_posofftv", "blue",
          "test_basictv", "blue",
          "test_notv", "blue",
          "test_postv", "blue",
          "test_offtv", "blue"
          );
    index("id", "5",
          "test_posofftv", "blud",
          "test_basictv", "blud",
          "test_notv", "blud",
          "test_postv", "blud",
          "test_offtv", "blud"
          );
    index("id", "6",
          "test_posofftv", "boue",
          "test_basictv", "boue",
          "test_notv", "boue",
          "test_postv", "boue",
          "test_offtv", "boue"
          );
    index("id", "7",
          "test_posofftv", "glue",
          "test_basictv", "glue",
          "test_notv", "glue",
          "test_postv", "glue",
          "test_offtv", "glue"
          );
    index("id", "8",
          "test_posofftv", "blee",
          "test_basictv", "blee",
          "test_notv", "blee",
          "test_postv", "blee",
          "test_offtv", "blee"
          );
    index("id", "9",
          "test_posofftv", "blah",
          "test_basictv", "blah",
          "test_notv", "blah",
          "test_postv", "blah",
          "test_offtv", "blah"
          );

    commit();

    final String tv = "tvrh";

    for (String q : new String[] {"id:0", "id:7", "id:[3 TO 6]", "*:*"}) {
      query("sort","id desc",
            "qt",tv, 
            "q", q,
            TermVectorComponent.COMPONENT_NAME, "true", 
            TermVectorParams.TF, "true");

      // tv.fl diff from fl
      query("sort", "id asc",
            "qt",tv, 
            "q", q,
            "fl", "*,score",
            "tv.fl", "test_basictv,test_offtv",
            TermVectorComponent.COMPONENT_NAME, "true", 
            TermVectorParams.TF, "true");

      // multi-valued tv.fl 
      query("sort", "id asc",
            "qt",tv, 
            "q", q,
            "fl", "*,score",
            "tv.fl", "test_basictv",
            "tv.fl","test_offtv",
            TermVectorComponent.COMPONENT_NAME, "true", 
            TermVectorParams.TF, "true");
      // re-use fl glob
      query("sort", "id desc",
            "qt",tv, 
            "q", q,
            "fl", "*,score",
            TermVectorComponent.COMPONENT_NAME, "true", 
            TermVectorParams.TF, "true");
      // re-use fl, ignore things we can't handle
      query("sort", "id desc",
            "qt",tv, 
            "q", q,
            "fl", "score,test_basictv,[docid],test_postv,val:sum(3,4)",
            TermVectorComponent.COMPONENT_NAME, "true", 
            TermVectorParams.TF, "true");

      // re-use (multi-valued) fl, ignore things we can't handle
      query("sort", "id desc",
            "qt",tv, 
            "q", q,
            "fl", "score,test_basictv",
            "fl", "[docid],test_postv,val:sum(3,4)",
            TermVectorComponent.COMPONENT_NAME, "true", 
            TermVectorParams.TF, "true");

      // test some other options
    
      query("sort", "id asc",
            "qt",tv, 
            "q", q,
            TermVectorComponent.COMPONENT_NAME, "true",
            TermVectorParams.TF, "true", 
            TermVectorParams.DF, "true", 
            TermVectorParams.OFFSETS, "true", 
            TermVectorParams.POSITIONS, "true", 
            TermVectorParams.TF_IDF, "true");
    
      query("sort", "id desc",
            "qt",tv, 
            "q", q,
            TermVectorComponent.COMPONENT_NAME, "true",
            TermVectorParams.ALL, "true");

      query("sort", "id desc",
          "qt",tv,
          "q", q,
          "rows", 1,
          ShardParams.DISTRIB_SINGLE_PASS, "true",
          TermVectorComponent.COMPONENT_NAME, "true",
          TermVectorParams.ALL, "true");

      // per field stuff

      query("sort", "id desc",
            "qt",tv, 
            "q", q,
            TermVectorComponent.COMPONENT_NAME, "true",
            TermVectorParams.TF, "true", 
            TermVectorParams.DF, "true", 
            TermVectorParams.OFFSETS, "true", 
            TermVectorParams.POSITIONS, "true", 
            TermVectorParams.TF_IDF, "true",
            TermVectorParams.FIELDS, "test_basictv,test_notv,test_postv,test_offtv,test_posofftv",
            "f.test_posofftv." + TermVectorParams.POSITIONS, "false",
            "f.test_offtv." + TermVectorParams.OFFSETS, "false",
            "f.test_basictv." + TermVectorParams.DF, "false",
            "f.test_basictv." + TermVectorParams.TF, "false",
            "f.test_basictv." + TermVectorParams.TF_IDF, "false");
    }
  }
}
