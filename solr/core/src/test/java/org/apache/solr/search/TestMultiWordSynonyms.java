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

import java.util.Arrays;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMultiWordSynonyms extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema-multiword-synonyms.xml");
    index();
  }

  private static void index() throws Exception {
    assertU(adoc("id","1", "text","USA Today"));
    assertU(adoc("id","2", "text","A dynamic US economy"));
    assertU(adoc("id","3", "text","The United States of America's 50 states"));
    assertU(adoc("id","4", "text","Party in the U.S.A."));
    assertU(adoc("id","5", "text","These United States"));

    assertU(adoc("id","6", "text","America United of States"));
    assertU(adoc("id","7", "text","States United"));

    assertU(commit());
  }

  @Test
  public void testNonPhrase() throws Exception {
    // Don't split on whitespace (sow=false)
    for (String q : Arrays.asList("US", "U.S.", "USA", "U.S.A.", "United States", "United States of America")) {
      for (String defType : Arrays.asList("lucene", "edismax")) {
        assertJQ(req("q", q,
            "defType", defType,
            "df", "text",
            "sow", "false")
            , "/response/numFound==7"
        );
      }
    }

    // Split on whitespace (sow=true)
    for (String q : Arrays.asList("US", "U.S.", "USA", "U.S.A.")) {
      for (String defType : Arrays.asList("lucene", "edismax")) {
        assertJQ(req("q", q,
            "defType", defType,
            "df", "text",
            "sow", "true")
            , "/response/numFound==7"
        );
      }
    }
    for (String q : Arrays.asList("United States", "United States of America")) {
      for (String defType : Arrays.asList("lucene", "edismax")) {
        assertJQ(req("q", q,
            "defType", defType,
            "df", "text",
            "sow", "true")
            , "/response/numFound==4"
        );
      }
    }
  }
  
  @Test
  public void testPhrase() throws Exception {
    for (String q : Arrays.asList
        ("\"US\"", "\"U.S.\"", "\"USA\"", "\"U.S.A.\"", "\"United States\"", "\"United States of America\"")) {
      for (String defType : Arrays.asList("lucene", "edismax")) {
        for (String sow : Arrays.asList("true", "false")) {
          assertJQ(req("q", q,
              "defType", defType,
              "df", "text",
              "sow", sow)
              , "/response/numFound==5"
          );
        }
      }
    }
  }
}
