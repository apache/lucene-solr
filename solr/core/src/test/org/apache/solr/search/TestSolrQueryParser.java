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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.response.transform.ScoreAugmenter;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestSolrQueryParser extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema12.xml");
    createIndex();
  }

  public static void createIndex() {
    String v;
    v="how now brown cow";
    assertU(adoc("id","1", "text",v,  "text_np",v));
    v="now cow";
    assertU(adoc("id","2", "text",v,  "text_np",v));
    assertU(commit());
  }

  @Test
  public void testPhrase() {
    // should generate a phrase of "now cow" and match only one doc
    assertQ(req("q","text:now-cow", "indent","true")
        ,"//*[@numFound='1']"
    );
    // should generate a query of (now OR cow) and match both docs
    assertQ(req("q","text_np:now-cow", "indent","true")
        ,"//*[@numFound='2']"
    );
  }

  @Test
  public void testReturnFields() {
    ReturnFields rf = new ReturnFields( req("fl", "id,score") );
    assertTrue( rf.wantsScore() );
    assertTrue( rf.wantsField( "score" ) );
    assertTrue( rf.wantsField( "id" ) );
    assertFalse( rf.wantsField( "xxx" ) );
    assertTrue( rf.getTransformer() instanceof ScoreAugmenter );
    
    rf = new ReturnFields( req("fl", "*") );
    assertFalse( rf.wantsScore() );
    assertTrue( rf.wantsField( "xxx" ) );
    assertTrue( rf.wantsAllFields() );
    assertNull( rf.getTransformer() );
    
    rf = new ReturnFields( req("fl", "[explain]") );
    assertFalse( rf.wantsScore() );
    assertFalse( rf.wantsField( "id" ) );
    assertEquals( "[explain]", rf.getTransformer().getName() );

    // Check that we want wildcards
    rf = new ReturnFields( req("fl", "id,aaa*,*bbb") );
    assertTrue( rf.wantsField( "id" ) );
    assertTrue( rf.wantsField( "aaaa" ) );
    assertTrue( rf.wantsField( "xxxbbb" ) );
    assertFalse( rf.wantsField( "aa" ) );
    assertFalse( rf.wantsField( "bb" ) );
  }
}
