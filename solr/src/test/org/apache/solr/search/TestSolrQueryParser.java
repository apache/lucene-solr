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
import org.apache.solr.response.transform.DocTransformers;
import org.apache.solr.response.transform.ScoreAugmenter;
import org.apache.solr.response.transform.ValueAugmenterFactory;
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

    // legacy, score is *,score
    rf = new ReturnFields( req("fl", "score ") );
    assertTrue( rf.wantsScore() );
    assertTrue( rf.wantsAllFields() );
    assertTrue( rf.wantsField( "score" ) );
    
    rf = new ReturnFields( req("fl", "[explain],score") );
    assertTrue( rf.wantsScore() );
    assertFalse( rf.wantsField( "id" ) );
    assertEquals( "Transformers[[explain],score]", rf.getTransformer().getName() );
    assertTrue( rf.wantsScore() );

    // Check that we want wildcards
    rf = new ReturnFields( req("fl", "id,aaa*,*bbb") );
    assertTrue( rf.wantsField( "id" ) );
    assertTrue( rf.wantsField( "aaaa" ) );
    assertTrue( rf.wantsField( "xxxbbb" ) );
    assertFalse( rf.wantsField( "aa" ) );
    assertFalse( rf.wantsField( "bb" ) );
    
    // Check pseudo fiels are replaced
    rf = new ReturnFields( req("fl", "price", "fl.pseudo.price", "[value 10]", "fl.pseudo", "true" ) );
    assertTrue( rf.wantsField( "price" ) );
    assertEquals( "price", rf.getTransformer().getName() );
    assertTrue( rf.getTransformer().getClass().getName().indexOf( "ValueAugmenter" ) > 0 );

    rf = new ReturnFields( req("fl", "xxx:price,yyy:name", "fl.pseudo.price", "[value 10]", "fl.pseudo", "true" ) );
    assertTrue( rf.wantsField( "price" ) );
    assertTrue( rf.wantsField( "yyy" ) );
    assertTrue( rf.getLuceneFieldNames().contains("name") );
    assertEquals( "xxx", rf.getTransformer().getName() );
    assertTrue( rf.getTransformer().getClass().getName().indexOf( "ValueAugmenter" ) > 0 );

    // multiple transformers
    rf = new ReturnFields( req("fl", "[value hello],[explain]" ) );
    DocTransformers tx = (DocTransformers)rf.getTransformer(); // will throw exception
    assertTrue( tx.get(0).getClass().getName().indexOf( "ValueAugmenter" ) > 0 );
    assertTrue( tx.get(1).getClass().getName().indexOf( "ExplainAugmenter" ) > 0 );
  }
}
