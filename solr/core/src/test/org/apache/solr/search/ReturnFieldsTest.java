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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.*;
import org.junit.BeforeClass;
import org.junit.Test;


public class ReturnFieldsTest extends SolrTestCaseJ4 {

  // :TODO: datatypes produced by the functions used may change

  /**
   * values of the fl param that mean all real fields
   */
  private static String[] ALL_REAL_FIELDS = new String[] { "", "*" };

  /**
   * values of the fl param that mean all real fields and score
   */
  private static String[] SCORE_AND_REAL_FIELDS = new String[] {
      "score", "score,*", "*,score"
  };

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema12.xml");
    String v = "how now brown cow";
    assertU(adoc("id","1", "text",v,  "text_np", v));
    v = "now cow";
    assertU(adoc("id","2", "text",v,  "text_np",v));
    assertU(commit());
  }

  @Test
  public void testCopyRename() throws Exception {

    // original
    assertQ(req("q","id:1", "fl","id")
        ,"//*[@numFound='1'] "
        ,"*[count(//doc/str)=1] "
        ,"*//doc[1]/str[1][.='1'] "
        );
    
    // rename
    assertQ(req("q","id:1", "fl","xxx:id")
        ,"//*[@numFound='1'] "
        ,"*[count(//doc/str)=1] "
        ,"*//doc[1]/str[1][.='1'] "
        );

    // original and copy
    assertQ(req("q","id:1", "fl","id,xxx:id")
        ,"//*[@numFound='1'] "
        ,"*[count(//doc/str)=2] "
        ,"*//doc[1]/str[1][.='1'] "
        ,"*//doc[1]/str[2][.='1'] "
        );
    assertQ(req("q","id:1", "fl","xxx:id,id")
        ,"//*[@numFound='1'] "
        ,"*[count(//doc/str)=2] "
        ,"*//doc[1]/str[1][.='1'] "
        ,"*//doc[1]/str[2][.='1'] "
        );

    // two copies
    assertQ(req("q","id:1", "fl","xxx:id,yyy:id")
        ,"//*[@numFound='1'] "
        ,"*[count(//doc/str)=2] "
        ,"*//doc[1]/str[1][.='1'] "
        ,"*//doc[1]/str[2][.='1'] "
        );
  }

  @Test
  public void testSeparators() {
    ReturnFields rf = new ReturnFields( req("fl", "id name test subject score") );
    assertTrue( rf.wantsScore() );
    assertTrue( rf.wantsField( "id" ) );
    assertTrue( rf.wantsField( "name" ) );
    assertTrue( rf.wantsField( "test" ) );
    assertTrue( rf.wantsField( "subject" ) );
    assertTrue( rf.wantsField( "score" ) );
    assertFalse( rf.wantsAllFields() );
    assertFalse( rf.wantsField( "xxx" ) );
    assertTrue( rf.getTransformer() instanceof ScoreAugmenter);

    rf = new ReturnFields( req("fl", "id,name,test,subject,score") );
    assertTrue( rf.wantsScore() );
    assertTrue( rf.wantsField( "id" ) );
    assertTrue( rf.wantsField( "name" ) );
    assertTrue( rf.wantsField( "test" ) );
    assertTrue( rf.wantsField( "subject" ) );
    assertTrue( rf.wantsField( "score" ) );
    assertFalse( rf.wantsAllFields() );
    assertFalse( rf.wantsField( "xxx" ) );
    assertTrue( rf.getTransformer() instanceof ScoreAugmenter);

    rf = new ReturnFields( req("fl", "id,name test,subject score") );
    assertTrue( rf.wantsScore() );
    assertTrue( rf.wantsField( "id" ) );
    assertTrue( rf.wantsField( "name" ) );
    assertTrue( rf.wantsField( "test" ) );
    assertTrue( rf.wantsField( "subject" ) );
    assertTrue( rf.wantsField( "score" ) );
    assertFalse( rf.wantsAllFields() );
    assertFalse( rf.wantsField( "xxx" ) );
    assertTrue( rf.getTransformer() instanceof ScoreAugmenter);

    rf = new ReturnFields( req("fl", "id, name  test , subject,score") );
    assertTrue( rf.wantsScore() );
    assertTrue( rf.wantsField( "id" ) );
    assertTrue( rf.wantsField( "name" ) );
    assertTrue( rf.wantsField( "test" ) );
    assertTrue( rf.wantsField( "subject" ) );
    assertTrue( rf.wantsField( "score" ) );
    assertFalse( rf.wantsAllFields() );
    assertFalse( rf.wantsField( "xxx" ) );
    assertTrue( rf.getTransformer() instanceof ScoreAugmenter);
  }

  @Test
  public void testWilcards() {
    ReturnFields rf = new ReturnFields( req("fl", "*") );
    assertFalse( rf.wantsScore() );
    assertTrue( rf.wantsField( "xxx" ) );
    assertTrue( rf.wantsAllFields() );
    assertNull( rf.getTransformer() );

    rf = new ReturnFields( req("fl", " * ") );
    assertFalse( rf.wantsScore() );
    assertTrue( rf.wantsField( "xxx" ) );
    assertTrue( rf.wantsAllFields() );
    assertNull( rf.getTransformer() );

    // Check that we want wildcards
    rf = new ReturnFields( req("fl", "id,aaa*,*bbb") );
    assertTrue( rf.wantsField( "id" ) );
    assertTrue( rf.wantsField( "aaaxxx" ) );
    assertFalse(rf.wantsField("xxxaaa"));
    assertTrue( rf.wantsField( "xxxbbb" ) );
    assertFalse(rf.wantsField("bbbxxx"));
    assertFalse( rf.wantsField( "aa" ) );
    assertFalse( rf.wantsField( "bb" ) );
  }

  @Test
  public void testManyParameters() {
    ReturnFields rf = new ReturnFields( req("fl", "id name", "fl", "test subject", "fl", "score") );
    assertTrue( rf.wantsScore() );
    assertTrue( rf.wantsField( "id" ) );
    assertTrue( rf.wantsField( "name" ) );
    assertTrue( rf.wantsField( "test" ) );
    assertTrue( rf.wantsField( "subject" ) );
    assertTrue( rf.wantsField( "score" ) );
    assertFalse( rf.wantsAllFields() );
    assertFalse( rf.wantsField( "xxx" ) );
    assertTrue( rf.getTransformer() instanceof ScoreAugmenter);
  }

  @Test
  public void testFunctions() {
    ReturnFields rf = new ReturnFields( req("fl", "id sum(1,1)") );
    assertFalse(rf.wantsScore());
    assertTrue( rf.wantsField( "id" ) );
    assertFalse( rf.wantsAllFields() );
    assertFalse( rf.wantsField( "xxx" ) );
    assertTrue( rf.getTransformer() instanceof ValueSourceAugmenter);
    assertEquals("sum(1,1)", ((ValueSourceAugmenter) rf.getTransformer()).name);
  }

  @Test
  public void testTransformers() {
    ReturnFields rf = new ReturnFields( req("fl", "[explain]") );
    assertFalse( rf.wantsScore() );
    assertFalse(rf.wantsField("id"));
    assertFalse(rf.wantsAllFields());
    assertEquals( "[explain]", rf.getTransformer().getName() );

    rf = new ReturnFields( req("fl", "[shard],id") );
    assertFalse( rf.wantsScore() );
    assertTrue(rf.wantsField("id"));
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());
    assertEquals( "[shard]", rf.getTransformer().getName() );

    rf = new ReturnFields( req("fl", "[docid]") );
    assertFalse( rf.wantsScore() );
    assertFalse( rf.wantsField( "id" ) );
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());
    assertEquals( "[docid]", rf.getTransformer().getName() );

    rf = new ReturnFields( req("fl", "mydocid:[docid]") );
    assertFalse( rf.wantsScore() );
    assertFalse( rf.wantsField( "id" ) );
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());
    assertEquals( "mydocid", rf.getTransformer().getName() );

    rf = new ReturnFields( req("fl", "[docid][shard]") );
    assertFalse( rf.wantsScore() );
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());
    assertTrue( rf.getTransformer() instanceof DocTransformers);
    assertEquals(2, ((DocTransformers)rf.getTransformer()).size());

    rf = new ReturnFields( req("fl", "[xxxxx]") );
    assertFalse( rf.wantsScore() );
    assertFalse( rf.wantsField( "id" ) );
    assertFalse(rf.wantsAllFields());
    assertNull(rf.getTransformer());
  }

  @Test
  public void testAliases() {
    ReturnFields rf = new ReturnFields( req("fl", "newId:id newName:name newTest:test newSubject:subject") );
    assertTrue(rf.wantsField("id"));
    assertTrue(rf.wantsField("name"));
    assertTrue(rf.wantsField("test"));
    assertTrue(rf.wantsField("subject"));
    assertTrue(rf.wantsField("newId"));
    assertTrue(rf.wantsField("newName"));
    assertTrue(rf.wantsField("newTest"));
    assertTrue(rf.wantsField("newSubject"));
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());

    rf = new ReturnFields( req("fl", "newId:id newName:name newTest:test newSubject:subject score") );
    assertTrue(rf.wantsField("id"));
    assertTrue(rf.wantsField("name"));
    assertTrue(rf.wantsField("test"));
    assertTrue(rf.wantsField("subject"));
    assertTrue(rf.wantsField("newId"));
    assertTrue(rf.wantsField("newName"));
    assertTrue(rf.wantsField("newTest"));
    assertTrue(rf.wantsField("newSubject"));
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());
    assertTrue( rf.getTransformer() instanceof DocTransformers);
    assertEquals(5, ((DocTransformers)rf.getTransformer()).size());  // 4 rename and score
  }

  // hyphens in field names are not supported in all contexts, but we wanted
  // the simplest case of fl=foo-bar to work
  @Test
  public void testHyphenInFieldName() {
    ReturnFields rf = new ReturnFields(req("fl", "id-test"));
    assertFalse(rf.wantsScore());
    assertTrue(rf.wantsField("id-test"));
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());
  }

  @Test
  public void testTrailingDotInFieldName() {
    ReturnFields rf = new ReturnFields(req("fl", "id.test"));
    assertFalse(rf.wantsScore());
    assertTrue(rf.wantsField("id.test"));
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());

    rf = new ReturnFields(req("fl", "test:id.test"));
    assertFalse(rf.wantsScore());
    assertTrue(rf.wantsField("id.test"));
    assertTrue(rf.wantsField("test"));
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());

    rf = new ReturnFields(req("fl", "test.id:id.test"));
    assertFalse(rf.wantsScore());
    assertTrue(rf.wantsField("id.test"));
    assertTrue(rf.wantsField("test.id"));
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());
  }

  @Test
  public void testTrailingDollarInFieldName() {
    ReturnFields rf = new ReturnFields(req("fl", "id$test"));
    assertFalse(rf.wantsScore());
    assertTrue(rf.wantsField("id$test"));
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());
  }
}
