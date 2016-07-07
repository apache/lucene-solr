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

import java.util.List;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.TestCloudPseudoReturnFields;
import org.apache.solr.schema.SchemaField;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;

import org.apache.commons.lang.StringUtils;

import org.junit.BeforeClass;


/** @see TestCloudPseudoReturnFields */
public class TestPseudoReturnFields extends SolrTestCaseJ4 {

  // :TODO: datatypes produced by the functions used may change

  /**
   * values of the fl param that mean all real fields
   */
  public static String[] ALL_REAL_FIELDS = new String[] { "", "*" };

  /**
   * values of the fl param that mean all real fields and score
   */
  public static String[] SCORE_AND_REAL_FIELDS = new String[] { 
    "score,*", "*,score"
  };

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-tlog.xml","schema-psuedo-fields.xml");

    assertU(adoc("id", "42", "val_i", "1", "ssto", "X", "subject", "aaa"));
    assertU(adoc("id", "43", "val_i", "9", "ssto", "X", "subject", "bbb"));
    assertU(adoc("id", "44", "val_i", "4", "ssto", "X", "subject", "aaa"));
    assertU(adoc("id", "45", "val_i", "6", "ssto", "X", "subject", "aaa"));
    assertU(adoc("id", "46", "val_i", "3", "ssto", "X", "subject", "ggg"));
    assertU(commit());

    // uncommitted doc in transaction log
    assertU(adoc("id", "99", "val_i", "1", "ssto", "X", "subject", "uncommitted"));

  }

  public void testMultiValued() throws Exception {
    // the response writers used to consult isMultiValued on the field
    // but this doesn't work when you alias a single valued field to
    // a multi valued field (the field value is copied first, then
    // if the type lookup is done again later, we get the wrong thing). SOLR-4036

    // score as psuedo field - precondition checks
    for (String name : new String[] {"score", "val_ss"}) {
      SchemaField sf = h.getCore().getLatestSchema().getFieldOrNull(name);
      assertNotNull("Test depends on a (dynamic) field mtching '"+name+
                    "', schema was changed out from under us!",sf);
      assertTrue("Test depends on a multivalued dynamic field matching '"+name+
                 "', schema was changed out from under us!", sf.multiValued());
    }

    // score as psuedo field
    assertJQ(req("q","*:*", "fq", "id:42", "fl","id,score")
             ,"/response/docs==[{'id':'42','score':1.0}]");
    
    // single value int using alias that matches multivalued dynamic field
    assertJQ(req("q","id:42", "fl","val_ss:val_i, val2_ss:10")
        ,"/response/docs==[{'val2_ss':10,'val_ss':1}]"
    );

    assertJQ(req("qt","/get", "id","42", "fl","val_ss:val_i, val2_ss:10")
        ,"/doc=={'val2_ss':10,'val_ss':1}"
    );
  }

  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-9285")
  public void testMultiValuedRTG() throws Exception {

    // single value int using alias that matches multivalued dynamic field - via RTG
    assertJQ(req("qt","/get", "id","42", "fl","val_ss:val_i, val2_ss:10, subject")
        ,"/doc=={'val2_ss':10,'val_ss':1, 'subject':'aaa'}"
    );
    
    // also check real-time-get from transaction log
    assertJQ(req("qt","/get", "id","99", "fl","val_ss:val_i, val2_ss:10, subject")
             ,"/doc=={'val2_ss':10,'val_ss':1,'subject':'uncommitted'}"
    );

  }
  
  public void testAllRealFields() throws Exception {

    for (String fl : ALL_REAL_FIELDS) {
      assertQ("fl="+fl+" ... all real fields",
              req("q","*:*", "rows", "1", "fl",fl)
              ,"//result[@numFound='5']"
              ,"//result/doc/str[@name='id']"
              ,"//result/doc/int[@name='val_i']"
              ,"//result/doc/str[@name='ssto']"
              ,"//result/doc/str[@name='subject']"
              
              ,"//result/doc[count(*)=4]"
              );
    }
  }
  
  public void testAllRealFieldsRTG() throws Exception {
    // shouldn't matter if we use RTG (committed or otherwise)
    for (String fl : ALL_REAL_FIELDS) {
      for (String id : Arrays.asList("42","99")) {
        assertQ("id="+id+", fl="+fl+" ... all real fields",
                req("qt","/get","id",id, "wt","xml","fl",fl)
                ,"count(//doc)=1"
                ,"//doc/str[@name='id']"
                ,"//doc/int[@name='val_i']"
                ,"//doc/str[@name='ssto']"
                ,"//doc/str[@name='subject']"
                ,"//doc[count(*)=4]"
                );
      }
    }

    
  }

  public void testScoreAndAllRealFields() throws Exception {

    for (String fl : SCORE_AND_REAL_FIELDS) {
      assertQ("fl="+fl+" ... score and real fields",
              req("q","*:*", "rows", "1", "fl",fl)
              ,"//result[@numFound='5']"
              ,"//result/doc/str[@name='id']"
              ,"//result/doc/int[@name='val_i']"
              ,"//result/doc/str[@name='ssto']"
              ,"//result/doc/str[@name='subject']"
              ,"//result/doc/float[@name='score']"
              
              ,"//result/doc[count(*)=5]"
              );
    }
  }
  
  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-9287")
  public void testScoreAndAllRealFieldsRTG() throws Exception {
  
    // if we use RTG (committed or otherwise) score should be ignored
    for (String fl : SCORE_AND_REAL_FIELDS) {
      for (String id : Arrays.asList("42","99")) {
        assertQ("id="+id+", fl="+fl+" ... score real fields",
                req("qt","/get","id",id, "wt","xml","fl",fl)
                ,"count(//doc)=1"
                ,"//doc/str[@name='id']"
                ,"//doc/int[@name='val_i']"
                ,"//doc/str[@name='ssto']"
                ,"//doc/str[@name='subject']"
                ,"//doc[count(*)=4]"
                );
      }
    }
  }

  public void testScoreAndExplicitRealFields() throws Exception {
    
    assertQ("fl=score,val_i",
            req("q","*:*", "rows", "1", "fl","score,val_i")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='val_i']"
            ,"//result/doc/float[@name='score']"
            
            ,"//result/doc[count(*)=2]"
            );
    assertQ("fl=score&fl=val_i",
            req("q","*:*", "rows", "1", "fl","score","fl","val_i")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='val_i']"
            ,"//result/doc/float[@name='score']"
            
            ,"//result/doc[count(*)=2]"
            );
    
    assertQ("fl=val_i",
            req("q","*:*", "rows", "1", "fl","val_i")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='val_i']"
            
            ,"//result/doc[count(*)=1]"
            );
  }

  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-9287")
  public void testScoreAndExplicitRealFieldsRTG() throws Exception {
    // if we use RTG (committed or otherwise) score should be ignored
    for (String id : Arrays.asList("42","99")) {
      assertQ("id="+id+", fl=score,val_i",
              req("qt","/get","id",id, "wt","xml", "fl","score,val_i")
              ,"count(//doc)=1"
              ,"//doc/int[@name='val_i']"
              ,"//doc[count(*)=1]"
              );
    }
  }

  public void testFunctions() throws Exception {
    assertQ("fl=log(val_i)",
            req("q","*:*", "rows", "1", "fl","log(val_i)")
            ,"//result[@numFound='5']"
            ,"//result/doc/double[@name='log(val_i)']"
            
            ,"//result/doc[count(*)=1]"
            );

    assertQ("fl=log(val_i),abs(val_i)",
            req("q","*:*", "rows", "1", "fl","log(val_i),abs(val_i)")
            ,"//result[@numFound='5']"
            ,"//result/doc/double[@name='log(val_i)']"
            ,"//result/doc/float[@name='abs(val_i)']"
            
            ,"//result/doc[count(*)=2]"
            );
    assertQ("fl=log(val_i)&fl=abs(val_i)",
            req("q","*:*", "rows", "1", "fl","log(val_i)","fl","abs(val_i)")
            ,"//result[@numFound='5']"
            ,"//result/doc/double[@name='log(val_i)']"
            ,"//result/doc/float[@name='abs(val_i)']"
            
            ,"//result/doc[count(*)=2]"
            );
  }
  
  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-9285")
  public void testFunctionsRTG() throws Exception {
    // if we use RTG (committed or otherwise) functions should behave the same
    for (String id : Arrays.asList("42","99")) {
      for (SolrParams p : Arrays.asList(params("qt","/get","id",id,"wt","xml",
                                               "fl","log(val_i),abs(val_i)"),
                                        params("qt","/get","id",id,"wt","xml",
                                               "fl","log(val_i)","fl", "abs(val_i)"))) {
        assertQ("id="+id+", params="+p, req(p)
                ,"count(//doc)=1"
                // true for both these specific docs
                ,"//doc/double[@name='log(val_i)'][.='0.0']"
                ,"//doc/float[@name='abs(val_i)'][.='1.0']"
                ,"//doc[count(*)=2]"
                );
      }
    }
  }

  public void testFunctionsAndExplicit() throws Exception {
    assertQ("fl=log(val_i),val_i",
            req("q","*:*", "rows", "1", "fl","log(val_i),val_i")
            ,"//result[@numFound='5']"
            ,"//result/doc/double[@name='log(val_i)']"
            ,"//result/doc/int[@name='val_i']"

            ,"//result/doc[count(*)=2]"
            );

    assertQ("fl=log(val_i)&fl=val_i",
            req("q","*:*", "rows", "1", "fl","log(val_i)","fl","val_i")
            ,"//result[@numFound='5']"
            ,"//result/doc/double[@name='log(val_i)']"
            ,"//result/doc/int[@name='val_i']"
            
            ,"//result/doc[count(*)=2]"
            );
  }

  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-9285")
  public void testFunctionsAndExplicitRTG() throws Exception {
    // shouldn't matter if we use RTG (committed or otherwise)
    for (String id : Arrays.asList("42","99")) {
      for (SolrParams p : Arrays.asList(params("fl","log(val_i),val_i"),
                                        params("fl","log(val_i)","fl","val_i"))) {
        assertQ(id + " " + p,
                req(p, "qt","/get", "wt","xml","id",id)
                ,"count(//doc)=1"
                // true for both these specific docs
                ,"//doc/double[@name='log(val_i)'][.='0.0']"
                ,"//doc/int[@name='val_i'][.='1']"
                ,"//doc[count(*)=2]"
                );
      }
    }
  }

  public void testFunctionsAndScore() throws Exception {

    assertQ("fl=log(val_i),score",
            req("q","*:*", "rows", "1", "fl","log(val_i),score")
            ,"//result[@numFound='5']"
            ,"//result/doc/float[@name='score']"
            ,"//result/doc/double[@name='log(val_i)']"
            
            ,"//result/doc[count(*)=2]"
            );
    assertQ("fl=log(val_i)&fl=score",
            req("q","*:*", "rows", "1", "fl","log(val_i)","fl","score")
            ,"//result[@numFound='5']"
            ,"//result/doc/float[@name='score']"
            ,"//result/doc/double[@name='log(val_i)']"
            
            ,"//result/doc[count(*)=2]"
            );

    assertQ("fl=score,log(val_i),abs(val_i)",
            req("q","*:*", "rows", "1", 
                "fl","score,log(val_i),abs(val_i)")
            ,"//result[@numFound='5']"
            ,"//result/doc/float[@name='score']"
            ,"//result/doc/double[@name='log(val_i)']"
            ,"//result/doc/float[@name='abs(val_i)']"
            
            ,"//result/doc[count(*)=3]"
            );
    assertQ("fl=score&fl=log(val_i)&fl=abs(val_i)",
            req("q","*:*", "rows", "1", 
                "fl","score","fl","log(val_i)","fl","abs(val_i)")
            ,"//result[@numFound='5']"
            ,"//result/doc/float[@name='score']"
            ,"//result/doc/double[@name='log(val_i)']"
            ,"//result/doc/float[@name='abs(val_i)']"
            
            ,"//result/doc[count(*)=3]"
            );
    
  }
  
  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-9287")
  public void testFunctionsAndScoreRTG() throws Exception {

    // if we use RTG (committed or otherwise) score should be ignored
    for (String id : Arrays.asList("42","99")) {
      for (SolrParams p : Arrays.asList(params("fl","score","fl","log(val_i)","fl","abs(val_i)"),
                                        params("fl","score","fl","log(val_i),abs(val_i)"),
                                        params("fl","score,log(val_i)","fl","abs(val_i)"),
                                        params("fl","score,log(val_i),abs(val_i)"))) {
        assertQ("id="+id+", p="+p,
                req(p, "qt","/get","id",id, "wt","xml")
                ,"count(//doc)=1"
                ,"//doc/double[@name='log(val_i)']"
                ,"//doc/float[@name='abs(val_i)']"
                ,"//doc[count(*)=2]"
                );
      }
    }
  }

  public void testGlobs() throws Exception {
    assertQ("fl=val_*",
            req("q","*:*", "rows", "1", "fl","val_*")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='val_i']"
            
            ,"//result/doc[count(*)=1]"
            );
    for (SolrParams p : Arrays.asList(params("q", "*:*", "rows", "1", "fl","val_*,subj*,ss*"),
                                      params("q", "*:*", "rows", "1", "fl","val_*","fl","subj*,ss*"),
                                      params("q", "*:*", "rows", "1", "fl","val_*","fl","subj*","fl","ss*"))) {

      assertQ(p.toString(),
              req(p)
              ,"//result[@numFound='5']"
              ,"//result/doc/int[@name='val_i']"
              ,"//result/doc/str[@name='subject']"
              ,"//result/doc/str[@name='ssto'][.='X']"
            
              ,"//result/doc[count(*)=3]"
              );
    }
  }
  
  public void testGlobsRTG() throws Exception {
    // behavior shouldn't matter if we are committed or uncommitted
    for (String id : Arrays.asList("42","99")) {
      assertQ(id + ": fl=val_*",
              req("qt","/get","id",id, "wt","xml", "fl","val_*")
              ,"count(//doc)=1"
              ,"//doc/int[@name='val_i'][.=1]"
              ,"//doc[count(*)=1]"
              );
      for (SolrParams p : Arrays.asList(params("fl","val_*,subj*,ss*"),
                                        params("fl","val_*","fl","subj*,ss*"))) {
        assertQ(id + ": " + p,
                req(p, "qt","/get","id",id, "wt","xml")
                ,"count(//doc)=1"
                ,"//doc/int[@name='val_i'][.=1]"
                ,"//doc/str[@name='subject']" // value differs between docs
                ,"//doc/str[@name='ssto'][.='X']" 
                ,"//doc[count(*)=3]"
                );
      }
    }
  }

  public void testGlobsAndExplicit() throws Exception {
    assertQ("fl=val_*,id",
            req("q","*:*", "rows", "1", "fl","val_*,id")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='val_i']"
            ,"//result/doc/str[@name='id']"
            
            ,"//result/doc[count(*)=2]"
            );

    for (SolrParams p : Arrays.asList(params("fl","val_*,subj*,id"),
                                      params("fl","val_*","fl","subj*","fl","id"),
                                      params("fl","val_*","fl","subj*,id"))) {
      assertQ("" + p,
              req(p, "q","*:*", "rows", "1")
              ,"//result[@numFound='5']"
              ,"//result/doc/int[@name='val_i']"
              ,"//result/doc/str[@name='subject']"
              ,"//result/doc/str[@name='id']"
              
              ,"//result/doc[count(*)=3]"
              );
    }
  }

  public void testGlobsAndExplicitRTG() throws Exception {
    // behavior shouldn't matter if we are committed or uncommitted
    for (String id : Arrays.asList("42","99")) {
      assertQ(id + " + fl=val_*,id",
              req("qt","/get","id",id, "wt","xml", "fl","val_*,id")
              ,"count(//doc)=1"
              ,"//doc/int[@name='val_i'][.=1]"
              ,"//doc/str[@name='id']"
              
              ,"//doc[count(*)=2]"
              );

      for (SolrParams p : Arrays.asList(params("fl","val_*,subj*,id"),
                                        params("fl","val_*","fl","subj*","fl","id"),
                                        params("fl","val_*","fl","subj*,id"))) {
        assertQ(id + " + " + p,
                req(p, "qt","/get","id",id, "wt","xml")
                ,"count(//doc)=1"
                ,"//doc/int[@name='val_i'][.=1]"
                ,"//doc/str[@name='subject']"
                ,"//doc/str[@name='id']"
                
                ,"//doc[count(*)=3]"
                );
      }
    }
  }

  public void testGlobsAndScore() throws Exception {
    assertQ("fl=val_*,score",
            req("q","*:*", "rows", "1", "fl","val_*,score", "indent", "true")
            ,"//result[@numFound='5']"
            ,"//result/doc/float[@name='score']"
            ,"//result/doc/int[@name='val_i']"
            
            ,"//result/doc[count(*)=2]"
            );
    for (SolrParams p : Arrays.asList(params("fl","val_*,subj*,score"),
                                      params("fl","val_*","fl","subj*","fl","score"),
                                      params("fl","val_*","fl","subj*,score"))) {
      assertQ("" + p,
              req(p, "q","*:*", "rows", "1")
              ,"//result[@numFound='5']"
              ,"//result/doc/float[@name='score']"
              ,"//result/doc/int[@name='val_i']"
              ,"//result/doc/str[@name='subject']"
              
              ,"//result/doc[count(*)=3]"
              );
    }
  }
  
  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-9287")
  public void testGlobsAndScoreRTG() throws Exception {
    // behavior shouldn't matter if we are committed or uncommitted, score should be ignored
    for (String id : Arrays.asList("42","99")) {
      assertQ(id + ": fl=val_*,score",
              req("qt","/get","id",id, "wt","xml", "fl","val_*,score")
              ,"count(//doc)=1"
              ,"//doc/int[@name='val_i']"
              ,"//doc[count(*)=1]"
              );
      for (SolrParams p : Arrays.asList(params("fl","val_*,subj*,score"),
                                        params("fl","val_*","fl","subj*","fl","score"),
                                        params("fl","val_*","fl","subj*,score"))) {
        assertQ("" + p,
                req(p, "qt","/get","id",id, "wt","xml")
                ,"count(//doc)=1"
                ,"//doc/int[@name='val_i']"
                ,"//doc/str[@name='subject']"
                ,"//doc[count(*)=2]"
                );
      }
    }
  }

  public void testAugmenters() throws Exception {
    assertQ("fl=[docid]",
            req("q","*:*", "rows", "1", "fl","[docid]")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='[docid]']"
            ,"//result/doc[count(*)=1]"
            );
    for (SolrParams p : Arrays.asList(params("fl","[docid],[shard],[explain],x_alias:[value v=10 t=int]"),
                                      params("fl","[docid],[shard]","fl","[explain],x_alias:[value v=10 t=int]"),
                                      params("fl","[docid]","fl","[shard]","fl","[explain]","fl","x_alias:[value v=10 t=int]"))) {
      assertQ("" + p,
              req(p, "q","*:*", "rows", "1")
              ,"//result[@numFound='5']"
              ,"//result/doc/int[@name='[docid]']"
              ,"//result/doc/str[@name='[shard]'][.='[not a shard request]']"
              ,"//result/doc/str[@name='[explain]']"
              ,"//result/doc/int[@name='x_alias'][.=10]"
              
              ,"//result/doc[count(*)=4]"
              );
    }
  }

  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-9288")
  public void testDocIdAugmenterRTG() throws Exception {
    // NOTE: once this test is fixed to pass, testAugmentersRTG should also be updated to test [docid]

    // TODO: behavior of fl=[docid] should be consistent regardless of wether doc is committed
    // what should behavior be?
    // right now, for an uncommited doc, [docid] is silently ignored and no value included in result
    // perhaps it should be "null" or "-1" ?
    
    // behavior shouldn't matter if we are committed or uncommitted
    for (String id : Arrays.asList("42","99")) {
      assertQ(id + ": fl=[docid]",
              req("qt","/get","id",id, "wt","xml", "fl","[docid]")
              ,"count(//doc)=1"
              ,"//doc/int[@name='[docid]']"
              ,"//doc[count(*)=1]"
              );
    }
  }
  
  public void testAugmentersRTG() throws Exception {
    // behavior shouldn't matter if we are committed or uncommitted
    for (String id : Arrays.asList("42","99")) {
      // NOTE: once testDocIdAugmenterRTG can pass, [docid] should be tested here as well.
      for (SolrParams p : Arrays.asList(params("fl","[shard],[explain],x_alias:[value v=10 t=int]"),
                                        params("fl","[shard]","fl","[explain],x_alias:[value v=10 t=int]"),
                                        params("fl","[shard]","fl","[explain]","fl","x_alias:[value v=10 t=int]"))) {
        assertQ(id + ": " + p,
                req(p, "qt","/get","id",id, "wt","xml")
                ,"count(//doc)=1"
                // ,"//doc/int[@name='[docid]']" // TODO
                ,"//doc/str[@name='[shard]'][.='[not a shard request]']"
                // RTG: [explain] should be missing (ignored)
                ,"//doc/int[@name='x_alias'][.=10]"
                
                ,"//doc[count(*)=2]"
                );
      }
    }
  }

  public void testAugmentersAndExplicit() throws Exception {
    for (SolrParams p : Arrays.asList(params("fl","id,[docid],[explain],x_alias:[value v=10 t=int]"),
                                      params("fl","id","fl","[docid],[explain],x_alias:[value v=10 t=int]"),
                                      params("fl","id","fl","[docid]","fl","[explain]","fl","x_alias:[value v=10 t=int]"))) {
      assertQ(p.toString(),
              req(p, "q","*:*", "rows", "1")
              ,"//result[@numFound='5']"
              ,"//result/doc/str[@name='id']"
              ,"//result/doc/int[@name='[docid]']"
              ,"//result/doc/str[@name='[explain]']"
              ,"//result/doc/int[@name='x_alias'][.=10]"
            
              ,"//result/doc[count(*)=4]"
              );
    }
  }
  
  public void testAugmentersAndExplicitRTG() throws Exception {
    // behavior shouldn't matter if we are committed or uncommitted
    for (String id : Arrays.asList("42","99")) {
      // NOTE: once testDocIdAugmenterRTG can pass, [docid] should be tested here as well.
      for (SolrParams p : Arrays.asList(params("fl","id,[explain],x_alias:[value v=10 t=int]"),
                                        params("fl","id","fl","[explain],x_alias:[value v=10 t=int]"),
                                        params("fl","id","fl","[explain]","fl","x_alias:[value v=10 t=int]"))) {
        assertQ(id + ": " + p,
                req(p, "qt","/get","id",id, "wt","xml")
                ,"count(//doc)=1"
                ,"//doc/str[@name='id']"
                // ,"//doc/int[@name='[docid]']" // TODO
                // RTG: [explain] should be missing (ignored)
                ,"//doc/int[@name='x_alias'][.=10]"
                
                ,"//doc[count(*)=2]"
              );
      }
    }
  }

  public void testAugmentersAndScore() throws Exception {
    assertQ(req("q","*:*", "rows", "1",
                "fl","[docid],x_alias:[value v=10 t=int],score")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='[docid]']"
            ,"//result/doc/int[@name='x_alias'][.=10]"
            ,"//result/doc/float[@name='score']"
            
            ,"//result/doc[count(*)=3]"
            );
    for (SolrParams p : Arrays.asList(params("fl","[docid],x_alias:[value v=10 t=int],[explain],score"),
                                      params("fl","[docid]","fl","x_alias:[value v=10 t=int],[explain]","fl","score"),
                                      params("fl","[docid]","fl","x_alias:[value v=10 t=int]","fl","[explain]","fl","score"))) {

      assertQ(p.toString(),
              req(p, "q","*:*", "rows", "1")
              ,"//result[@numFound='5']"
              
              ,"//result/doc/int[@name='[docid]']"
              ,"//result/doc/int[@name='x_alias'][.=10]"
              ,"//result/doc/str[@name='[explain]']"
              ,"//result/doc/float[@name='score']"
              
              ,"//result/doc[count(*)=4]"
              );
    }
  }
  
  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-9287")
  public void testAugmentersAndScoreRTG() throws Exception {
    // if we use RTG (committed or otherwise) score should be ignored
    for (String id : Arrays.asList("42","99")) {
      // NOTE: once testDocIdAugmenterRTG can pass, [docid] should be tested here as well.
      assertQ(id,
              req("qt","/get","id",id, "wt","xml",
                  "fl","x_alias:[value v=10 t=int],score")
              // ,"//doc/int[@name='[docid]']" // TODO
              ,"//doc/int[@name='x_alias'][.=10]"
              
              ,"//doc[count(*)=1]"
              );
      for (SolrParams p : Arrays.asList(params("fl","x_alias:[value v=10 t=int],[explain],score"),
                                        params("fl","x_alias:[value v=10 t=int],[explain]","fl","score"),
                                        params("fl","x_alias:[value v=10 t=int]","fl","[explain]","fl","score"))) {
        
        assertQ(p.toString(),
                req(p, "qt","/get","id",id, "wt","xml")
                
                // ,"//doc/int[@name='[docid]']" // TODO
                ,"//doc/int[@name='x_alias'][.=10]"
                // RTG: [explain] and score should be missing (ignored)
                
                ,"//doc[count(*)=1]"
                );
      }
    }
  }

  public void testAugmentersGlobsExplicitAndScoreOhMy() throws Exception {
    Random random = random();

    // NOTE: 'ssto' is the missing one
    final List<String> fl = Arrays.asList
      ("id","[docid]","[explain]","score","val_*","subj*");
    
    final int iters = atLeast(random, 10);
    for (int i = 0; i< iters; i++) {
      
      Collections.shuffle(fl, random);

      final SolrParams singleFl = params("q","*:*", "rows", "1","fl",StringUtils.join(fl.toArray(),','));
      final ModifiableSolrParams multiFl = params("q","*:*", "rows", "1");
      for (String item : fl) {
        multiFl.add("fl",item);
      }
      for (SolrParams p : Arrays.asList(singleFl, multiFl)) {
        assertQ(p.toString(),
                req(p)
                ,"//result[@numFound='5']"
                ,"//result/doc/str[@name='id']"
                ,"//result/doc/float[@name='score']"
                ,"//result/doc/str[@name='subject']"
                ,"//result/doc/int[@name='val_i']"
                ,"//result/doc/int[@name='[docid]']"
                ,"//result/doc/str[@name='[explain]']"
                
                ,"//result/doc[count(*)=6]"
                );
      }
    }
  }
  
  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-9287")
  public void testAugmentersGlobsExplicitAndScoreOhMyRTG() throws Exception {
    Random random = random();

    // NOTE: 'ssto' is the missing one
    final List<String> fl = Arrays.asList
      // NOTE: once testDocIdAugmenterRTG can pass, [docid] should be tested here as well.
      ("id","[explain]","score","val_*","subj*");
    
    final int iters = atLeast(random, 10);
    for (int i = 0; i< iters; i++) {
      
      Collections.shuffle(fl, random);

      final SolrParams singleFl = params("fl",StringUtils.join(fl.toArray(),','));
      final ModifiableSolrParams multiFl = params();
      for (String item : fl) {
        multiFl.add("fl",item);
      }

      // RTG behavior should be consistent, (committed or otherwise) 
      for (String id : Arrays.asList("42","99")) { 
        for (SolrParams p : Arrays.asList(singleFl, multiFl)) {
          assertQ(id + ": " + p,
                  req(p, "qt","/get","id",id, "wt","xml")
                  ,"count(//doc)=1"
                  ,"//doc/str[@name='id']"
                  // ,"//doc/int[@name='[docid]']" // TODO
                  // RTG: [explain] and score should be missing (ignored)
                  ,"//doc/int[@name='val_i'][.=1]"
                  ,"//doc/str[@name='subject']"
                  ,"//result/doc[count(*)=3]"
                  );
        }
      }
    }
  }
}
