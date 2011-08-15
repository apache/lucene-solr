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

import org.apache.commons.lang.StringUtils;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class TestPseudoReturnFields extends SolrTestCaseJ4 {

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
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml","schema12.xml");


    assertU(adoc("id", "42", "val_i", "1", "ssto", "X", "subject", "aaa"));
    assertU(adoc("id", "43", "val_i", "9", "ssto", "X", "subject", "bbb"));
    assertU(adoc("id", "44", "val_i", "4", "ssto", "X", "subject", "aaa"));
    assertU(adoc("id", "45", "val_i", "6", "ssto", "X", "subject", "aaa"));
    assertU(adoc("id", "46", "val_i", "3", "ssto", "X", "subject", "ggg"));
    assertU(commit());
  }
  
  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
  public void testGlobs() throws Exception {
    assertQ("fl=val_*",
            req("q","*:*", "rows", "1", "fl","val_*")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='val_i']"
            
            ,"//result/doc[count(*)=1]"
            );

    assertQ("fl=val_*,subj*",
            req("q","*:*", "rows", "1", "fl","val_*,subj*")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='val_i']"
            ,"//result/doc/str[@name='subject']"
            
            ,"//result/doc[count(*)=2]"
            );
    assertQ("fl=val_*&fl=subj*",
            req("q","*:*", "rows", "1", "fl","val_*","fl","subj*")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='val_i']"
            ,"//result/doc/str[@name='subject']"
            
            ,"//result/doc[count(*)=2]"
            );
  }

  @Test
  public void testGlobsAndExplicit() throws Exception {
    assertQ("fl=val_*,id",
            req("q","*:*", "rows", "1", "fl","val_*,id")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='val_i']"
            ,"//result/doc/str[@name='id']"
            
            ,"//result/doc[count(*)=2]"
            );

    assertQ("fl=val_*,subj*,id",
            req("q","*:*", "rows", "1", "fl","val_*,subj*,id")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='val_i']"
            ,"//result/doc/str[@name='subject']"
            ,"//result/doc/str[@name='id']"
            
            ,"//result/doc[count(*)=3]"
            );
    assertQ("fl=val_*&fl=subj*&fl=id",
            req("q","*:*", "rows", "1", "fl","val_*","fl","subj*","fl","id")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='val_i']"
            ,"//result/doc/str[@name='subject']"
            ,"//result/doc/str[@name='id']"
            
            ,"//result/doc[count(*)=3]"
            );
  }

  @Test
  public void testGlobsAndScore() throws Exception {
    assertQ("fl=val_*,score",
            req("q","*:*", "rows", "1", "fl","val_*,score", "indent", "true")
            ,"//result[@numFound='5']"
            ,"//result/doc/float[@name='score']"
            ,"//result/doc/int[@name='val_i']"
            
            ,"//result/doc[count(*)=2]"
            );

    assertQ("fl=val_*,subj*,score",
            req("q","*:*", "rows", "1", "fl","val_*,subj*,score")
            ,"//result[@numFound='5']"
            ,"//result/doc/float[@name='score']"
            ,"//result/doc/int[@name='val_i']"
            ,"//result/doc/str[@name='subject']"
            
            ,"//result/doc[count(*)=3]"
            );
    assertQ("fl=val_*&fl=subj*&fl=score",
            req("q","*:*", "rows", "1", 
                "fl","val_*","fl","subj*","fl","score")
            ,"//result[@numFound='5']"
            ,"//result/doc/float[@name='score']"
            ,"//result/doc/int[@name='val_i']"
            ,"//result/doc/str[@name='subject']"
            
            ,"//result/doc[count(*)=3]"
            );

    
  }

  @Test
  public void testAugmenters() throws Exception {
    assertQ("fl=[docid]",
            req("q","*:*", "rows", "1", "fl","[docid]")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='[docid]']"
            
            ,"//result/doc[count(*)=1]"
            );

    assertQ("fl=[docid],[explain]",
            req("q","*:*", "rows", "1", "fl","[docid],[explain]")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='[docid]']"
            ,"//result/doc/str[@name='[explain]']"
            
            ,"//result/doc[count(*)=2]"
            );
    assertQ("fl=[docid]&fl=[explain]",
            req("q","*:*", "rows", "1", "fl","[docid]","fl","[explain]")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='[docid]']"
            ,"//result/doc/str[@name='[explain]']"
            
            ,"//result/doc[count(*)=2]"
            );
  }

  @Test
  public void testAugmentersAndExplicit() throws Exception {
    assertQ("fl=[docid],id",
            req("q","*:*", "rows", "1", 
                "fl","[docid],id")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='[docid]']"
            ,"//result/doc/str[@name='id']"
            
            ,"//result/doc[count(*)=2]"
            );

    assertQ("fl=[docid],[explain],id",
            req("q","*:*", "rows", "1", 
                "fl","[docid],[explain],id")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='[docid]']"
            ,"//result/doc/str[@name='[explain]']"
            ,"//result/doc/str[@name='id']"
            
            ,"//result/doc[count(*)=3]"
            );
    assertQ("fl=[docid]&fl=[explain]&fl=id",
            req("q","*:*", "rows", "1", 
                "fl","[docid]","fl","[explain]","fl","id")
            ,"//result[@numFound='5']"
            ,"//result/doc/int[@name='[docid]']"
            ,"//result/doc/str[@name='[explain]']"
            ,"//result/doc/str[@name='id']"
            
            ,"//result/doc[count(*)=3]"
            );
  }

  @Test
  public void testAugmentersAndScore() throws Exception {
    assertQ("fl=[docid],score",
            req("q","*:*", "rows", "1",
                "fl","[docid],score")
            ,"//result[@numFound='5']"
            ,"//result/doc/float[@name='score']"
            ,"//result/doc/int[@name='[docid]']"
            
            ,"//result/doc[count(*)=2]"
            );

    assertQ("fl=[docid],[explain],score",
            req("q","*:*", "rows", "1",
                "fl","[docid],[explain],score")
            ,"//result[@numFound='5']"
            ,"//result/doc/float[@name='score']"
            ,"//result/doc/int[@name='[docid]']"
            ,"//result/doc/str[@name='[explain]']"
            
            ,"//result/doc[count(*)=3]"
            );
    assertQ("fl=[docid]&fl=[explain]&fl=score",
            req("q","*:*", "rows", "1", 
                "fl","[docid]","fl","[explain]","fl","score")
            ,"//result[@numFound='5']"
            ,"//result/doc/float[@name='score']"
            ,"//result/doc/int[@name='[docid]']"
            ,"//result/doc/str[@name='[explain]']"
            
            ,"//result/doc[count(*)=3]"
            );
  }

  @Test
  public void testAugmentersGlobsExplicitAndScoreOhMy() throws Exception {

    // NOTE: 'ssto' is the missing one
    final List<String> fl = Arrays.asList
      ("id","[docid]","[explain]","score","val_*","subj*");
    
    final int iters = atLeast(random, 10);
    for (int i = 0; i< iters; i++) {
      
      Collections.shuffle(fl, random);

      final String singleFl = StringUtils.join(fl.toArray(),',');
      assertQ("fl=" + singleFl,
              req("q","*:*", "rows", "1","fl",singleFl)
              ,"//result[@numFound='5']"
              ,"//result/doc/str[@name='id']"
              ,"//result/doc/float[@name='score']"
              ,"//result/doc/str[@name='subject']"
              ,"//result/doc/int[@name='val_i']"
              ,"//result/doc/int[@name='[docid]']"
              ,"//result/doc/str[@name='[explain]']"
              
              ,"//result/doc[count(*)=6]"
              );

      final List<String> params = new ArrayList<String>((fl.size()*2) + 4);
      final StringBuilder info = new StringBuilder();
      params.addAll(Arrays.asList("q","*:*", "rows", "1"));
      for (String item : fl) {
        params.add("fl");
        params.add(item);
        info.append("&fl=").append(item);
      }
      
      assertQ(info.toString(),
              req((String[])params.toArray(new String[0]))
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
