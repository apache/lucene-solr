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
    assertU(adoc("id","3", "foo_s","a ' \" \\ {! ) } ( { z"));  // A value filled with special chars

    assertU(adoc("id","10", "qqq_s","X"));
    assertU(adoc("id","11", "www_s","X"));
    assertU(adoc("id","12", "eee_s","X"));
    assertU(adoc("id","13", "eee_s","'balance'"));

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
  public void testLocalParamsInQP() throws Exception {
    assertJQ(req("q","qaz {!term f=text v=$qq} wsx", "qq","now")
        ,"/response/numFound==2"
    );

    assertJQ(req("q","qaz {!term f=text v=$qq} wsx", "qq","nomatch")
        ,"/response/numFound==0"
    );

    assertJQ(req("q","qaz {!term f=text}now wsx", "qq","now")
        ,"/response/numFound==2"
    );

    assertJQ(req("q","qaz {!term f=foo_s v='a \\' \" \\\\ {! ) } ( { z'} wsx")           // single quote escaping
        ,"/response/numFound==1"
    );

    assertJQ(req("q","qaz {!term f=foo_s v=\"a ' \\\" \\\\ {! ) } ( { z\"} wsx")         // double quote escaping
        ,"/response/numFound==1"
    );

    // double-join to test back-to-back local params
    assertJQ(req("q","qaz {!join from=www_s to=eee_s}{!join from=qqq_s to=www_s}id:10" )
        ,"/response/docs/[0]/id=='12'"
    );
  }

  @Test
  public void testSolr4121() throws Exception {
    // At one point, balanced quotes messed up the parser(SOLR-4121)
    assertJQ(req("q","eee_s:'balance'", "indent","true")
        ,"/response/numFound==1"
    );
  }

  @Test
  public void testSyntax() throws Exception {
    // a bare * should be treated as *:*
    assertJQ(req("q","*", "df","doesnotexist_s")
        ,"/response/docs/[0]=="   // make sure we get something...
    );
    assertJQ(req("q","doesnotexist_s:*")
        ,"/response/numFound==0"   // nothing should be found
    );
    assertJQ(req("q","doesnotexist_s:( * * * )")
        ,"/response/numFound==0"   // nothing should be found
    );
  }

}
