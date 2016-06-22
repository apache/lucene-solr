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
package org.apache.solr.response.transform;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestChildDocTransformer extends SolrTestCaseJ4 {

  private static String ID_FIELD = "id";
  private String[] titleVals = new String[2];

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }

  @After
  public void cleanup() throws Exception {
    assertU(delQ("*:*"));
    assertU(commit());
  }

  @Test
  public void testParentFilter() throws Exception {
    for(int i=0; i<titleVals.length; i++) {
      titleVals[i] = TestUtil.randomSimpleString(random(), 1, 20);
    }
    createIndex(titleVals);
    testParentFilterJSON();
    testParentFilterXML();
    testSubQueryParentFilterJSON(); 
    testSubQueryParentFilterXML();
  }

  @Test
  public void testAllParams() throws Exception {
    createSimpleIndex();
    testChildDoctransformerJSON();
    testChildDoctransformerXML();
    
    testSubQueryXML();
    testSubQueryJSON();
  }

  private void testChildDoctransformerXML() {
    String test1[] = new String[] {
        "//*[@numFound='1']",
        "/response/result/doc[1]/doc[1]/int[@name='id']='2'" ,
        "/response/result/doc[1]/doc[2]/int[@name='id']='3'" ,
        "/response/result/doc[1]/doc[3]/int[@name='id']='4'" ,
        "/response/result/doc[1]/doc[4]/int[@name='id']='5'" ,
        "/response/result/doc[1]/doc[5]/int[@name='id']='6'" ,
        "/response/result/doc[1]/doc[6]/int[@name='id']='7'"};

    String test2[] = new String[] {
        "//*[@numFound='1']",
        "/response/result/doc[1]/doc[1]/int[@name='id']='2'" ,
        "/response/result/doc[1]/doc[2]/int[@name='id']='4'" ,
        "/response/result/doc[1]/doc[3]/int[@name='id']='6'" };

    String test3[] = new String[] {
        "//*[@numFound='1']",
        "/response/result/doc[1]/doc[1]/int[@name='id']='3'" ,
        "/response/result/doc[1]/doc[2]/int[@name='id']='5'" };



    assertQ(req("q", "*:*", "fq", "subject:\"parentDocument\" ",
        "fl", "*,[child parentFilter=\"subject:parentDocument\"]"), test1);

    assertQ(req("q", "*:*", "fq", "subject:\"parentDocument\" ",
        "fl", "subject,[child parentFilter=\"subject:parentDocument\" childFilter=\"title:foo\"]"), test2);

    assertQ(req("q", "*:*", "fq", "subject:\"parentDocument\" ",
        "fl", "subject,[child parentFilter=\"subject:parentDocument\" childFilter=\"title:bar\" limit=2]"), test3);
  }
  
  private void testSubQueryXML() {
    String test1[];
    {
    final String subqueryPath = "/result[@name='children'][@numFound='6']"; 
    test1 = new String[] {
        "//*[@numFound='1']",
        "/response/result/doc[1]" + subqueryPath + "/doc[1]/int[@name='id']='2'" ,
        "/response/result/doc[1]" + subqueryPath + "/doc[2]/int[@name='id']='3'" ,
        "/response/result/doc[1]" + subqueryPath + "/doc[3]/int[@name='id']='4'" ,
        "/response/result/doc[1]" + subqueryPath + "/doc[4]/int[@name='id']='5'" ,
        "/response/result/doc[1]" + subqueryPath + "/doc[5]/int[@name='id']='6'" ,
        "/response/result/doc[1]" + subqueryPath + "/doc[6]/int[@name='id']='7'"};
    }

    assertQ(req("q", "*:*", "fq", "subject:\"parentDocument\" ",
        "fl", "*,children:[subquery]",
        "children.q","{!child of=subject:parentDocument}{!terms f=id v=$row.id}",
        "children.rows","10"), 
        test1);

    String test2[] = new String[] {
        "//*[@numFound='1']",
        "/response/result/doc[1]/result[@name='children'][@numFound='3']/doc[1]/int[@name='id']='2'" ,
        "/response/result/doc[1]/result[@name='children'][@numFound='3']/doc[2]/int[@name='id']='4'" ,
        "/response/result/doc[1]/result[@name='children'][@numFound='3']/doc[3]/int[@name='id']='6'" };
    
    assertQ(req("q", "*:*", "fq", "subject:\"parentDocument\" ",
        "fl", "*,children:[subquery]",
        "children.q","{!child of=subject:parentDocument}{!terms f=id v=$row.id}",
        "children.rows","10",
        "children.fq","title:foo"
        ), test2);
    

    String test3[] = new String[] {
        "//*[@numFound='1']",
        "/response/result/doc[1]/result[@name='children'][@numFound='3']/doc[1]/int[@name='id']='3'" ,
        "/response/result/doc[1]/result[@name='children'][@numFound='3']/doc[2]/int[@name='id']='5'" };

    
    assertQ(req("q", "*:*", "fq", "subject:\"parentDocument\" ",
        "fl", "*,children:[subquery]",
        "children.q","{!child of=subject:parentDocument}{!terms f=id v=$row.id}",
        "children.rows","2",
        "children.fq","title:bar",
        "children.sort","_docid_ asc"
        ), test3);
  }
  
  private void testSubQueryJSON() throws Exception {
    String[] test1 = new String[] {
        "/response/docs/[0]/children/docs/[0]/id==2",
        "/response/docs/[0]/children/docs/[1]/id==3",
        "/response/docs/[0]/children/docs/[2]/id==4",
        "/response/docs/[0]/children/docs/[3]/id==5",
        "/response/docs/[0]/children/docs/[4]/id==6",
        "/response/docs/[0]/children/docs/[5]/id==7"
    };

    String[] test2 = new String[] {
        "/response/docs/[0]/children/docs/[0]/id==2",
        "/response/docs/[0]/children/docs/[1]/id==4",
        "/response/docs/[0]/children/docs/[2]/id==6"
    };

    String[] test3 = new String[] {
        "/response/docs/[0]/children/docs/[0]/id==3",
        "/response/docs/[0]/children/docs/[1]/id==5"
    };


    assertJQ(req("q", "*:*", "fq", "subject:\"parentDocument\" ",
        "fl", "*,children:[subquery]",
        "children.q","{!child of=subject:parentDocument}{!terms f=id v=$row.id}",
        "children.rows","10"), test1);

    assertJQ(req("q", "*:*", "fq", "subject:\"parentDocument\" ",
        "fl", "*,children:[subquery]",
        "children.q","{!child of=subject:parentDocument}{!terms f=id v=$row.id}",
        "children.rows","10",
        "children.fq","title:foo"), test2);

    assertJQ(req("q", "*:*", "fq", "subject:\"parentDocument\" ",
        "fl", "*,children:[subquery]",
        "children.q","{!child of=subject:parentDocument}{!terms f=id v=$row.id}",
        "children.rows","2",
        "children.fq","title:bar",
        "children.sort","_docid_ asc"), test3);
  }

  private void testChildDoctransformerJSON() throws Exception {
    String[] test1 = new String[] {
        "/response/docs/[0]/_childDocuments_/[0]/id==2",
        "/response/docs/[0]/_childDocuments_/[1]/id==3",
        "/response/docs/[0]/_childDocuments_/[2]/id==4",
        "/response/docs/[0]/_childDocuments_/[3]/id==5",
        "/response/docs/[0]/_childDocuments_/[4]/id==6",
        "/response/docs/[0]/_childDocuments_/[5]/id==7"
    };

    String[] test2 = new String[] {
        "/response/docs/[0]/_childDocuments_/[0]/id==2",
        "/response/docs/[0]/_childDocuments_/[1]/id==4",
        "/response/docs/[0]/_childDocuments_/[2]/id==6"
    };

    String[] test3 = new String[] {
        "/response/docs/[0]/_childDocuments_/[0]/id==3",
        "/response/docs/[0]/_childDocuments_/[1]/id==5"
    };


    assertJQ(req("q", "*:*", "fq", "subject:\"parentDocument\" ",
        "fl", "*,[child parentFilter=\"subject:parentDocument\"]"), test1);

    assertJQ(req("q", "*:*", "fq", "subject:\"parentDocument\" ",
        "fl", "subject,[child parentFilter=\"subject:parentDocument\" childFilter=\"title:foo\"]"), test2);

    assertJQ(req("q", "*:*", "fq", "subject:\"parentDocument\" ",
        "fl", "subject,[child parentFilter=\"subject:parentDocument\" childFilter=\"title:bar\" limit=2]"), test3);
  }

  private void createSimpleIndex() {

    SolrInputDocument parentDocument = new SolrInputDocument();
    parentDocument.addField(ID_FIELD, "1");
    parentDocument.addField("subject", "parentDocument");
    for(int i=0; i< 6; i++) {
      SolrInputDocument childDocument = new SolrInputDocument();
      childDocument.addField(ID_FIELD, Integer.toString(i+2));
      if(i%2==0) {
        childDocument.addField("title", "foo");
      } else {
        childDocument.addField("title", "bar");
      }

      parentDocument.addChildDocument(childDocument);
    }
    try {
      Long version = addAndGetVersion(parentDocument, null);
      assertNotNull(version);
    } catch (Exception e) {
      fail("Failed to add document to the index");
    }
    assertU(commit());
    assertQ(req("q", "*:*"), "//*[@numFound='" + 7 + "']");
  }


  private static void createIndex(String[] titleVals) {

    String[] parentIDS = new String[] {"1", "4"};
    String[] childDocIDS = new String[] {"2", "5"};
    String[] grandChildIDS = new String[] {"3", "6"};

    for(int i=0; i< parentIDS.length; i++) {
      SolrInputDocument parentDocument = new SolrInputDocument();
      parentDocument.addField(ID_FIELD, parentIDS[i]);
      parentDocument.addField("subject", "parentDocument");
      parentDocument.addField("title", titleVals[i]);

      SolrInputDocument childDocument = new SolrInputDocument();
      childDocument.addField(ID_FIELD, childDocIDS[i]);
      childDocument.addField("cat", "childDocument");
      childDocument.addField("title", titleVals[i]);

      SolrInputDocument grandChildDocument = new SolrInputDocument();
      grandChildDocument.addField(ID_FIELD, grandChildIDS[i]);

      childDocument.addChildDocument(grandChildDocument);
      parentDocument.addChildDocument(childDocument);

      try {
        Long version = addAndGetVersion(parentDocument, null);
        assertNotNull(version);
      } catch (Exception e) {
        fail("Failed to add document to the index");
      }
      if (random().nextBoolean()) {
        assertU(commit());
      }
    }

    assertU(commit());
    assertQ(req("q", "*:*"), "//*[@numFound='" + (parentIDS.length + childDocIDS.length + grandChildIDS.length) + "']");

  }

  private void testParentFilterJSON() throws Exception {

    String[] tests = new String[] {
        "/response/docs/[0]/id==1",
        "/response/docs/[0]/_childDocuments_/[0]/id==2",
        "/response/docs/[0]/_childDocuments_/[0]/cat/[0]/=='childDocument'",
        "/response/docs/[0]/_childDocuments_/[0]/title/[0]/=='" + titleVals[0] + "'",
        "/response/docs/[1]/id==4",
        "/response/docs/[1]/_childDocuments_/[0]/id==5",
        "/response/docs/[1]/_childDocuments_/[0]/cat/[0]/=='childDocument'",
        "/response/docs/[1]/_childDocuments_/[0]/title/[0]/=='" + titleVals[1] + "'"
    };


    assertJQ(req("q", "*:*", 
                 "sort", "id asc",
                 "fq", "subject:\"parentDocument\" ",
                 "fl", "*,[child childFilter='cat:childDocument' parentFilter=\"subject:parentDocument\"]"), 
             tests);

    assertJQ(req("q", "*:*", 
                 "sort", "id asc",
                 "fq", "subject:\"parentDocument\" ",
                 "fl", "id,[child childFilter='cat:childDocument' parentFilter=\"subject:parentDocument\"]"), 
             tests);

  }
  
  private void testSubQueryParentFilterJSON() throws Exception {

    String[] tests = new String[] {
        "/response/docs/[0]/id==1",
        "/response/docs/[0]/children/docs/[0]/id==2",
        "/response/docs/[0]/children/docs/[0]/cat/[0]/=='childDocument'",
        "/response/docs/[0]/children/docs/[0]/title/[0]/=='" + titleVals[0] + "'",
        "/response/docs/[1]/id==4",
        "/response/docs/[1]/children/docs/[0]/id==5",
        "/response/docs/[1]/children/docs/[0]/cat/[0]/=='childDocument'",
        "/response/docs/[1]/children/docs/[0]/title/[0]/=='" + titleVals[1] + "'"
    };


    assertJQ(req(
        "q", "*:*", "fq", "subject:\"parentDocument\" ",
        "fl", "*,children:[subquery]",
        "sort", "id asc",
        "children.q","{!child of=subject:parentDocument}{!terms f=id v=$row.id}",
        "children.fq","cat:childDocument",
        "children.sort","_docid_ asc"),
             tests);
    assertJQ(req(
        "q", "*:*", "fq", "subject:\"parentDocument\" ",
        "fl", "id,children:[subquery]",
        "sort", "id asc",
        "children.q","{!child of=subject:parentDocument}{!terms f=id v=$row.id}",
        "children.fq","cat:childDocument",
        "children.sort","_docid_ asc"),
             tests);
  }

  private void testParentFilterXML() {

    String tests[] = new String[] {
        "//*[@numFound='2']",
        "/response/result/doc[1]/int[@name='id']='1'" ,
        "/response/result/doc[1]/doc[1]/int[@name='id']='2'" ,
        "/response/result/doc[1]/doc[1]/arr[@name='cat']/str[1]='childDocument'" ,
        "/response/result/doc[1]/doc[1]/arr[@name='title']/str[1]='" + titleVals[0] + "'" ,
        "/response/result/doc[2]/int[@name='id']='4'" ,
        "/response/result/doc[2]/doc[1]/int[@name='id']='5'",
        "/response/result/doc[2]/doc[1]/arr[@name='cat']/str[1]='childDocument'",
        "/response/result/doc[2]/doc[1]/arr[@name='title']/str[1]='" + titleVals[1] + "'"};

    assertQ(req("q", "*:*", 
                "sort", "id asc",
                "fq", "subject:\"parentDocument\" ",
                "fl", "*,[child childFilter='cat:childDocument' parentFilter=\"subject:parentDocument\"]"), 
            tests);

    assertQ(req("q", "*:*", 
                "sort", "id asc",
                "fq", "subject:\"parentDocument\" ",
                "fl", "id,[child childFilter='cat:childDocument' parentFilter=\"subject:parentDocument\"]"), 
            tests);
  }

  private void testSubQueryParentFilterXML() {

    String tests[] = new String[] {
        "//*[@numFound='2']",
        "/response/result/doc[1]/int[@name='id']='1'" ,
        "/response/result/doc[1]/result[@name='children'][@numFound=1]/doc[1]/int[@name='id']='2'" ,
        "/response/result/doc[1]/result[@name='children'][@numFound=1]/doc[1]/arr[@name='cat']/str[1]='childDocument'" ,
        "/response/result/doc[1]/result[@name='children'][@numFound=1]/doc[1]/arr[@name='title']/str[1]='" + titleVals[0] + "'" ,
        "/response/result/doc[2]/int[@name='id']='4'" ,
        "/response/result/doc[2]/result[@name='children'][@numFound=1]/doc[1]/int[@name='id']='5'",
        "/response/result/doc[2]/result[@name='children'][@numFound=1]/doc[1]/arr[@name='cat']/str[1]='childDocument'",
        "/response/result/doc[2]/result[@name='children'][@numFound=1]/doc[1]/arr[@name='title']/str[1]='" + titleVals[1] + "'"};

    assertQ(req(
        "q", "*:*", "fq", "subject:\"parentDocument\" ",
        "fl", "*,children:[subquery]",
        "sort", "id asc",
        "children.q","{!child of=subject:parentDocument}{!terms f=id v=$row.id}",
        "children.fq","cat:childDocument",
        "children.sort","_docid_ asc"
       ), 
            tests);

    assertQ(req("q", "*:*", "fq", "subject:\"parentDocument\" ",
        "fl", "id,children:[subquery]",
        "sort", "id asc",
        "children.q","{!child of=subject:parentDocument}{!terms f=id v=$row.id}",
        "children.fq","cat:childDocument",
        "children.sort","_docid_ asc"), 
            tests);
  }
  
}
