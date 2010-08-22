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

package org.apache.solr;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestGroupingSearch extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml","schema12.xml");
  }

  @Before
  public void cleanIndex() {
    assertU(delQ("*:*"));
    assertU(commit());
  }

  @Test
  public void testGroupingGroupSortingScore_basic() {
    assertU(add(doc("id", "1","name", "author1", "title", "a book title")));
    assertU(add(doc("id", "2","name", "author1", "title", "the title")));
    assertU(add(doc("id", "3","name", "author2", "title", "a book title")));
    assertU(add(doc("id", "4","name", "author2", "title", "title")));
    assertU(add(doc("id", "5","name", "author3", "title", "the title of a title")));
    assertU(commit());
    
    assertQ(req("q","title:title", "group", "true", "group.field","name")
            ,"//lst[@name='grouped']/lst[@name='name']"
            ,"*[count(//arr[@name='groups']/lst) = 3]"

            ,"//arr[@name='groups']/lst[1]/str[@name='groupValue'][.='author2']"
    //        ,"//arr[@name='groups']/lst[1]/int[@name='matches'][.='2']"
            ,"//arr[@name='groups']/lst[1]/result[@numFound='2']"
            ,"//arr[@name='groups']/lst[1]/result/doc/*[@name='id'][.='4']"

            ,"//arr[@name='groups']/lst[2]/str[@name='groupValue'][.='author1']"
    //       ,"//arr[@name='groups']/lst[2]/int[@name='matches'][.='2']"
            ,"//arr[@name='groups']/lst[2]/result[@numFound='2']"
            ,"//arr[@name='groups']/lst[2]/result/doc/*[@name='id'][.='2']"

            ,"//arr[@name='groups']/lst[3]/str[@name='groupValue'][.='author3']"
    //        ,"//arr[@name='groups']/lst[3]/int[@name='matches'][.='1']"
            ,"//arr[@name='groups']/lst[3]/result[@numFound='1']"
            ,"//arr[@name='groups']/lst[3]/result/doc/*[@name='id'][.='5']"
            );
  }

  @Test
  public void testGroupingGroupSortingScore_basicWithGroupSortEqualToSort() {
    assertU(add(doc("id", "1","name", "author1", "title", "a book title")));
    assertU(add(doc("id", "2","name", "author1", "title", "the title")));
    assertU(add(doc("id", "3","name", "author2", "title", "a book title")));
    assertU(add(doc("id", "4","name", "author2", "title", "title")));
    assertU(add(doc("id", "5","name", "author3", "title", "the title of a title")));
    assertU(commit());

    assertQ(req("q","title:title", "group", "true", "group.field","name", "sort", "score desc", "group.sort", "score desc")
            ,"//arr[@name='groups']/lst[1]/str[@name='groupValue'][.='author2']"
    //        ,"//arr[@name='groups']/lst[1]/int[@name='matches'][.='2']"
            ,"//arr[@name='groups']/lst[1]/result[@numFound='2']"
            ,"//arr[@name='groups']/lst[1]/result/doc/*[@name='id'][.='4']"

            ,"//arr[@name='groups']/lst[2]/str[@name='groupValue'][.='author1']"
    //        ,"//arr[@name='groups']/lst[2]/int[@name='matches'][.='2']"
            ,"//arr[@name='groups']/lst[2]/result[@numFound='2']"
            ,"//arr[@name='groups']/lst[2]/result/doc/*[@name='id'][.='2']"

            ,"//arr[@name='groups']/lst[3]/str[@name='groupValue'][.='author3']"
    //        ,"//arr[@name='groups']/lst[3]/int[@name='matches'][.='1']"
            ,"//arr[@name='groups']/lst[3]/result[@numFound='1']"
            ,"//arr[@name='groups']/lst[3]/result/doc/*[@name='id'][.='5']"
            );
  }

  @Test
  public void testGroupingGroupSortingName() {
    assertU(add(doc("id", "1","name", "author1", "title", "a book title")));
    assertU(add(doc("id", "2","name", "author1", "title", "the title")));
    assertU(add(doc("id", "3","name", "author2", "title", "book title")));
    assertU(add(doc("id", "4","name", "author2", "title", "the title")));
    assertU(commit());

    assertQ(req("q","title:title", "group", "true", "group.field","name", "group.sort", "title asc")
            ,"*[count(//arr[@name='groups']/lst) = 2]"
            ,"//arr[@name='groups']/lst[1]/str[@name='groupValue'][.='author2']"
    //       ,"//arr[@name='groups']/lst[1]/int[@name='matches'][.='2']"
            ,"//arr[@name='groups']/lst[1]/result[@numFound='2']"
            ,"//arr[@name='groups']/lst[1]/result/doc/*[@name='id'][.='3']"

            ,"//arr[@name='groups']/lst[2]/str[@name='groupValue'][.='author1']"
    //        ,"//arr[@name='groups']/lst[2]/int[@name='matches'][.='2']"
            ,"//arr[@name='groups']/lst[2]/result[@numFound='2']"
            ,"//arr[@name='groups']/lst[2]/result/doc/*[@name='id'][.='1']"
            );
  }

  @Test
  public void testGroupingGroupSortingWeight() {
    assertU(add(doc("id", "1","name", "author1", "weight", "12.1")));
    assertU(add(doc("id", "2","name", "author1", "weight", "2.1")));
    assertU(add(doc("id", "3","name", "author2", "weight", "0.1")));
    assertU(add(doc("id", "4","name", "author2", "weight", "0.11")));
    assertU(commit());

    assertQ(req("q","*:*", "group", "true", "group.field","name", "sort", "id asc", "group.sort", "weight desc")
            ,"*[count(//arr[@name='groups']/lst) = 2]"
            ,"//arr[@name='groups']/lst[1]/str[@name='groupValue'][.='author1']"
    //        ,"//arr[@name='groups']/lst[1]/int[@name='matches'][.='2']"
            ,"//arr[@name='groups']/lst[1]/result[@numFound='2']"
            ,"//arr[@name='groups']/lst[1]/result/doc/*[@name='id'][.='1']"

            ,"//arr[@name='groups']/lst[2]/str[@name='groupValue'][.='author2']"
    //        ,"//arr[@name='groups']/lst[2]/int[@name='matches'][.='2']"
            ,"//arr[@name='groups']/lst[2]/result[@numFound='2']"
            ,"//arr[@name='groups']/lst[2]/result/doc/*[@name='id'][.='4']"
            );
  }



  static String f = "foo_i";
  static String f2 = "foo2_i";

  public static void createIndex() {
    assertU(adoc("id","1", f,"5",  f2,"4"));
    assertU(adoc("id","2", f,"4",  f2,"2"));
    assertU(adoc("id","3", f,"3",  f2,"7"));
    assertU(commit());
    assertU(adoc("id","4", f,"2",  f2,"6"));
    assertU(adoc("id","5", f,"1",  f2,"2"));
    assertU(adoc("id","6", f,"3",  f2,"2"));
    assertU(adoc("id","7", f,"2",  f2,"3"));
    assertU(commit());
    assertU(adoc("id","8", f,"1",  f2,"10"));
    assertU(adoc("id","9", f,"2",  f2,"1"));
    assertU(commit());    
    assertU(adoc("id","10", f,"1", f2,"3"));
    assertU(commit());
  }

  @Test
  public void testGroupAPI() throws Exception {
    createIndex();
    String filt = f + ":[* TO *]";

    assertQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f)
        ,"/response/lst[@name='grouped']/lst[@name='"+f+"']/arr[@name='groups']"
    );

    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id")
      ,"/responseHeader/status:0"                         // exact match
      ,"/responseHeader:{'_SKIP_':'QTime', 'status':0}"   // partial match by skipping some elements
      ,"/responseHeader:{'_MATCH_':'status', 'status':0}" // partial match by only including some elements
      ,"/grouped:{'foo_i':{'matches':10,'groups':[\n" +
              "{'groupValue':1,'doclist':{'numFound':3,'start':0,'docs':[{'id':'8'}]}}," +
              "{'groupValue':3,'doclist':{'numFound':2,'start':0,'docs':[{'id':'3'}]}}," +
              "{'groupValue':2,'doclist':{'numFound':3,'start':0,'docs':[{'id':'4'}]}}," +
              "{'groupValue':5,'doclist':{'numFound':1,'start':0,'docs':[{'id':'1'}]}}," +
              "{'groupValue':4,'doclist':{'numFound':1,'start':0,'docs':[{'id':'2'}]}}" +
            "]}}"
    );

    // test limiting the number of groups returned
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","2")
      ,"/grouped:{'foo_i':{'matches':10,'groups':[" +
              "{'groupValue':1,'doclist':{'numFound':3,'start':0,'docs':[{'id':'8'}]}}," +
              "{'groupValue':3,'doclist':{'numFound':2,'start':0,'docs':[{'id':'3'}]}}" +
            "]}}"
    );

    // test increasing the docs per group returned
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","2", "group.limit","3")
      ,"/grouped:{'foo_i':{'matches':10,'groups':[" +
            "{'groupValue':1,'doclist':{'numFound':3,'start':0,'docs':[{'id':'8'},{'id':'10'},{'id':'5'}]}}," +
            "{'groupValue':3,'doclist':{'numFound':2,'start':0,'docs':[{'id':'3'},{'id':'6'}]}}" +
          "]}}"
    );

    // test adding in scores
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id,score", "rows","2", "group.limit","2", "indent","off")
      ,"/grouped/foo_i/groups:" +
            "[" +
              "{'groupValue':1,'doclist':{'numFound':3,'start':0,'maxScore':10.0,'docs':[{'id':'8','score':10.0},{'id':'10','score':3.0}]}}," +
              "{'groupValue':3,'doclist':{'numFound':2,'start':0,'maxScore':7.0,'docs':[{'id':'3','score':7.0},{'id':'6','score':2.0}]}}" +
            "]"

    );

    // test function (functions are currently all float - this may change)
    String func = "add("+f+","+f+")";
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.func", func  , "fl","id", "rows","2")
      ,"/grouped:{'"+func+"':{'matches':10,'groups':[" +
              "{'groupValue':2.0,'doclist':{'numFound':3,'start':0,'docs':[{'id':'8'}]}}," +
              "{'groupValue':6.0,'doclist':{'numFound':2,'start':0,'docs':[{'id':'3'}]}}" +
            "]}}"
    );

  };  


}
