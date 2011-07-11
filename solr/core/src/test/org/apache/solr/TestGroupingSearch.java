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

import org.apache.lucene.search.FieldCache;
import org.apache.noggit.JSONUtil;
import org.apache.noggit.ObjectBuilder;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

public class TestGroupingSearch extends SolrTestCaseJ4 {

  public static final String FOO_STRING_FIELD = "foo_s1";
  public static final String SMALL_STRING_FIELD = "small_s1";
  public static final String SMALL_INT_FIELD = "small_i";

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
    assertU(add(doc("id", "1","name", "author1", "title", "a book title", "group_si", "1")));
    assertU(add(doc("id", "2","name", "author1", "title", "the title", "group_si", "2")));
    assertU(add(doc("id", "3","name", "author2", "title", "a book title", "group_si", "1")));
    assertU(add(doc("id", "4","name", "author2", "title", "title", "group_si", "2")));
    assertU(add(doc("id", "5","name", "author3", "title", "the title of a title", "group_si", "1")));
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

    assertQ(req("q","title:title", "group", "true", "group.field","group_si")
            ,"//lst[@name='grouped']/lst[@name='group_si']"
            ,"*[count(//arr[@name='groups']/lst) = 2]"

            ,"//arr[@name='groups']/lst[1]/int[@name='groupValue'][.='2']"
            ,"//arr[@name='groups']/lst[1]/result[@numFound='2']"
            ,"//arr[@name='groups']/lst[1]/result/doc/*[@name='id'][.='4']"

            ,"//arr[@name='groups']/lst[2]/int[@name='groupValue'][.='1']"
            ,"//arr[@name='groups']/lst[2]/result[@numFound='3']"
            ,"//arr[@name='groups']/lst[2]/result/doc/*[@name='id'][.='5']"
            );
  }

  @Test
  public void testGroupingGroupSortingScore_withTotalGroupCount() {
    assertU(add(doc("id", "1","name", "author1", "title", "a book title", "group_si", "1")));
    assertU(add(doc("id", "2","name", "author1", "title", "the title", "group_si", "2")));
    assertU(add(doc("id", "3","name", "author2", "title", "a book title", "group_si", "1")));
    assertU(add(doc("id", "4","name", "author2", "title", "title", "group_si", "2")));
    assertU(add(doc("id", "5","name", "author3", "title", "the title of a title", "group_si", "1")));
    assertU(commit());

    assertQ(req("q","title:title", "group", "true", "group.field","name", "group.ngroups", "true")
            ,"//lst[@name='grouped']/lst[@name='name']"
            ,"//lst[@name='grouped']/lst[@name='name']/int[@name='matches'][.='5']"
            ,"//lst[@name='grouped']/lst[@name='name']/int[@name='ngroups'][.='3']"
            ,"*[count(//arr[@name='groups']/lst) = 3]"

            ,"//arr[@name='groups']/lst[1]/str[@name='groupValue'][.='author2']"
            ,"//arr[@name='groups']/lst[1]/result[@numFound='2']"
            ,"//arr[@name='groups']/lst[1]/result/doc/*[@name='id'][.='4']"

            ,"//arr[@name='groups']/lst[2]/str[@name='groupValue'][.='author1']"
            ,"//arr[@name='groups']/lst[2]/result[@numFound='2']"
            ,"//arr[@name='groups']/lst[2]/result/doc/*[@name='id'][.='2']"

            ,"//arr[@name='groups']/lst[3]/str[@name='groupValue'][.='author3']"
            ,"//arr[@name='groups']/lst[3]/result[@numFound='1']"
            ,"//arr[@name='groups']/lst[3]/result/doc/*[@name='id'][.='5']"
            );

    assertQ(req("q","title:title", "group", "true", "group.field","group_si", "group.ngroups", "true")
            ,"//lst[@name='grouped']/lst[@name='group_si']/int[@name='matches'][.='5']"
            ,"//lst[@name='grouped']/lst[@name='group_si']/int[@name='ngroups'][.='2']"
            ,"*[count(//arr[@name='groups']/lst) = 2]"

            ,"//arr[@name='groups']/lst[1]/int[@name='groupValue'][.='2']"
            ,"//arr[@name='groups']/lst[1]/result[@numFound='2']"
            ,"//arr[@name='groups']/lst[1]/result/doc/*[@name='id'][.='4']"

            ,"//arr[@name='groups']/lst[2]/int[@name='groupValue'][.='1']"
            ,"//arr[@name='groups']/lst[2]/result[@numFound='3']"
            ,"//arr[@name='groups']/lst[2]/result/doc/*[@name='id'][.='5']"
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

  @Test
  public void testGroupingSimpleFormatArrayIndexOutOfBoundsException() throws Exception {
    assertU(add(doc("id", "1")));
    assertU(add(doc("id", "2")));
    assertU(add(doc("id", "3")));
    assertU(commit());

    assertJQ(
        req("q", "*:*", "start", "1", "group", "true", "group.field", "id", "group.main", "true"),
        "/response=={'numFound':3,'start':1,'docs':[{'id':'2'},{'id':'3'}]}"
    );
    assertJQ(
        req("q", "*:*", "start", "1", "rows", "1", "group", "true", "group.field", "id", "group.main", "true"),
        "/response=={'numFound':3,'start':1,'docs':[{'id':'2'}]}"
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
      ,"/responseHeader/status==0"                         // exact match
      ,"/responseHeader=={'_SKIP_':'QTime', 'status':0}"   // partial match by skipping some elements
      ,"/responseHeader=={'_MATCH_':'status', 'status':0}" // partial match by only including some elements
      ,"/grouped=={'"+f+"':{'matches':10,'groups':[\n" +
              "{'groupValue':1,'doclist':{'numFound':3,'start':0,'docs':[{'id':'8'}]}}," +
              "{'groupValue':3,'doclist':{'numFound':2,'start':0,'docs':[{'id':'3'}]}}," +
              "{'groupValue':2,'doclist':{'numFound':3,'start':0,'docs':[{'id':'4'}]}}," +
              "{'groupValue':5,'doclist':{'numFound':1,'start':0,'docs':[{'id':'1'}]}}," +
              "{'groupValue':4,'doclist':{'numFound':1,'start':0,'docs':[{'id':'2'}]}}" +
            "]}}"
    );

    // test that filtering cuts down the result set
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "fq",f+":2")
      ,"/grouped=={'"+f+"':{'matches':3,'groups':[" +
            "{'groupValue':2,'doclist':{'numFound':3,'start':0,'docs':[{'id':'4'}]}}" +
            "]}}"
    );

    // test limiting the number of groups returned
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","2")
      ,"/grouped=={'"+f+"':{'matches':10,'groups':[" +
              "{'groupValue':1,'doclist':{'numFound':3,'start':0,'docs':[{'id':'8'}]}}," +
              "{'groupValue':3,'doclist':{'numFound':2,'start':0,'docs':[{'id':'3'}]}}" +
            "]}}"
    );

    // test offset into group list
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","1", "start","1")
      ,"/grouped=={'"+f+"':{'matches':10,'groups':[" +
              "{'groupValue':3,'doclist':{'numFound':2,'start':0,'docs':[{'id':'3'}]}}" +
            "]}}"
    );

    // test big offset into group list
     assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","1", "start","100")
      ,"/grouped=={'"+f+"':{'matches':10,'groups':[" +
            "]}}"
    );

    // test increasing the docs per group returned
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","2", "group.limit","3")
      ,"/grouped=={'"+f+"':{'matches':10,'groups':[" +
            "{'groupValue':1,'doclist':{'numFound':3,'start':0,'docs':[{'id':'8'},{'id':'10'},{'id':'5'}]}}," +
            "{'groupValue':3,'doclist':{'numFound':2,'start':0,'docs':[{'id':'3'},{'id':'6'}]}}" +
          "]}}"
    );

    // test offset into each group
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","2", "group.limit","3", "group.offset","1")
      ,"/grouped=={'"+f+"':{'matches':10,'groups':[" +
            "{'groupValue':1,'doclist':{'numFound':3,'start':1,'docs':[{'id':'10'},{'id':'5'}]}}," +
            "{'groupValue':3,'doclist':{'numFound':2,'start':1,'docs':[{'id':'6'}]}}" +
          "]}}"
    );

    // test big offset into each group
     assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","2", "group.limit","3", "group.offset","10")
      ,"/grouped=={'"+f+"':{'matches':10,'groups':[" +
            "{'groupValue':1,'doclist':{'numFound':3,'start':10,'docs':[]}}," +
            "{'groupValue':3,'doclist':{'numFound':2,'start':10,'docs':[]}}" +
          "]}}"
    );

    // test adding in scores
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id,score", "rows","2", "group.limit","2", "indent","off")
      ,"/grouped/"+f+"/groups==" +
            "[" +
              "{'groupValue':1,'doclist':{'numFound':3,'start':0,'maxScore':10.0,'docs':[{'id':'8','score':10.0},{'id':'10','score':3.0}]}}," +
              "{'groupValue':3,'doclist':{'numFound':2,'start':0,'maxScore':7.0,'docs':[{'id':'3','score':7.0},{'id':'6','score':2.0}]}}" +
            "]"

    );

    // test function (functions are currently all float - this may change)
    String func = "add("+f+","+f+")";
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.func", func  , "fl","id", "rows","2")
      ,"/grouped=={'"+func+"':{'matches':10,'groups':[" +
              "{'groupValue':2.0,'doclist':{'numFound':3,'start':0,'docs':[{'id':'8'}]}}," +
              "{'groupValue':6.0,'doclist':{'numFound':2,'start':0,'docs':[{'id':'3'}]}}" +
            "]}}"
    );

    // test that faceting works with grouping
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id"
                 ,"facet","true", "facet.field",f)
      ,"/grouped/"+f+"/matches==10"
      ,"/facet_counts/facet_fields/"+f+"==['1',3, '2',3, '3',2, '4',1, '5',1]"
    );
    purgeFieldCache(FieldCache.DEFAULT);   // avoid FC insanity

    // test that grouping works with highlighting
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id"
                 ,"hl","true", "hl.fl",f)
      ,"/grouped/"+f+"/matches==10"
      ,"/highlighting=={'_ORDERED_':'', '8':{},'3':{},'4':{},'1':{},'2':{}}"
    );

    // test that grouping works with debugging
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id"
                 ,"debugQuery","true")
      ,"/grouped/"+f+"/matches==10"
      ,"/debug/explain/8=="
      ,"/debug/explain/2=="
    );

     ///////////////////////// group.query
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.query","id:[2 TO 5]", "fl","id", "group.limit","3")
       ,"/grouped=={'id:[2 TO 5]':{'matches':10," +
           "'doclist':{'numFound':4,'start':0,'docs':[{'id':'3'},{'id':'4'},{'id':'2'}]}}}"
    );

    // group.query and offset
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.query","id:[2 TO 5]", "fl","id", "group.limit","3", "group.offset","2")
       ,"/grouped=={'id:[2 TO 5]':{'matches':10," +
           "'doclist':{'numFound':4,'start':2,'docs':[{'id':'2'},{'id':'5'}]}}}"
    );

    // group.query and big offset
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.query","id:[2 TO 5]", "fl","id", "group.limit","3", "group.offset","10")
       ,"/grouped=={'id:[2 TO 5]':{'matches':10," +
           "'doclist':{'numFound':4,'start':10,'docs':[]}}}"
    );

    ///////////////////////// group.query as main result
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.query","id:[2 TO 5]", "fl","id", "rows","3", "group.main","true")
       ,"/response=={'numFound':4,'start':0,'docs':[{'id':'3'},{'id':'4'},{'id':'2'}]}"
    );

    // group.query and offset
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.query","id:[2 TO 5]", "fl","id", "rows","3", "start","2", "group.main","true")
       ,"/response=={'numFound':4,'start':2,'docs':[{'id':'2'},{'id':'5'}]}"
    );

    // group.query and big offset
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.query","id:[2 TO 5]", "fl","id", "rows","3", "start","10", "group.main","true")
       ,"/response=={'numFound':4,'start':10,'docs':[]}"
    );


    // multiple at once
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true",
        "group.query","id:[2 TO 5]",
        "group.query","id:[5 TO 5]",
        "group.field",f,
        "rows","1",
        "fl","id", "group.limit","2")
       ,"/grouped/id:[2 TO 5]=={'matches':10,'doclist':{'numFound':4,'start':0,'docs':[{'id':'3'},{'id':'4'}]}}"
       ,"/grouped/id:[5 TO 5]=={'matches':10,'doclist':{'numFound':1,'start':0,'docs':[{'id':'5'}]}}"        
       ,"/grouped/"+f+"=={'matches':10,'groups':[{'groupValue':1,'doclist':{'numFound':3,'start':0,'docs':[{'id':'8'},{'id':'10'}]}}]}"
    );


    ///////////////////////// group.field as main result
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "group.main","true")
        ,"/response=={'numFound':10,'start':0,'docs':[{'id':'8'},{'id':'3'},{'id':'4'},{'id':'1'},{'id':'2'}]}"
    );
    // test that rows limits #docs
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","3", "group.main","true")
        ,"/response=={'numFound':10,'start':0,'docs':[{'id':'8'},{'id':'3'},{'id':'4'}]}"
    );
    // small  offset
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","2", "start","1", "group.main","true")
        ,"/response=={'numFound':10,'start':1,'docs':[{'id':'3'},{'id':'4'}]}"
    );
    // large offset
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","2", "start","20", "group.main","true")
        ,"/response=={'numFound':10,'start':20,'docs':[]}"
    );
    // group.limit>1
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","3", "group.limit","2", "group.main","true")
        ,"/response=={'numFound':10,'start':0,'docs':[{'id':'8'},{'id':'10'},{'id':'3'}]}"
    );
    // group.limit>1 with start>0
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","3", "start","1", "group.limit","2", "group.main","true")
        ,"/response=={'numFound':10,'start':1,'docs':[{'id':'10'},{'id':'3'},{'id':'6'}]}"
    );

    ///////////////////////// group.format == simple
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","3", "start","1", "group.limit","2", "group.format","simple")
    , "/grouped/foo_i=={'matches':10,'doclist':"
        +"{'numFound':10,'start':1,'docs':[{'id':'10'},{'id':'3'},{'id':'6'}]}}"
    );
  }



  @Test
  public void testRandomGrouping() throws Exception {
    /**
     updateJ("{\"add\":{\"doc\":{\"id\":\"77\"}}}", params("commit","true"));
     assertJQ(req("q","id:77"), "/response/numFound==1");

     Doc doc = createDocObj(types);
     updateJ(toJSON(doc), params("commit","true"));

     assertJQ(req("q","id:"+doc.id), "/response/numFound==1");
    **/

    int indexIter=50 * RANDOM_MULTIPLIER;  // make >0 to enable test
    int queryIter=100 * RANDOM_MULTIPLIER;

    while (--indexIter >= 0) {

      int indexSize = random.nextInt(25 * RANDOM_MULTIPLIER);
//indexSize=2;
      List<FldType> types = new ArrayList<FldType>();
      types.add(new FldType("id",ONE_ONE, new SVal('A','Z',4,4)));
      types.add(new FldType("score_f",ONE_ONE, new FVal(1,100)));  // field used to score
      types.add(new FldType("foo_i",ZERO_ONE, new IRange(0,indexSize)));
      types.add(new FldType(FOO_STRING_FIELD,ZERO_ONE, new SVal('a','z',1,2)));
      types.add(new FldType(SMALL_STRING_FIELD,ZERO_ONE, new SVal('a',(char)('c'+indexSize/10),1,1)));
      types.add(new FldType(SMALL_INT_FIELD,ZERO_ONE, new IRange(0,5+indexSize/10)));

      clearIndex();
      Map<Comparable, Doc> model = indexDocs(types, null, indexSize);
      //System.out.println("############### model=" + model);

      // test with specific docs
      if (false) {
        clearIndex();
        model.clear();
        Doc d1 = createDoc(types);
        d1.getValues(SMALL_STRING_FIELD).set(0,"c");
        d1.getValues(SMALL_INT_FIELD).set(0,5);
        d1.order = 0;
        updateJ(toJSON(d1), params("commit","true"));
        model.put(d1.id, d1);

        d1 = createDoc(types);
        d1.getValues(SMALL_STRING_FIELD).set(0,"b");
        d1.getValues(SMALL_INT_FIELD).set(0,5);
        d1.order = 1;
        updateJ(toJSON(d1), params("commit","false"));
        model.put(d1.id, d1);

        d1 = createDoc(types);
        d1.getValues(SMALL_STRING_FIELD).set(0,"c");
        d1.getValues(SMALL_INT_FIELD).set(0,5);
        d1.order = 2;
        updateJ(toJSON(d1), params("commit","false"));
        model.put(d1.id, d1);

        d1 = createDoc(types);
        d1.getValues(SMALL_STRING_FIELD).set(0,"c");
        d1.getValues(SMALL_INT_FIELD).set(0,5);
        d1.order = 3;
        updateJ(toJSON(d1), params("commit","false"));
        model.put(d1.id, d1);

        d1 = createDoc(types);
        d1.getValues(SMALL_STRING_FIELD).set(0,"b");
        d1.getValues(SMALL_INT_FIELD).set(0,2);
        d1.order = 4;
        updateJ(toJSON(d1), params("commit","true"));
        model.put(d1.id, d1);
      }


      for (int qiter=0; qiter<queryIter; qiter++) {
        String groupField = types.get(random.nextInt(types.size())).fname;

        int rows = random.nextInt(10)==0 ? random.nextInt(model.size()+2) : random.nextInt(11)-1;
        int start = random.nextInt(5)==0 ? random.nextInt(model.size()+2) : random.nextInt(5); // pick a small start normally for better coverage
        int group_limit = random.nextInt(10)==0 ? random.nextInt(model.size()+2) : random.nextInt(11)-1;    
        int group_offset = random.nextInt(10)==0 ? random.nextInt(model.size()+2) : random.nextInt(2); // pick a small start normally for better coverage

        String[] stringSortA = new String[1];
        Comparator<Doc> sortComparator = createSort(h.getCore().getSchema(), types, stringSortA);
        String sortStr = stringSortA[0];
        Comparator<Doc> groupComparator = random.nextBoolean() ? sortComparator : createSort(h.getCore().getSchema(), types, stringSortA);
        String groupSortStr = stringSortA[0];

        // since groupSortStr defaults to sortStr, we need to normalize null to "score desc" if
        // sortStr != null.
        if (groupSortStr == null && groupSortStr != sortStr) {
          groupSortStr = "score desc";
        }
        
         // Test specific case
        if (false) {
          groupField=SMALL_INT_FIELD;
          sortComparator=createComparator(Arrays.asList(createComparator(SMALL_STRING_FIELD, true, true, false, true)));
          sortStr = SMALL_STRING_FIELD + " asc";
          groupComparator = createComparator(Arrays.asList(createComparator(SMALL_STRING_FIELD, true, true, false, false)));
          groupSortStr = SMALL_STRING_FIELD + " asc";
          rows=1; start=0; group_offset=1; group_limit=1;
        }

        Map<Comparable, Grp> groups = groupBy(model.values(), groupField);

        // first sort the docs in each group
        for (Grp grp : groups.values()) {
          Collections.sort(grp.docs, groupComparator);
        }

        // now sort the groups

        // if sort != group.sort, we need to find the max doc by "sort"
        if (groupComparator != sortComparator) {
          for (Grp grp : groups.values()) grp.setMaxDoc(sortComparator); 
        }

        List<Grp> sortedGroups = new ArrayList(groups.values());
        Collections.sort(sortedGroups,  groupComparator==sortComparator ? createFirstDocComparator(sortComparator) : createMaxDocComparator(sortComparator));

        boolean includeNGroups = random.nextBoolean();
        Object modelResponse = buildGroupedResult(h.getCore().getSchema(), sortedGroups, start, rows, group_offset, group_limit, includeNGroups);

        int randomPercentage = random.nextInt(101);
        // TODO: create a random filter too
        SolrQueryRequest req = req("group","true","wt","json","indent","true", "echoParams","all", "q","{!func}score_f", "group.field",groupField
            ,sortStr==null ? "nosort":"sort", sortStr ==null ? "": sortStr
            ,(groupSortStr==null || groupSortStr==sortStr) ? "noGroupsort":"group.sort", groupSortStr==null ? "": groupSortStr
            ,"rows",""+rows, "start",""+start, "group.offset",""+group_offset, "group.limit",""+group_limit,
            GroupParams.GROUP_CACHE_PERCENTAGE, Integer.toString(randomPercentage), GroupParams.GROUP_TOTAL_COUNT, includeNGroups ? "true" : "false"
        );

        String strResponse = h.query(req);

        Object realResponse = ObjectBuilder.fromJSON(strResponse);
        String err = JSONTestUtil.matchObj("/grouped/"+groupField, realResponse, modelResponse);
        if (err != null) {
          log.error("GROUPING MISMATCH: " + err
           + "\n\trequest="+req
           + "\n\tresult="+strResponse
           + "\n\texpected="+ JSONUtil.toJSON(modelResponse)
           + "\n\tsorted_model="+ sortedGroups
          );

          // re-execute the request... good for putting a breakpoint here for debugging
          String rsp = h.query(req);

          fail(err);
        }
      } // end query iter
    } // end index iter

  }

  public static Object buildGroupedResult(IndexSchema schema, List<Grp> sortedGroups, int start, int rows, int group_offset, int group_limit, boolean includeNGroups) {
    Map<String,Object> result = new LinkedHashMap<String,Object>();

    long matches = 0;
    for (Grp grp : sortedGroups) {
      matches += grp.docs.size();
    }
    result.put("matches", matches);
    if (includeNGroups) {
      result.put("ngroups", sortedGroups.size());
    }
    List groupList = new ArrayList();
    result.put("groups", groupList);

    for (int i=start; i<sortedGroups.size(); i++) {
      if (rows != -1 && groupList.size() >= rows) break;  // directly test rather than calculating, so we can catch any calc errors in the real code
      Map<String,Object> group = new LinkedHashMap<String,Object>();
      groupList.add(group);

      Grp grp = sortedGroups.get(i);
      group.put("groupValue", grp.groupValue);

      Map<String,Object> resultSet = new LinkedHashMap<String,Object>();
      group.put("doclist", resultSet);
      resultSet.put("numFound", grp.docs.size());
      resultSet.put("start", group_offset);
      List docs = new ArrayList();
      resultSet.put("docs", docs);
      for (int j=group_offset; j<grp.docs.size(); j++) {
        if (group_limit != -1 && docs.size() >= group_limit) break;
        docs.add( grp.docs.get(j).toObject(schema) );
      }
    }

    return result;
  }


  public static Comparator<Grp> createMaxDocComparator(final Comparator<Doc> docComparator) {
    return new Comparator<Grp>() {
      @Override
      public int compare(Grp o1, Grp o2) {
        // all groups should have at least one doc
        Doc d1 = o1.maxDoc;
        Doc d2 = o2.maxDoc;
        return docComparator.compare(d1, d2);
      }
    };
  }

  public static Comparator<Grp> createFirstDocComparator(final Comparator<Doc> docComparator) {
    return new Comparator<Grp>() {
      @Override
      public int compare(Grp o1, Grp o2) {
        // all groups should have at least one doc
        Doc d1 = o1.docs.get(0);
        Doc d2 = o2.docs.get(0);
        return docComparator.compare(d1, d2);
      }
    };
  }

  public static Map<Comparable, Grp> groupBy(Collection<Doc> docs, String field) {
    Map<Comparable, Grp> groups = new HashMap<Comparable, Grp>();
    for (Doc doc : docs) {
      List<Comparable> vals = doc.getValues(field);
      if (vals == null) {
        Grp grp = groups.get(null);
        if (grp == null) {
          grp = new Grp();
          grp.groupValue = null;
          grp.docs = new ArrayList<Doc>();
          groups.put(null, grp);
        }
        grp.docs.add(doc);
      } else {
        for (Comparable val : vals) {

          Grp grp = groups.get(val);
          if (grp == null) {
            grp = new Grp();
            grp.groupValue = val;
            grp.docs = new ArrayList<Doc>();
            groups.put(grp.groupValue, grp);
          }
          grp.docs.add(doc);
        }
      }
    }
    return groups;
  }


  public static class Grp {
    public Comparable groupValue;
    public List<Doc> docs;
    public Doc maxDoc;  // the document highest according to the "sort" param


    public void setMaxDoc(Comparator<Doc> comparator) {
      Doc[] arr = docs.toArray(new Doc[docs.size()]);
      Arrays.sort(arr, comparator);
      maxDoc = arr.length > 0 ? arr[0] : null;
    }

    @Override
    public String toString() {
      return "{groupValue="+groupValue+",docs="+docs+"}";
    }
  }
}


