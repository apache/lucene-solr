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

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.commons.io.output.ByteArrayOutputStream;

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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.BinaryQueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSubQueryTransformer extends SolrTestCaseJ4 {
  private static int peopleMultiplier;
  private static int deptMultiplier;

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "false");
    initCore("solrconfig-basic.xml", "schema-docValuesJoin.xml");
    peopleMultiplier = atLeast(1);
    deptMultiplier = atLeast(1);
    
    int id=0;
    for (int p=0; p < peopleMultiplier; p++){
      assertU(add(doc("id", ""+id++,"name_s", "john", "title_s", "Director", 
                                                      "dept_ss_dv","Engineering",
                                                      "dept_i", "0",
                                                      "dept_is", "0")));
      assertU(add(doc("id", ""+id++,"name_s", "mark", "title_s", "VP", 
                                                         "dept_ss_dv","Marketing",
                                                         "dept_i", "1",
                                                         "dept_is", "1")));
      assertU(add(doc("id", ""+id++,"name_s", "nancy", "title_s", "MTS",
                                                         "dept_ss_dv","Sales",
                                                         "dept_i", "2",
                                                         "dept_is", "2")));
      assertU(add(doc("id", ""+id++,"name_s", "dave", "title_s", "MTS", 
                                                         "dept_ss_dv","Support", "dept_ss_dv","Engineering",
                                                         "dept_i", "3",
                                                         "dept_is", "3", "dept_is", "0")));
      assertU(add(doc("id", ""+id++,"name_s", "tina", "title_s", "VP", 
                                                         "dept_ss_dv","Engineering",
                                                         "dept_i", "0",
                                                         "dept_is", "0")));
      
      if (rarely()) {
        assertU(commit("softCommit", "true"));
      }
    }
    
    for (int d=0; d < deptMultiplier; d++){
      assertU(add(doc("id",""+id, "id_i",""+id++,
          "dept_id_s", "Engineering", "text_t","These guys develop stuff", "salary_i_dv", "1000",
                                     "dept_id_i", "0")));
      assertU(add(doc("id",""+id++,"id_i",""+id++,
           "dept_id_s", "Marketing", "text_t","These guys make you look good","salary_i_dv", "1500",
                                     "dept_id_i", "1")));
      assertU(add(doc("id",""+id, "id_i",""+id++,
          "dept_id_s", "Sales", "text_t","These guys sell stuff","salary_i_dv", "1600",
                                    "dept_id_i", "2")));
      assertU(add(doc("id",""+id,"id_i",""+id++,
           "dept_id_s", "Support", "text_t","These guys help customers","salary_i_dv", "800",
                                    "dept_id_i", "3")));
      
      if (rarely()) {
        assertU(commit("softCommit", "true"));
      }
    }
    assertU(commit());

  }

  
  @Test
  public void testJohnOrNancySingleField() throws Exception {
     //System.out.println("p "+peopleMultiplier+" d "+deptMultiplier);
    assertQ("subq1.fl is limited to single field",
        req("q","name_s:(john nancy)", "indent","true",
            "fl","dept_ss_dv,name_s_dv,depts:[subquery]", 
            "rows","" + (2 * peopleMultiplier),
            "depts.q","{!term f=dept_id_s v=$row.dept_ss_dv}", 
            "depts.fl","text_t",
            "depts.indent","true",
            "depts.rows",""+deptMultiplier),
        "count(//result/doc/str[@name='name_s_dv'][.='john']/../result[@name='depts'][@numFound='" +
            deptMultiplier+ "']/doc/str[@name='text_t'][.='These guys develop stuff'])="+
            (peopleMultiplier * deptMultiplier),
         "count(//result/doc/str[@name='name_s_dv'][.='nancy']/../result[@name='depts'][@numFound='" +
            deptMultiplier+ "']/doc/str[@name='text_t'][.='These guys sell stuff'])="+
                (peopleMultiplier * deptMultiplier),
        "count((//result/doc/str[@name='name_s_dv'][.='john']/..)[1]/result[@name='depts']/doc[1]/*)=1",
        "count((//result/doc/str[@name='name_s_dv'][.='john']/..)[1]/result[@name='depts']/doc["+ deptMultiplier+ "]/*)=1",
        "count((//result/doc/str[@name='name_s_dv'][.='john']/..)["+ peopleMultiplier +"]/result[@name='depts'][@numFound='" +
            deptMultiplier+ "']/doc[1]/*)=1",
        "count((//result/doc/str[@name='name_s_dv'][.='john']/..)["+ peopleMultiplier +"]/result[@name='depts'][@numFound='" +
            deptMultiplier+ "']/doc["+ deptMultiplier+ "]/*)=1"
                
                );
    }
  
  final String[] johnAndNancyParams = new String[]{"q","name_s:(john nancy)", "indent","true",
      "fl","dept_ss_dv,name_s_dv,depts:[subquery]",
      "fl","dept_i_dv,depts_i:[subquery]",
      "rows","" + (2 * peopleMultiplier),
      "depts.q","{!term f=dept_id_s v=$row.dept_ss_dv}", 
      "depts.fl","text_t",
      "depts.indent","true",
      "depts.rows",""+deptMultiplier,
      
      "depts_i.q","{!term f=dept_id_i v=$row.dept_i_dv}", 
      "depts_i.fl","text_t", // multi val subquery param check
      "depts_i.fl","dept_id_s_dv",
      "depts_i.indent","true",
      "depts_i.rows",""+deptMultiplier};
  
  @Test
  public void testTwoSubQueriesAndByNumberWithTwoFields() throws Exception {
    final SolrQueryRequest johnOrNancyTwoFL = req(johnAndNancyParams);
    
    assertQ("call subquery twice a row, once by number, with two fls via multival params",
        johnOrNancyTwoFL,
        "count(//result/doc/str[@name='name_s_dv'][.='john']/../result[@name='depts']/doc/str[@name='text_t'][.='These guys develop stuff'])="+
                (peopleMultiplier * deptMultiplier),
         "count(//result/doc/str[@name='name_s_dv'][.='john']/../result[@name='depts_i']/doc/str[@name='dept_id_s_dv'][.='Engineering'])="+
            (peopleMultiplier * deptMultiplier), 
        "count(//result/doc/str[@name='name_s_dv'][.='nancy']/../result[@name='depts_i']/doc/str[@name='text_t'][.='These guys sell stuff'])="+
                (peopleMultiplier * deptMultiplier),
         "count(//result/doc/str[@name='name_s_dv'][.='nancy']/../result[@name='depts_i']/doc/str[@name='dept_id_s_dv'][.='Sales'])="+
            (peopleMultiplier * deptMultiplier),
         "count((//result/doc/str[@name='name_s_dv'][.='john']/..)["+ peopleMultiplier +"]/result[@name='depts_i']/doc["+ deptMultiplier+ "]/str[@name='dept_id_s_dv'][.='Engineering'])=1",
         "count((//result/doc/str[@name='name_s_dv'][.='john']/..)["+ peopleMultiplier +"]/result[@name='depts_i']/doc["+ deptMultiplier+ "]/str[@name='text_t'][.='These guys develop stuff'])=1"
                );
  }
  
  @Test
  public void testRowsStartForSubqueryAndScores() throws Exception {
    
    String johnDeptsIds = h.query(req(new String[]{"q","{!join from=dept_ss_dv to=dept_id_s}name_s:john", 
        "wt","csv",
        "csv.header","false",
        "fl","id",
        "rows",""+deptMultiplier,
        "sort", "id_i desc"
      }));
    
    ArrayList<Object> deptIds = Collections.list(
        new StringTokenizer( johnDeptsIds));
    
    final int a = random().nextInt(deptMultiplier+1);
    final int b = random().nextInt(deptMultiplier+1);
    final int start = Math.min(a, b) ;
    final int toIndex = Math.max(a, b) ;
    List<Object> expectIds = deptIds.subList(start , toIndex);
    ArrayList<String> assertions = new ArrayList<>();
    // count((//result/doc/str[@name='name_s_dv'][.='john']/../result[@name='depts'])[1]/doc/str[@name='id'])
   // random().nextInt(peopleMultiplier);
    assertions.add("count((//result/doc/str[@name='name_s_dv'][.='john']/.."
        + "/result[@name='depts'][@numFound='"+deptMultiplier+"'][@start='"+start+"'])["+
        (random().nextInt(peopleMultiplier)+1)
        +"]/doc/str[@name='id'])=" +(toIndex-start));
    
   // System.out.println(expectIds);
    
    for (int i=0; i< expectIds.size(); i++) {
      // (//result/doc/str[@name='name_s_dv'][.='john']/../result[@name='depts'])[1]/doc[1]/str[@name='id']='15'
      String ithDoc = "(//result/doc/str[@name='name_s_dv'][.='john']/.."
                + "/result[@name='depts'][@numFound='"+deptMultiplier+"'][@start='"+start+"'])["+ 
                (random().nextInt(peopleMultiplier)+1) +
                "]/doc[" +(i+1)+ "]";
      assertions.add(ithDoc+"/str[@name='id'][.='"+expectIds.get(i)+"']");
      // let's test scores right there
      assertions.add(ithDoc+"/float[@name='score'][.='"+expectIds.get(i)+".0']");
      
    }
    
    String[] john = new String[]{"q","name_s:john", "indent","true",
        "fl","dept_ss_dv,name_s_dv,depts:[subquery]",
        "rows","" + (2 * peopleMultiplier),
        "depts.q","+{!term f=dept_id_s v=$row.dept_ss_dv}^=0 _val_:id_i", 
        "depts.fl","id",
        "depts.fl","score",
        "depts.indent","true",
        "depts.rows",""+(toIndex-start),
        "depts.start",""+start};
        
    assertQ(req(john), assertions.toArray(new String[]{}));
  }
  
  @Test
  public void testThreeLevel() throws Exception {
    List<String> asserts =  new ArrayList<>();
    // dave works in both dept, get his coworkers from both
    for (String dept : new String[] {"Engineering", "Support"}) { //dept_id_s_dv">Engineering
      
      ArrayList<Object> deptWorkers = Collections.list(
          new StringTokenizer( h.query(req(
              "q","dept_ss_dv:"+dept ,//dept_id_i_dv
             "wt","csv",
             "csv.header","false",
             "fl","name_s_dv",
             "rows",""+peopleMultiplier*3, // dave has three coworkers in two depts
             "sort", "id desc"
           ))));
     // System.out.println(deptWorkers);
      
      // looping dave clones
      for (int p : new int []{1, peopleMultiplier}) {
        // looping dept clones
        for (int d : new int []{1, deptMultiplier}) {
          // looping coworkers
          int wPos = 1;
          for (Object mate : deptWorkers) {
            // (/response/result/doc/str[@name='name_s_dv'][.='dave']/..)[1]
            //  /result[@name='subq1']/doc/str[@name='dept_id_s_dv'][.='Engineering']/..
            //  /result[@name='neighbours']/doc/str[@name='name_s_dv'][.='tina']
            asserts.add("((/response/result/doc/str[@name='name_s_dv'][.='dave']/..)["+p+"]"+
              "/result[@name='subq1']/doc/str[@name='dept_id_s_dv'][.='"+dept+"']/..)["+ d +"]"+
              "/result[@name='neighbours']/doc[" + wPos + "]/str[@name='name_s_dv'][.='"+ mate+"']");
            wPos ++; 
          }
          
        }
      }
    }
    //System.out.println(asserts);
    assertQ("dave works at both dept with other folks",
  //  System.out.println(h.query( 
        req(new String[]{"q","name_s:dave", "indent","true",
        "fl","dept_ss_dv,name_s_dv,subq1:[subquery]", 
        "rows","" + peopleMultiplier,
        "subq1.q","{!terms f=dept_id_s v=$row.dept_ss_dv}", 
        "subq1.fl","dept_id_i_dv,text_t,dept_id_s_dv,neighbours:[subquery]",
        "subq1.indent","true",
        "subq1.rows",""+(deptMultiplier*2),
        "subq1.neighbours.q",//flipping via numbers 
        random().nextBoolean() ?
          "{!terms f=dept_ss_dv v=$row.dept_id_s_dv}"
        : "{!terms f=dept_is v=$row.dept_id_i_dv}",
        "subq1.neighbours.fl", "name_s_dv" ,
        "subq1.neighbours.rows", ""+peopleMultiplier*3},
        "subq1.neighbours.sort", "id desc")//,
       ,asserts.toArray(new String[]{})
    //        ) 
    );
  
  }
  
  @Test
  public void testNoExplicitName() throws Exception {
    String[] john = new String[]{"q","name_s:john", "indent","true",
        "fl","name_s_dv,"
            + "[subquery]",
        "rows","" + (2 * peopleMultiplier),
        "depts.q","+{!term f=dept_id_s v=$row.dept_ss_dv}^=0 _val_:id_i", 
        "depts.fl","id",
        "depts.fl","score",
        "depts.indent","true",
        "depts.rows",""+deptMultiplier,
        "depts.start","0"};
        
    assertQEx("no prefix, no subquery", req(john), ErrorCode.BAD_REQUEST);
    
        
    assertQEx("no prefix, no subsubquery", 
        req("q","name_s:john", "indent","true",
            "fl","name_s_dv,"
                + "depts:[subquery]",
            "rows","" + (2 * peopleMultiplier),
            "depts.q","+{!term f=dept_id_s v=$row.dept_ss_dv}^=0 _val_:id_i", 
            "depts.fl","id",
            "depts.fl","score",
            "depts.fl","[subquery]",// <- here is a trouble
            "depts.indent","true",
            "depts.rows",""+deptMultiplier,
            "depts.start","0"), ErrorCode.BAD_REQUEST);
  }
  
  @Test
  public void testDupePrefix() throws Exception {
    assertQEx("subquery name clash", req(new String[]{"q","name_s:(john nancy)", "indent","true",
      "fl","name_s_dv,depts:[subquery]",
      "fl","depts:[subquery]",
      "rows","" + (2 * peopleMultiplier),
      "depts.q","{!term f=dept_id_s v=$row.dept_ss_dv}", 
      "depts.fl","text_t",
      "depts.indent","true",
      "depts.rows",""+deptMultiplier,
      
      "depts_i.q","{!term f=dept_id_i v=$depts_i.row.dept_i_dv}", 
      "depts_i.fl","text_t", // multi val subquery param check
      "depts_i.fl","dept_id_s_dv",
      "depts_i.indent","true",
      "depts_i.rows",""+deptMultiplier}
  ), ErrorCode.BAD_REQUEST);
  }
  
  @Test
  public void testJustJohnJson() throws Exception {
    
    final SolrQueryRequest johnTwoFL = req(johnAndNancyParams);
    ModifiableSolrParams params = new ModifiableSolrParams(johnTwoFL.getParams());
    params.set("q","name_s:john");
    johnTwoFL.setParams(params);
    assertJQ(johnTwoFL,
        "/response/docs/[0]/depts/docs/[0]=={text_t:\"These guys develop stuff\"}",
        "/response/docs/[" + (peopleMultiplier-1) + "]/depts/docs/[" + (deptMultiplier-1) + "]=={text_t:\"These guys develop stuff\"}",
        
        "/response/docs/[0]/depts_i/docs/[0]=={dept_id_s_dv:\"Engineering\", text_t:\"These guys develop stuff\"}",// seem like key order doesn't matter , well
        "/response/docs/[" + (peopleMultiplier-1) + "]/depts_i/docs/[" + (deptMultiplier-1) + "]=="
            + "{text_t:\"These guys develop stuff\", dept_id_s_dv:\"Engineering\"}");
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void testJustJohnJavabin() throws Exception {
    final SolrQueryRequest johnTwoFL = req(johnAndNancyParams);
    ModifiableSolrParams params = new ModifiableSolrParams(johnTwoFL.getParams());
    params.set("q","name_s:john");
    params.set("wt","javabin");
    
    johnTwoFL.setParams(params);
    
    final NamedList<Object> unmarshalled;
    SolrCore core = johnTwoFL.getCore();
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrRequestInfo.setRequestInfo(new SolrRequestInfo(johnTwoFL, rsp));

    SolrQueryResponse response = h.queryAndResponse(
        johnTwoFL.getParams().get(CommonParams.QT), johnTwoFL);

    BinaryQueryResponseWriter responseWriter = (BinaryQueryResponseWriter) core.getQueryResponseWriter(johnTwoFL);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    responseWriter.write(bytes, johnTwoFL, response);

    try (JavaBinCodec jbc = new JavaBinCodec()) {
      unmarshalled = (NamedList<Object>) jbc.unmarshal(
          new ByteArrayInputStream(bytes.toByteArray()));
    }

    johnTwoFL.close();
    SolrRequestInfo.clearRequestInfo();
    
    SolrDocumentList resultDocs = (SolrDocumentList)(unmarshalled.get("response"));
    
      Map<String,String> engText = new HashMap<>();
      engText.put("text_t", "These guys develop stuff");
      
      Map<String,String> engId = new HashMap<>();
      engId.put("text_t", "These guys develop stuff");
      engId.put("dept_id_s_dv", "Engineering");
      
      for (int docNum : new int []{0, peopleMultiplier-1}) {
        SolrDocument employeeDoc = resultDocs.get(docNum);
        assertEquals("john", employeeDoc.getFieldValue("name_s_dv"));
        for (String subResult : new String []{"depts", "depts_i"}) {

          SolrDocumentList subDoc = (SolrDocumentList)employeeDoc.getFieldValue(subResult);
          for (int deptNum : new int []{0, deptMultiplier-1}) {
            SolrDocument deptDoc = subDoc.get(deptNum);
            Object expectedDept = (subResult.equals("depts") ? engText : engId);
            assertTrue( "" + expectedDept + " equals to " + deptDoc,
                expectedDept.equals(deptDoc));
          }
      }
    }
  }
  
  @Test
  public void testExceptionPropagation() throws Exception {
    final SolrQueryRequest r = req("q","name_s:dave", "indent","true",
        "fl","depts:[subquery]", 
        "rows","" + ( peopleMultiplier),
        "depts.q","{!lucene}(", 
        "depts.fl","text_t",
        "depts.indent","true",
        "depts.rows",""+(deptMultiplier*2),
        "depts.logParamsList","q,fl,rows,subq1.row.dept_ss_dv");

   // System.out.println(h.query(r));
    
    assertQEx("wrong subquery",
        r,
        ErrorCode.BAD_REQUEST);
    
      assertQEx(   "",     req("q","name_s:dave", "indent","true",
            "fl","depts:[subquery]", 
            "rows","1",
            "depts.q","{!lucene}", 
            "depts.fl","text_t",
            "depts.indent","true",
            "depts.rows","NAN",
            "depts.logParamsList","q,fl,rows,subq1.row.dept_ss_dv"),
          ErrorCode.BAD_REQUEST);
  }
  
  @Test
  public void testMultiValue() throws Exception {
    
    String [] happyPathAsserts = new String[]{
        "count(//result/doc/str[@name='name_s_dv'][.='dave']/../result[@name='subq1']/doc/str[@name='text_t'][.='These guys develop stuff'])="+
        (peopleMultiplier * deptMultiplier),
        "count(//result/doc/str[@name='name_s_dv'][.='dave']/../result[@name='subq1']/doc/str[@name='text_t'][.='These guys help customers'])="+
            (peopleMultiplier * deptMultiplier),
        "//result[@numFound="+peopleMultiplier+"]"};
    Random random1 = random();
    
    assertQ("dave works at both, whether we set a  default separator or both",
        req(new String[]{"q","name_s:dave", "indent","true",
        "fl", (random().nextBoolean() ? "name_s_dv,dept_ss_dv" : "*") + 
              ",subq1:[subquery " +((random1.nextBoolean() ? "" : "separator=,"))+"]", 
        "rows","" + peopleMultiplier,
        "subq1.q","{!terms f=dept_id_s v=$row.dept_ss_dv "+((random1.nextBoolean() ? "" : "separator=,"))+"}", 
        "subq1.fl","text_t",
        "subq1.indent","true",
        "subq1.rows",""+(deptMultiplier*2),
        "subq1.logParamsList","q,fl,rows,row.dept_ss_dv"}),
        happyPathAsserts        
            );
    
    assertQ("even via numbers",
        req("q","name_s:dave", "indent","true",
            "fl","dept_is_dv,name_s_dv,subq1:[subquery]", 
            "rows","" + ( peopleMultiplier),
            "subq1.q","{!terms f=dept_id_i v=$row.dept_is_dv}", 
            "subq1.fl","text_t",
            "subq1.indent","true",
            "subq1.rows",""+(deptMultiplier*2)),
        happyPathAsserts        
            );
  
    
    assertQ("even if we set a separator both",
        req("q","name_s:dave", "indent","true",
            "fl","dept_ss_dv,name_s_dv,name_s_dv,subq1:[subquery separator=\" \"]", 
            "rows","" + ( peopleMultiplier),
            "subq1.q","{!terms f=dept_id_s v=$row.dept_ss_dv separator=\" \"}", 
            "subq1.fl","text_t",
            "subq1.indent","true",
            "subq1.rows",""+(deptMultiplier*2)),
        happyPathAsserts        
            );
    
    String [] noMatchAtSubQ = new String[] {
        "count(//result/doc/str[@name='name_s_dv'][.='dave']/../result[@name='subq1'][@numFound=0])="+
        (peopleMultiplier),
    "//result[@numFound="+peopleMultiplier+"]" };
        
    assertQ("different separators, no match",
        req("q","name_s:dave", "indent","true",
            "fl","dept_ss_dv,name_s_dv,subq1:[subquery]", 
            "rows","" + ( peopleMultiplier),
            "subq1.q","{!terms f=dept_id_s v=$row.dept_ss_dv separator=\" \"}", 
            "subq1.fl","text_t",
            "subq1.indent","true",
            "subq1.rows",""+(deptMultiplier*2)),
        noMatchAtSubQ
    );
    
    assertQ("and no matter where",
        req("q","name_s:dave", "indent","true",
            "fl","dept_ss_dv,name_s_dv,subq1:[subquery separator=\" \"]", 
            "rows","" + ( peopleMultiplier),
            "subq1.q","{!terms f=dept_id_s v=$row.dept_ss_dv}", 
            "subq1.fl","text_t",
            "subq1.indent","true",
            "subq1.rows",""+(deptMultiplier*2)),
        noMatchAtSubQ
    );
    
    assertQ("setting a wrong parser gets you nowhere",
        req("q","name_s:dave", "indent","true",
            "fl","dept_ss_dv,name_s_dv,subq1:[subquery]", 
            "rows","" + ( peopleMultiplier),
            "subq1.q","{!term f=dept_id_s v=$row.dept_ss_dv}", 
            "subq1.fl","text_t",
            "subq1.indent","true",
            "subq1.rows",""+(deptMultiplier*2)),
        noMatchAtSubQ        
            );
    
    assertQ("but it luckily works with default query parser, but it's not really reliable",
        req("q","name_s:dave", "indent","true",
            "fl","dept_ss_dv,name_s_dv,subq1:[subquery separator=\" \"]", 
            "rows","" + ( peopleMultiplier),
            "subq1.q","{!lucene df=dept_id_s v=$row.dept_ss_dv}", 
            "subq1.fl","text_t",
            "subq1.indent","true",
            "subq1.rows",""+(deptMultiplier*2)),
        happyPathAsserts        
            );
    
    assertQ("even lucene qp can't help at any separator but space",
        req("q","name_s:dave", "indent","true",
            "fl","dept_ss_dv,name_s_dv,"
                + "subq1:[subquery "+(random().nextBoolean() ? "" : "separator=" +((random().nextBoolean() ? "" : ",")))+"]", 
            "rows","" + ( peopleMultiplier),
            "subq1.q","{!lucene df=dept_id_s v=$row.dept_ss_dv}", 
            "subq1.fl","text_t",
            "subq1.indent","true",
            "subq1.rows",""+(deptMultiplier*2)),
        noMatchAtSubQ        
            );
  }

  static String[] daveMultiValueSearchParams(Random random, int peopleMult, int deptMult) {
    return new String[]{"q","name_s:dave", "indent","true",
        "fl",(random().nextBoolean() ? "name_s_dv" : "*")+ //"dept_ss_dv,
                    ",subq1:[subquery "
                +((random.nextBoolean() ? "" : "separator=,"))+"]", 
        "rows","" + peopleMult,
        "subq1.q","{!terms f=dept_id_s v=$row.dept_ss_dv "+((random.nextBoolean() ? "" : "separator=,"))+"}", 
        "subq1.fl","text_t",
        "subq1.indent","true",
        "subq1.rows",""+(deptMult*2),
        "subq1.logParamsList","q,fl,rows,row.dept_ss_dv"};
  }
}
