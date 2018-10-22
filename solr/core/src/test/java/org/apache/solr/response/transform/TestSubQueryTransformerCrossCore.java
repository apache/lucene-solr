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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.servlet.DirectSolrConnection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class TestSubQueryTransformerCrossCore extends SolrTestCaseJ4 {

  private static SolrCore fromCore;

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig-basic.xml","schema-docValuesJoin.xml");
    final CoreContainer coreContainer = h.getCoreContainer();

    fromCore = coreContainer.create("fromCore", //FileSystems.getDefault().getPath( TEST_HOME()), ImmutableMap.of("config","solrconfig-basic.xml","schema","schema-docValuesJoin.xml"
        ImmutableMap.of("configSet", "minimal")
        );
    assertU(add(doc("id", "1","name_s", "john", "title_s", "Director", "dept_ss_dv","Engineering",
        "text_t","These guys develop stuff")));
    assertU(add(doc("id", "2","name_s", "mark", "title_s", "VP", "dept_ss_dv","Marketing",
        "text_t","These guys make you look good")));
    assertU(add(doc("id", "3","name_s", "nancy", "title_s", "MTS", "dept_ss_dv","Sales",
        "text_t","These guys sell stuff")));
    assertU(add(doc("id", "4","name_s", "dave", "title_s", "MTS", "dept_ss_dv","Support", "dept_ss_dv","Engineering"
        , "text_t","These guys help customers")));
    assertU(add(doc("id", "5","name_s", "tina", "title_s", "VP", "dept_ss_dv","Engineering",
        "text_t","These guys develop stuff")));
    assertU(commit());

    update(fromCore, add(doc("id","10", "dept_id_s", "Engineering", "text_t","These guys develop stuff", "salary_i_dv", "1000")));
    update(fromCore, add(doc("id","11", "dept_id_s", "Marketing", "text_t","These guys make you look good","salary_i_dv", "1500")));
    update(fromCore, add(doc("id","12", "dept_id_s", "Sales", "text_t","These guys sell stuff","salary_i_dv", "1600")));
    update(fromCore, add(doc("id","13", "dept_id_s", "Support", "text_t","These guys help customers","salary_i_dv", "800")));
    update(fromCore, commit());
  }


  public static String update(SolrCore core, String xml) throws Exception {
    DirectSolrConnection connection = new DirectSolrConnection(core);
    SolrRequestHandler handler = core.getRequestHandler("/update");
    return connection.request(handler, null, xml);
  }

  @Test
  public void testSameCoreSingleField() throws Exception {
    assertQ("subq1.fl is limited to single field",
        req("q","name_s:john",
            "fl","*,depts:[subquery fromIndex=fromCore]", 
            "depts.q","{!term f=dept_id_s v=$row.dept_ss_dv}", 
            "depts.fl","text_t"),
        "//result/doc/str[@name='name_s_dv'][.='john']/../result[@name='depts']/doc/str[@name='text_t'][.='These guys develop stuff']",
        "count(//result/doc/str[@name='name_s_dv'][.='john']/../result[@name='depts']/doc/*)=1");// only text_t
  }

  @Test
  public void testAbsentCore() throws Exception {
    assertQEx("from index not exist",
        req("q","name_s:dave",
            "fl","*,depts:[subquery fromIndex=fromCore2]",
            "depts.q","{!term f=dept_id_s v=$row.dept_ss_dv}", 
            "depts.fl","text_t"),
        SolrException.ErrorCode.BAD_REQUEST
        );
    
  }
  
  @Test
  public void testCrossCoreSubQueryTransformer() throws Exception {
  
    assertQ("make sure request is parsed in this core",
        req("q","name_s:john",
            "fl","*,depts:[subquery]",  
            // text is tokenized and can be found, despite there is no substitution magic
                                           "depts.q","{!field f=text_t}These guys"),
                                           "//result/doc/str[@name='name_s_dv'][.='john']/../result[@name='depts']/doc"
        );
    
    assertQ("make sure request is parsed in that core",
        req("q","name_s:john",
            "fl","*,depts:[subquery fromIndex=fromCore]",  
            // text is NOT tokenized and can NOT be found
                                           "depts.q","{!field f=text_t}These guys"),
                                           "count(//result/doc/str[@name='name_s_dv'][.='john']/../result[@name='depts']/doc)=0"
        );

    assertQ("make sure request is parsed in that core",
        req("q","-name_s:dave", "indent", "true",
            "fl","*,depts:[subquery fromIndex=fromCore]",  
            // stored text (text_t is string in minimal configset) can be found as 
                                           "depts.q","{!field f=text_t v=$row.text_t}",
                                           "depts.fl", "dept_id_s" ),
                          "//result/doc/str[@name='name_s_dv'][.='john']/.."
                          + "/result[@name='depts']/doc/str[@name='dept_id_s'][.='Engineering']",
                          "//result/doc/str[@name='name_s_dv'][.='tina']/.."
                          + "/result[@name='depts']/doc/str[@name='dept_id_s'][.='Engineering']",
                          "//result/doc/str[@name='name_s_dv'][.='mark']/.."
                          + "/result[@name='depts']/doc/str[@name='dept_id_s'][.='Marketing']"
        );
  }

  @AfterClass
  public static void nukeAll() {
    fromCore = null;
  }
}
