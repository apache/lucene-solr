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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.ContentStreamBase;
import org.junit.Test;

@SuppressSSL 
public class TestSubQueryTransformerDistrib extends AbstractFullDistribZkTestBase {
  
  @Override
  protected String getCloudSchemaFile() {
    return "schema-docValuesJoin.xml";
  }
  
  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig-basic.xml";
  }
  
  @SuppressWarnings("serial")
  @Test
  public void test() throws SolrServerException, IOException {
    int peopleMultiplier = atLeast(1);
    int deptMultiplier = atLeast(1);
    
    final String people = "people";
    createCollection(people, 2, 1, 10);

    
    final String depts = "departments";
    createCollection(depts, 2, 1, 10);

    
    createIndex(people, peopleMultiplier, depts, deptMultiplier);
    
    Random random1 = random();
    
    {
     
      final QueryRequest  qr = new QueryRequest(params(
          new String[]{"q","name_s:dave", "indent","true",
          "fl","*,depts:[subquery "+((random1.nextBoolean() ? "" : "separator=,"))+"]", 
          "rows","" + peopleMultiplier,
          "depts.q","{!terms f=dept_id_s v=$row.dept_ss_dv "+((random1.nextBoolean() ? "" : "separator=,"))+"}", 
          "depts.fl","text_t",
          "depts.indent","true",
          "depts.collection","departments",
          "depts.rows",""+(deptMultiplier*2),
          "depts.logParamsList","q,fl,rows,row.dept_ss_dv"}));
      final QueryResponse  rsp = new QueryResponse();
      rsp.setResponse(cloudClient.request(qr, people));
      final SolrDocumentList hits = rsp.getResults();
      
      assertEquals(peopleMultiplier, hits.getNumFound());
      
      Map<String,String> engText = new HashMap<String,String>() {
        { put("text_t", "These guys develop stuff");
        }
      };
      Map<String,String> suppText = new HashMap<String,String>() {
        { put("text_t", "These guys help customers");
        }
      };
      
      int engineer = 0;
      int support = 0;
      
      for (int res : new int [] {0, (peopleMultiplier-1) /2, peopleMultiplier-1}) {
        SolrDocument doc = hits.get(res);
        assertEquals("dave", doc.getFieldValue("name_s_dv"));
        SolrDocumentList relDepts = (SolrDocumentList) doc.getFieldValue("depts");
        assertEquals("dave works in both depts "+rsp,
            deptMultiplier * 2, relDepts.getNumFound());
        for (int deptN = 0 ; deptN < relDepts.getNumFound(); deptN++ ) {
          SolrDocument deptDoc = relDepts.get(deptN);
          assertTrue(deptDoc + "should be either "+engText +" or "+suppText,
              (engText.equals(deptDoc) && ++engineer>0) || 
                   (suppText.equals(deptDoc) && ++support>0));
        }
      }
      assertEquals(hits.toString(), engineer, support); 
    }
    
  }

  private void createIndex(String people, int peopleMultiplier, String depts, int deptMultiplier)
      throws SolrServerException, IOException {
    int id=0;
    List<String> peopleDocs = new ArrayList<>();
    for (int p=0; p < peopleMultiplier; p++){

      peopleDocs.add(add(doc("id", ""+id++,"name_s", "john", "title_s", "Director", 
                                                      "dept_ss_dv","Engineering",
                                                      "dept_i", "0",
                                                      "dept_is", "0")));
      peopleDocs.add(add(doc("id", ""+id++,"name_s", "mark", "title_s", "VP", 
                                                         "dept_ss_dv","Marketing",
                                                         "dept_i", "1",
                                                         "dept_is", "1")));
      peopleDocs.add(add(doc("id", ""+id++,"name_s", "nancy", "title_s", "MTS",
                                                         "dept_ss_dv","Sales",
                                                         "dept_i", "2",
                                                         "dept_is", "2")));
      peopleDocs.add(add(doc("id", ""+id++,"name_s", "dave", "title_s", "MTS", 
                                                         "dept_ss_dv","Support", "dept_ss_dv","Engineering",
                                                         "dept_i", "3",
                                                         "dept_is", "3", "dept_is", "0")));
      peopleDocs.add(add(doc("id", ""+id++,"name_s", "tina", "title_s", "VP", 
                                                         "dept_ss_dv","Engineering",
                                                         "dept_i", "0",
                                                         "dept_is", "0")));
    }

    addDocs(people, peopleDocs);

    List<String> deptsDocs = new ArrayList<>();
    for (int d=0; d < deptMultiplier; d++) {
      deptsDocs.add(add(doc("id",""+id++, "dept_id_s", "Engineering", "text_t","These guys develop stuff", "salary_i_dv", "1000",
                                     "dept_id_i", "0")));
      deptsDocs.add(add(doc("id",""+id++, "dept_id_s", "Marketing", "text_t","These guys make you look good","salary_i_dv", "1500",
                                     "dept_id_i", "1")));
      deptsDocs.add(add(doc("id",""+id++, "dept_id_s", "Sales", "text_t","These guys sell stuff","salary_i_dv", "1600",
                                    "dept_id_i", "2")));
      deptsDocs.add(add(doc("id",""+id++, "dept_id_s", "Support", "text_t","These guys help customers","salary_i_dv", "800",
                                    "dept_id_i", "3")));
      
    }
    addDocs(depts, deptsDocs);
  }

  private void addDocs(String collection, List<String> docs) throws SolrServerException, IOException {
    StringBuilder upd = new StringBuilder("<update>");
    for (Iterator<String> iterator = docs.iterator(); iterator.hasNext();) {
      String add =  iterator.next();
      upd.append(add);
      if (rarely()) {
        upd.append(commit("softCommit", "true"));
      }
      if (!rarely() || !iterator.hasNext()) {
        if (!iterator.hasNext()) {
          upd.append(commit("softCommit", "false"));
        }
        upd.append("</update>");
        
        ContentStreamUpdateRequest req = new ContentStreamUpdateRequest("/update");
        req.addContentStream(new ContentStreamBase.StringStream(upd.toString(),"text/xml"));
        
        cloudClient.request(req, collection);
        upd.setLength("<update>".length());
      }
    }
  }
}
