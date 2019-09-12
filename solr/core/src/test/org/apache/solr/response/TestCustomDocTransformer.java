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
package org.apache.solr.response;

import java.io.IOException;

import org.apache.lucene.index.IndexableField;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.TransformerFactory;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCustomDocTransformer extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-doctransformers.xml","schema.xml");
  }

  @After
  public void cleanup() throws Exception {
    assertU(delQ("*:*"));
    assertU(commit());
  }

  @Test
  public void testCustomTransformer() throws Exception {
    // Build a simple index
    int max = 10;
    for(int i=0; i<max; i++) {
      SolrInputDocument sdoc = new SolrInputDocument();
      sdoc.addField("id", i);
      sdoc.addField("subject", "xx");
      sdoc.addField("title", "title_"+i);
      updateJ(jsonAdd(sdoc), null);
    }
    assertU(commit());
    assertQ(req("q", "*:*"), "//*[@numFound='" + max + "']");
    
    assertQ( req(
        "q", "*:*", 
        "fl", "id,out:[custom extra=subject,title]"), 
        // Check that the concatenated fields make it in the results
        "//*[@numFound='" + max + "']",
        "//str[.='xx#title_0#']",
        "//str[.='xx#title_1#']",
        "//str[.='xx#title_2#']",
        "//str[.='xx#title_3#']");
  }
  
  public static class CustomTransformerFactory extends TransformerFactory {
    @Override
    public DocTransformer create(String field, SolrParams params, SolrQueryRequest req) {
      String[] extra = null;
      String ext = params.get("extra");
      if(ext!=null) {
        extra = ext.split(",");
      }
      return new CustomTransformer(field, extra);
    }
  }
  
  public static class CustomTransformer extends DocTransformer {
    final String name;
    final String[] extra;
    final StringBuilder str = new StringBuilder();
    
    public CustomTransformer(String name, String[] extra) {
      this.name = name;
      this.extra = extra;
    }
    
    @Override
    public String getName() {
      return "custom";
    }

    @Override
    public String[] getExtraRequestFields() {
      return extra;
    }

    /**
     * This transformer simply concatenates the values of multiple fields
     */
    @Override
    public void transform(SolrDocument doc, int docid) throws IOException {
      str.setLength(0);
      for(String s : extra) {
        String v = getAsString(s, doc);
        str.append(v).append('#');
      }
      System.out.println( "HELLO: "+str );
      doc.setField(name, str.toString());
    }
  }
  public static String getAsString(String field, SolrDocument doc) {
    Object v = doc.getFirstValue(field);
    if(v != null) {
      if(v instanceof IndexableField) {
        return ((IndexableField)v).stringValue();
      }
      return v.toString();
    }
    return null;
  }
}
