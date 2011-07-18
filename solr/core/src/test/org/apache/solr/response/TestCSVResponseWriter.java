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

package org.apache.solr.response;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.ReturnFields;
import org.junit.*;

import java.io.StringWriter;

public class TestCSVResponseWriter extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema12.xml");
    createIndex();
  }

  public static void createIndex() {
    assertU(adoc("id","1", "foo_i","-1", "foo_s","hi", "foo_l","12345678987654321", "foo_b","false", "foo_f","1.414","foo_d","-1.0E300","foo_dt","2000-01-02T03:04:05Z"));
    assertU(adoc("id","2", "v_ss","hi",  "v_ss","there", "v2_ss","nice", "v2_ss","output"));
    assertU(commit());
  }

  
  @Test
  public void testCSVOutput() throws Exception {
    // test our basic types,and that fields come back in the requested order
    assertEquals("id,foo_s,foo_i,foo_l,foo_b,foo_f,foo_d,foo_dt\n1,hi,-1,12345678987654321,false,1.414,-1.0E300,2000-01-02T03:04:05Z\n"
    , h.query(req("q","id:1", "wt","csv", "fl","id,foo_s,foo_i,foo_l,foo_b,foo_f,foo_d,foo_dt")));

    // test retrieving score, csv.header
    assertEquals("1,0.0,hi\n"
    , h.query(req("q","id:1^0", "wt","csv", "csv.header","false", "fl","id,score,foo_s")));

    // test multivalued
    assertEquals("2,\"hi,there\"\n"
    , h.query(req("q","id:2", "wt","csv", "csv.header","false", "fl","id,v_ss")));

    // test separator change
    assertEquals("2|\"hi|there\"\n"
    , h.query(req("q","id:2", "wt","csv", "csv.header","false", "csv.separator","|", "fl","id,v_ss")));

    // test mv separator change
    assertEquals("2,hi|there\n"
    , h.query(req("q","id:2", "wt","csv", "csv.header","false", "csv.mv.separator","|", "fl","id,v_ss")));

    // test mv separator change for a single field
    assertEquals("2,hi|there,nice:output\n"
    , h.query(req("q","id:2", "wt","csv", "csv.header","false", "csv.mv.separator","|", "f.v2_ss.csv.separator",":", "fl","id,v_ss,v2_ss")));

    // test retrieving fields from index
    String result = h.query(req("q","*:*", "wt","csv", "csv.header","true", "fl","*,score"));
    for (String field : "id,foo_s,foo_i,foo_l,foo_b,foo_f,foo_d,foo_dt,v_ss,v2_ss,score".split(",")) {
      assertTrue(result.indexOf(field) >= 0);
    }

    // test null values
    assertEquals("2,,hi|there\n"
    , h.query(req("q","id:2", "wt","csv", "csv.header","false", "csv.mv.separator","|", "fl","id,foo_s,v_ss")));

    // test alternate null value
    assertEquals("2,NULL,hi|there\n"
    , h.query(req("q","id:2", "wt","csv", "csv.header","false", "csv.mv.separator","|", "csv.null","NULL","fl","id,foo_s,v_ss")));

    // test alternate newline
    assertEquals("2,\"hi,there\"\r\n"
    , h.query(req("q","id:2", "wt","csv", "csv.header","false", "csv.newline","\r\n", "fl","id,v_ss")));

    // test alternate encapsulator
    assertEquals("2,'hi,there'\n"
    , h.query(req("q","id:2", "wt","csv", "csv.header","false", "csv.encapsulator","'", "fl","id,v_ss")));

    // test using escape instead of encapsulator
    assertEquals("2,hi\\,there\n"
    , h.query(req("q","id:2", "wt","csv", "csv.header","false", "csv.escape","\\", "fl","id,v_ss")));

    // test multiple lines
    assertEquals("1,,hi\n2,\"hi,there\",\n"
    , h.query(req("q","id:[1 TO 2]", "wt","csv", "csv.header","false", "fl","id,v_ss,foo_s")));


    // now test SolrDocumentList
    SolrDocument d = new SolrDocument();
    SolrDocument d1 = d;
    d.addField("id","1");
    d.addField("foo_i",-1);
    d.addField("foo_s","hi");
    d.addField("foo_l","12345678987654321L");
    d.addField("foo_b",false);
    d.addField("foo_f",1.414f);
    d.addField("foo_d",-1.0E300);
    d.addField("foo_dt", DateUtil.parseDate("2000-01-02T03:04:05Z"));
    d.addField("score", "2.718");

    d = new SolrDocument();
    SolrDocument d2 = d;
    d.addField("id","2");
    d.addField("v_ss","hi");
    d.addField("v_ss","there");
    d.addField("v2_ss","nice");
    d.addField("v2_ss","output");
    d.addField("score", "89.83");

    SolrDocumentList sdl = new SolrDocumentList();
    sdl.add(d1);
    sdl.add(d2);
    
    SolrQueryRequest req = req("q","*:*");
    SolrQueryResponse rsp = new SolrQueryResponse();
    rsp.add("response", sdl);
    QueryResponseWriter w = new CSVResponseWriter();
    
    rsp.setReturnFields( new ReturnFields("id,foo_s", req) );
    StringWriter buf = new StringWriter();
    w.write(buf, req, rsp);
    assertEquals("id,foo_s\n1,hi\n2,\n", buf.toString());

    // try scores
    rsp.setReturnFields( new ReturnFields("id,score,foo_s", req) );
    buf = new StringWriter();
    w.write(buf, req, rsp);
    assertEquals("id,score,foo_s\n1,2.718,hi\n2,89.83,\n", buf.toString());

    // get field values from docs... should be ordered and not include score unless requested
    rsp.setReturnFields( new ReturnFields("*", req) );
    buf = new StringWriter();
    w.write(buf, req, rsp);
    assertEquals("id,foo_i,foo_s,foo_l,foo_b,foo_f,foo_d,foo_dt,v_ss,v2_ss\n" +
        "1,-1,hi,12345678987654321L,false,1.414,-1.0E300,2000-01-02T03:04:05Z,,\n" +
        "2,,,,,,,,\"hi,there\",\"nice,output\"\n",
      buf.toString());
    

    // get field values and scores - just check that the scores are there... we don't guarantee where
    rsp.setReturnFields( new ReturnFields("*,score", req) );
    buf = new StringWriter();
    w.write(buf, req, rsp);
    String s = buf.toString();
    assertTrue(s.indexOf("score") >=0 && s.indexOf("2.718") > 0 && s.indexOf("89.83") > 0 );

    req.close();
  }

}