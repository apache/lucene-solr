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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrReturnFields;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestXLSXResponseWriter extends SolrTestCaseJ4 {

  private static XLSXResponseWriter writerXlsx;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml","schema12.xml");
    TestCSVResponseWriter.createIndex();
    writerXlsx = new XLSXResponseWriter();
  }

  @AfterClass
  public static void cleanupWriter() throws Exception {
    writerXlsx = null;
  }

  @Test
  public void testStructuredDataViaBaseWriters() throws IOException, Exception {
    SolrQueryResponse rsp = new SolrQueryResponse();
    // Don't send a ContentStream back, this will fall back to the configured base writer.
    // But abuse the CONTENT key to ensure writer is also checking type
    rsp.add(RawResponseWriter.CONTENT, "test");
    rsp.add("foo", "bar");

    SolrQueryRequest r = req();

    // check Content-Type
    assertEquals("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", writerXlsx.getContentType(r, rsp));


    // test our basic types,and that fields come back in the requested order
    XSSFSheet resultSheet = getWSResultForQuery(req("q","id:1", "wt","xlsx", "fl","id,foo_s,foo_i,foo_l,foo_b,foo_f,foo_d,foo_dt"));

    String result = getStringFromSheet(resultSheet);
    assertEquals("id,foo_s,foo_i,foo_l,foo_b,foo_f,foo_d,foo_dt\n1,hi,-1,12345678987654321,false,1.414,-1.0E300,2000-01-02T03:04:05Z\n"
    , result);
/*
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

    // test csv field for polyfield (currency) SOLR-3959
    assertEquals("4,\"1.50\\,EUR\"\n"
    , h.query(req("q","id:4", "wt","csv", "csv.header","false", "fl","id,amount_c")));
 
    // test csv field for polyfield (latlon) SOLR-3959
    assertEquals("5,\"12.434\\,-134.1\"\n"
    , h.query(req("q","id:5", "wt","csv", "csv.header","false", "fl","id,store")) );
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

    // test SOLR-2970 not returning non-stored fields by default. Compare sorted list
    assertEquals(sortHeader("amount_c,store,v_ss,foo_b,v2_ss,foo_f,foo_i,foo_d,foo_s,foo_dt,id,foo_l\n")
    , sortHeader(h.query(req("q","id:3", "wt","csv", "csv.header","true", "fl","*", "rows","0"))));


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
    d.addField("foo_dt", new Date(Instant.parse("2000-01-02T03:04:05Z").toEpochMilli()));
    d.addField("score", "2.718");

    d = new SolrDocument();
    SolrDocument d2 = d;
    d.addField("id","2");
    d.addField("v_ss","hi");
    d.addField("v_ss","there");
    d.addField("v2_ss","nice");
    d.addField("v2_ss","output");
    d.addField("score", "89.83");
    d.addField("shouldbeunstored","foo");

    SolrDocumentList sdl = new SolrDocumentList();
    sdl.add(d1);
    sdl.add(d2);
    
    SolrQueryRequest req = req("q","*:*");
    SolrQueryResponse rsp = new SolrQueryResponse();
    rsp.addResponse(sdl);
    QueryResponseWriter w = new CSVResponseWriter();
    
    rsp.setReturnFields( new SolrReturnFields("id,foo_s", req) );
    StringWriter buf = new StringWriter();
    w.write(buf, req, rsp);
    assertEquals("id,foo_s\n1,hi\n2,\n", buf.toString());

    // try scores
    rsp.setReturnFields( new SolrReturnFields("id,score,foo_s", req) );
    buf = new StringWriter();
    w.write(buf, req, rsp);
    assertEquals("id,score,foo_s\n1,2.718,hi\n2,89.83,\n", buf.toString());

    // get field values from docs... should be ordered and not include score unless requested
    rsp.setReturnFields( new SolrReturnFields("*", req) );
    buf = new StringWriter();
    w.write(buf, req, rsp);
    assertEquals("id,foo_i,foo_s,foo_l,foo_b,foo_f,foo_d,foo_dt,v_ss,v2_ss\n" +
        "1,-1,hi,12345678987654321L,false,1.414,-1.0E300,2000-01-02T03:04:05Z,,\n" +
        "2,,,,,,,,\"hi,there\",\"nice,output\"\n",
      buf.toString());
    

    // get field values and scores - just check that the scores are there... we don't guarantee where
    rsp.setReturnFields( new SolrReturnFields("*,score", req) );
    buf = new StringWriter();
    w.write(buf, req, rsp);
    String s = buf.toString();
    assertTrue(s.indexOf("score") >=0 && s.indexOf("2.718") > 0 && s.indexOf("89.83") > 0 );
    
    // Test field globs
    rsp.setReturnFields( new SolrReturnFields("id,foo*", req) );
    buf = new StringWriter();
    w.write(buf, req, rsp);
    assertEquals("id,foo_i,foo_s,foo_l,foo_b,foo_f,foo_d,foo_dt\n" +
        "1,-1,hi,12345678987654321L,false,1.414,-1.0E300,2000-01-02T03:04:05Z\n" +
        "2,,,,,,,\n",
      buf.toString());

    rsp.setReturnFields( new SolrReturnFields("id,*_d*", req) );
    buf = new StringWriter();
    w.write(buf, req, rsp);
    assertEquals("id,foo_d,foo_dt\n" +
        "1,-1.0E300,2000-01-02T03:04:05Z\n" +
        "2,,\n",
      buf.toString());

    // Test function queries
    rsp.setReturnFields( new SolrReturnFields("sum(1,1),id,exists(foo_i),div(9,1),foo_f", req) );
    buf = new StringWriter();
    w.write(buf, req, rsp);
    assertEquals("\"sum(1,1)\",id,exists(foo_i),\"div(9,1)\",foo_f\n" +
        "\"\",1,,,1.414\n" +
        "\"\",2,,,\n",
        buf.toString());

    // Test transformers
    rsp.setReturnFields( new SolrReturnFields("mydocid:[docid],[explain]", req) );
    buf = new StringWriter();
    w.write(buf, req, rsp);
    assertEquals("mydocid,[explain]\n" +
        "\"\",\n" +
        "\"\",\n",
        buf.toString());

    req.close();
    */
  }
  

  @Test
  public void testPseudoFields() throws Exception {
    // Use Pseudo Field
    /*
    assertEquals("1,hi",
        h.query(req("q","id:1", "wt","csv", "csv.header","false", "fl","XXX:id,foo_s")).trim());
    
    String txt = h.query(req("q","id:1", "wt","csv", "csv.header","true", "fl","XXX:id,YYY:[docid],FOO:foo_s"));
    String[] lines = txt.split("\n");
    assertEquals(2, lines.length);
    assertEquals("XXX,YYY,FOO", lines[0] );
    assertEquals("1,0,hi", lines[1] );

    //assertions specific to multiple pseudofields functions like abs, div, exists, etc.. (SOLR-5423)
    String funcText = h.query(req("q","*", "wt","csv", "csv.header","true", "fl","XXX:id,YYY:exists(foo_i),exists(shouldbeunstored)"));
    String[] funcLines = funcText.split("\n");
    assertEquals(6, funcLines.length);
    assertEquals("XXX,YYY,exists(shouldbeunstored)", funcLines[0] );
    assertEquals("1,true,false", funcLines[1] );
    assertEquals("3,false,true", funcLines[3] );
    
    
    //assertions specific to single function without alias (SOLR-5423)
    String singleFuncText = h.query(req("q","*", "wt","csv", "csv.header","true", "fl","exists(shouldbeunstored),XXX:id"));
    String[] singleFuncLines = singleFuncText.split("\n");
    assertEquals(6, singleFuncLines.length);
    assertEquals("exists(shouldbeunstored),XXX", singleFuncLines[0] );
    assertEquals("false,1", singleFuncLines[1] );
    assertEquals("true,3", singleFuncLines[3] );
    */
  }

  // returns first worksheet as XLSXResponseWriter only returns one sheet
  private XSSFSheet getWSResultForQuery(SolrQueryRequest req) throws IOException {
    SolrQueryResponse rsp = new SolrQueryResponse();
    ByteArrayOutputStream xmlBout = new ByteArrayOutputStream();
    writerXlsx.write(xmlBout, req, rsp);
    XSSFWorkbook output = new XSSFWorkbook(new ByteArrayInputStream(xmlBout.toByteArray()));

    //TODO: DELETEME
    File file = new File("/Users/tdm/temp/solr.xlsx");
    FileOutputStream fod = new FileOutputStream(file);

    output.write(fod);
    fod.close();

    return output.getSheetAt(0);
  }

  private String getStringFromSheet(XSSFSheet sheet) {
    StringBuilder output = new StringBuilder();
    for (Row row: sheet) {
      for (Cell cell: row) {
        output.append(cell.getStringCellValue());
        output.append(",");
      }
      output.setLength(output.length() - 1);
      output.append("\n");
    }
    output.setLength(output.length() - 1);
    return output.toString();
  }
}
