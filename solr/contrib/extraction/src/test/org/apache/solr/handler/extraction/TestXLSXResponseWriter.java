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
package org.apache.solr.handler.extraction;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Date;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.search.SolrReturnFields;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestXLSXResponseWriter extends SolrTestCaseJ4 {

  private static XLSXResponseWriter writerXlsx;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false");
    initCore("solrconfig.xml","schema.xml",getFile("extraction/solr").getAbsolutePath());
    createIndex();
    //find a reference to the default response writer so we can redirect its output later
    SolrCore testCore = h.getCore();
    QueryResponseWriter writer = testCore.getQueryResponseWriter("xlsx");
    if (writer instanceof XLSXResponseWriter) {
      writerXlsx = (XLSXResponseWriter) testCore.getQueryResponseWriter("xlsx");
    } else {
      throw new Exception("XLSXResponseWriter not registered - are you using the right solrconfig?");
    }
  }

  public static void createIndex() {
    assertU(adoc("id","1", "foo_i","-1", "foo_s","hi", "foo_l","12345678987654321", "foo_b","false", "foo_f","1.414","foo_d","-1.0E300","foo_dt1","2000-01-02T03:04:05Z"));
    assertU(adoc("id","2", "v_ss","hi",  "v_ss","there", "v2_ss","nice", "v2_ss","output", "shouldbeunstored","foo"));
    assertU(adoc("id","3", "shouldbeunstored","foo"));
    assertU(adoc("id","4", "foo_s1","foo"));
    assertU(commit());
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

    // check it read solrconfig
    assertEquals("Foo Integer", writerXlsx.colNamesMap.get("foo_i"));
    assertTrue("1 specific width read in from solrconfig", writerXlsx.colWidthsMap.get("foo_l") == 50);

    assertTrue("All names read in from solrconfig", writerXlsx.colNamesMap.size() == 4);
    assertTrue("All widths read in from solrconfig", writerXlsx.colWidthsMap.size() == 4);

    // test our basic types,and that fields come back in the requested order
    XSSFSheet resultSheet = getWSResultForQuery(req("q","id:1", "wt","xlsx", "fl","id,foo_s,foo_i,foo_l,foo_b,foo_f,foo_d,foo_dt1"));

    assertEquals("ID,Foo String,Foo Integer,Foo Long,foo_b,foo_f,foo_d,foo_dt1\n1,hi,-1,12345678987654321,F,1.414,-1.0E300,2000-01-02T03:04:05Z\n"
        , getStringFromSheet(resultSheet));

    resultSheet = getWSResultForQuery(req("q","id:1^0", "wt","xlsx", "fl","id,score,foo_s"));
    // test retrieving score
    assertEquals("ID,score,Foo String\n1,0.0,hi\n", getStringFromSheet(resultSheet));
    // test column width (result is in 256ths of a character for some reason)
    assertEquals(3*256, resultSheet.getColumnWidth(0));

    resultSheet = getWSResultForQuery(req("q","id:2", "wt","xlsx", "fl","id,v_ss"));
    // test multivalued
    assertEquals("ID,v_ss\n2,hi; there\n", getStringFromSheet(resultSheet));

    // test retrieving fields from index
    resultSheet = getWSResultForQuery(req("q","*:*", "wt","xslx", "fl","*,score"));
    String result = getStringFromSheet(resultSheet);
    for (String field : "ID,Foo String,Foo Integer,Foo Long,foo_b,foo_f,foo_d,foo_dt1,v_ss,v2_ss,score".split(",")) {
      assertTrue(result.indexOf(field) >= 0);
    }

    // test null values
    resultSheet = getWSResultForQuery(req("q","id:2", "wt","xlsx", "fl","id,foo_s,v_ss"));
    assertEquals("ID,Foo String,v_ss\n2,,hi; there\n", getStringFromSheet(resultSheet));

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
    d.addField("foo_dt1", new Date(Instant.parse("2000-01-02T03:04:05Z").toEpochMilli()));
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
    rsp = new SolrQueryResponse();
    rsp.addResponse(sdl);

    rsp.setReturnFields( new SolrReturnFields("id,foo_s", req) );

    resultSheet = getWSResultForQuery(req, rsp);
    assertEquals("ID,Foo String\n1,hi\n2,\n", getStringFromSheet(resultSheet));

    // try scores
    rsp.setReturnFields( new SolrReturnFields("id,score,foo_s", req) );

    resultSheet = getWSResultForQuery(req, rsp);
    assertEquals("ID,score,Foo String\n1,2.718,hi\n2,89.83,\n", getStringFromSheet(resultSheet));

    // get field values from docs... should be ordered and not include score unless requested
    rsp.setReturnFields( new SolrReturnFields("*", req) );

    resultSheet = getWSResultForQuery(req, rsp);
    assertEquals("ID,Foo Integer,Foo String,Foo Long,foo_b,foo_f,foo_d,foo_dt1,v_ss,v2_ss\n" +
        "1,-1,hi,12345678987654321L,false,1.414,-1.0E300,2000-01-02T03:04:05Z,,\n" +
        "2,,,,,,,,hi; there,nice; output\n",
      getStringFromSheet(resultSheet));
    

    // get field values and scores - just check that the scores are there... we don't guarantee where
    rsp.setReturnFields( new SolrReturnFields("*,score", req) );
    resultSheet = getWSResultForQuery(req, rsp);
    String s = getStringFromSheet(resultSheet);
    assertTrue(s.indexOf("score") >=0 && s.indexOf("2.718") > 0 && s.indexOf("89.83") > 0 );
    
    // Test field globs
    rsp.setReturnFields( new SolrReturnFields("id,foo*", req) );
    resultSheet = getWSResultForQuery(req, rsp);
    assertEquals("ID,Foo Integer,Foo String,Foo Long,foo_b,foo_f,foo_d,foo_dt1\n" +
        "1,-1,hi,12345678987654321L,false,1.414,-1.0E300,2000-01-02T03:04:05Z\n" +
        "2,,,,,,,\n",
       getStringFromSheet(resultSheet));

    rsp.setReturnFields( new SolrReturnFields("id,*_d*", req) );
    resultSheet = getWSResultForQuery(req, rsp);
    assertEquals("ID,foo_d,foo_dt1\n" +
        "1,-1.0E300,2000-01-02T03:04:05Z\n" +
        "2,,\n",
      getStringFromSheet(resultSheet));

    // Test function queries
    rsp.setReturnFields( new SolrReturnFields("sum(1,1),id,exists(foo_s1),div(9,1),foo_f", req) );
    resultSheet = getWSResultForQuery(req, rsp);
    assertEquals("sum(1,1),ID,exists(foo_s1),div(9,1),foo_f\n" +
        ",1,,,1.414\n" +
        ",2,,,\n",
        getStringFromSheet(resultSheet));

    // Test transformers
    rsp.setReturnFields( new SolrReturnFields("mydocid:[docid],[explain]", req) );
    resultSheet = getWSResultForQuery(req, rsp);
    assertEquals("mydocid,[explain]\n" +
        ",\n" +
        ",\n",
        getStringFromSheet(resultSheet));

    req.close();
  }
  

  @Test
  public void testPseudoFields() throws Exception {
    // Use Pseudo Field
    SolrQueryRequest req = req("q","id:1", "wt","xlsx", "fl","XXX:id,foo_s");
    XSSFSheet resultSheet = getWSResultForQuery(req);
    assertEquals("XXX,Foo String\n1,hi\n", getStringFromSheet(resultSheet));
    
    String txt = getStringFromSheet(getWSResultForQuery(req("q","id:1", "wt","xlsx", "fl","XXX:id,YYY:[docid],FOO:foo_s")));
    String[] lines = txt.split("\n");
    assertEquals(2, lines.length);
    assertEquals("XXX,YYY,FOO", lines[0] );
    assertEquals("1,0,hi", lines[1] );

    //assertions specific to multiple pseudofields functions like abs, div, exists, etc.. (SOLR-5423)
    String funcText = getStringFromSheet(getWSResultForQuery(req("q","*", "wt","xlsx", "fl","XXX:id,YYY:exists(foo_s1)")));
    String[] funcLines = funcText.split("\n");
    assertEquals(5, funcLines.length);
    assertEquals("XXX,YYY", funcLines[0] );
    assertEquals("1,false", funcLines[1] );
    assertEquals("3,false", funcLines[3] );
  }

  // returns first worksheet as XLSXResponseWriter only returns one sheet
  private XSSFSheet getWSResultForQuery(SolrQueryRequest req) throws IOException, Exception {
    SolrQueryResponse rsp = h.queryAndResponse("standard", req);
    return getWSResultForQuery(req, rsp);
  }

  private XSSFSheet getWSResultForQuery(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException, Exception {
    ByteArrayOutputStream xmlBout = new ByteArrayOutputStream();
    writerXlsx.write(xmlBout, req, rsp);
    XSSFWorkbook output = new XSSFWorkbook(new ByteArrayInputStream(xmlBout.toByteArray()));
    XSSFSheet sheet = output.getSheetAt(0);
    req.close();
    output.close();
    return sheet;
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
    return output.toString();
  }
}
