package org.apache.solr.handler.extraction;
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

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.extraction.ExtractingDocumentLoader;
import org.apache.solr.handler.extraction.ExtractingParams;
import org.apache.solr.handler.extraction.ExtractingRequestHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 *
 *
 **/
public class ExtractingRequestHandlerTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml", "solr-extraction");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  @Test
  public void testExtraction() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);
    loadLocal("solr-word.pdf",
            "fmap.created", "extractedDate",
            "fmap.producer", "extractedProducer",
            "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
            "fmap.Creation-Date", "extractedDate",
            "fmap.AAPL:Keywords", "ignored_a",
            "fmap.xmpTPg:NPages", "ignored_a",
            "fmap.Author", "extractedAuthor",
            "fmap.content", "extractedContent",
           "literal.id", "one",
            "fmap.Last-Modified", "extractedDate"
    );
    assertQ(req("title:solr-word"), "//*[@numFound='0']");
    assertU(commit());
    assertQ(req("title:solr-word"), "//*[@numFound='1']");


    loadLocal("simple.html", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
            "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
            "fmap.Author", "extractedAuthor",
            "fmap.language", "extractedLanguage",
            "literal.id", "two",
            "fmap.content", "extractedContent",
            "fmap.Last-Modified", "extractedDate"
    );
    assertQ(req("title:Welcome"), "//*[@numFound='0']");
    assertU(commit());
    assertQ(req("title:Welcome"), "//*[@numFound='1']");


    loadLocal("simple.html",
      "literal.id","simple2",
      "uprefix", "t_",
      "lowernames", "true",
      "captureAttr", "true",
      "fmap.a","t_href",
      "fmap.content_type", "abcxyz",  // test that lowernames is applied before mapping, and uprefix is applied after mapping
      "commit", "true"  // test immediate commit
    );

    // test that purposely causes a failure to print out the doc for test debugging
    // assertQ(req("q","id:simple2","indent","true"), "//*[@numFound='0']");

    // test both lowernames and unknown field mapping
    //assertQ(req("+id:simple2 +t_content_type:[* TO *]"), "//*[@numFound='1']");
    assertQ(req("+id:simple2 +t_href:[* TO *]"), "//*[@numFound='1']");
    assertQ(req("+id:simple2 +t_abcxyz:[* TO *]"), "//*[@numFound='1']");

    // load again in the exact same way, but boost one field
    loadLocal("simple.html",
      "literal.id","simple3",
      "uprefix", "t_",
      "lowernames", "true",
      "captureAttr", "true",  "fmap.a","t_href",
      "commit", "true"

      ,"boost.t_href", "100.0"
    );

    assertQ(req("t_href:http"), "//*[@numFound='2']");
    assertQ(req("t_href:http"), "//doc[1]/str[.='simple3']");
    assertQ(req("+id:simple3 +t_content_type:[* TO *]"), "//*[@numFound='1']");//test lowercase and then uprefix

    // test capture
     loadLocal("simple.html",
      "literal.id","simple4",
      "uprefix", "t_",
      "capture","p",     // capture only what is in the title element
      "commit", "true"
    );
    assertQ(req("+id:simple4 +t_content:Solr"), "//*[@numFound='1']");
    assertQ(req("+id:simple4 +t_p:\"here is some text\""), "//*[@numFound='1']");

    loadLocal("version_control.xml", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
            "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
            "fmap.Author", "extractedAuthor",
            "literal.id", "three",
            "fmap.content", "extractedContent",
            "fmap.language", "extractedLanguage",
            "fmap.Last-Modified", "extractedDate"
    );
    assertQ(req("stream_name:version_control.xml"), "//*[@numFound='0']");
    assertU(commit());
    assertQ(req("stream_name:version_control.xml"), "//*[@numFound='1']");


  }


  @Test
  public void testDefaultField() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);
    try {
      ignoreException("unknown field 'a'");
      ignoreException("unknown field 'meta'");  // TODO: should this exception be happening?
      loadLocal("simple.html",
      "literal.id","simple2",
      "lowernames", "true",
        "captureAttr", "true",
        //"fmap.content_type", "abcxyz",
        "commit", "true"  // test immediate commit
      );
      assertTrue(false);

    } catch (SolrException e) {
      //do nothing
    } finally {
      resetExceptionIgnores();
    }
    

    loadLocal("simple.html",
      "literal.id","simple2",
      ExtractingParams.DEFAULT_FIELD, "defaultExtr",//test that unmapped fields go to the text field when no uprefix is specified
      "lowernames", "true",
      "captureAttr", "true",
      //"fmap.content_type", "abcxyz",
      "commit", "true"  // test immediate commit
    );
    assertQ(req("id:simple2"), "//*[@numFound='1']");
    assertQ(req("defaultExtr:http\\://www.apache.org"), "//*[@numFound='1']");

    //Test when both uprefix and default are specified.
    loadLocal("simple.html",
      "literal.id","simple2",
      ExtractingParams.DEFAULT_FIELD, "defaultExtr",//test that unmapped fields go to the text field when no uprefix is specified
            ExtractingParams.UNKNOWN_FIELD_PREFIX, "t_",
      "lowernames", "true",
      "captureAttr", "true",
      "fmap.a","t_href",
      //"fmap.content_type", "abcxyz",
      "commit", "true"  // test immediate commit
    );
    assertQ(req("+id:simple2 +t_href:[* TO *]"), "//*[@numFound='1']");
  }

  @Test
  public void testLiterals() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);
    //test literal
    loadLocal("version_control.xml", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
            "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
            "fmap.Author", "extractedAuthor",
            "fmap.content", "extractedContent",
            "literal.id", "one",
            "fmap.language", "extractedLanguage",
            "literal.extractionLiteralMV", "one",
            "literal.extractionLiteralMV", "two",
            "fmap.Last-Modified", "extractedDate"

    );
    assertQ(req("stream_name:version_control.xml"), "//*[@numFound='0']");
    assertU(commit());
    assertQ(req("stream_name:version_control.xml"), "//*[@numFound='1']");

    assertQ(req("extractionLiteralMV:one"), "//*[@numFound='1']");
    assertQ(req("extractionLiteralMV:two"), "//*[@numFound='1']");

    try {
      loadLocal("version_control.xml", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
              "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
              "fmap.Author", "extractedAuthor",
              "fmap.content", "extractedContent",
              "literal.id", "two",
              "fmap.language", "extractedLanguage",
              "literal.extractionLiteral", "one",
              "literal.extractionLiteral", "two",
              "fmap.Last-Modified", "extractedDate"
      );
      // TODO: original author did not specify why an exception should be thrown... how to fix?
      // assertTrue("Exception should have been thrown", false);
    } catch (SolrException e) {
      //nothing to see here, move along
    }

    loadLocal("version_control.xml", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
            "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
            "fmap.Author", "extractedAuthor",
            "fmap.content", "extractedContent",
            "literal.id", "three",
            "fmap.language", "extractedLanguage",
            "literal.extractionLiteral", "one",
            "fmap.Last-Modified", "extractedDate"
    );
    assertU(commit());
    assertQ(req("extractionLiteral:one"), "//*[@numFound='1']");

  }

  @Test
  public void testPlainTextSpecifyingMimeType() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);

    // Load plain text specifying MIME type:
    loadLocal("version_control.txt", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
            "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
            "fmap.Author", "extractedAuthor",
            "literal.id", "one",
            "fmap.language", "extractedLanguage",
            "fmap.content", "extractedContent",
            ExtractingParams.STREAM_TYPE, "text/plain"
    );
    assertQ(req("extractedContent:Apache"), "//*[@numFound='0']");
    assertU(commit());
    assertQ(req("extractedContent:Apache"), "//*[@numFound='1']");
  }

  @Test
  public void testPlainTextSpecifyingResourceName() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);

    // Load plain text specifying filename
    loadLocal("version_control.txt", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
            "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
            "fmap.Author", "extractedAuthor",
            "literal.id", "one",
            "fmap.language", "extractedLanguage",
            "fmap.content", "extractedContent",
            ExtractingParams.RESOURCE_NAME, "version_control.txt"
    );
    assertQ(req("extractedContent:Apache"), "//*[@numFound='0']");
    assertU(commit());
    assertQ(req("extractedContent:Apache"), "//*[@numFound='1']");
  }

  // Note: If you load a plain text file specifying neither MIME type nor filename, extraction will silently fail. This is because Tika's
  // automatic MIME type detection will fail, and it will default to using an empty-string-returning default parser

  @Test
  public void testExtractOnly() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);
    SolrQueryResponse rsp = loadLocal("solr-word.pdf", ExtractingParams.EXTRACT_ONLY, "true");
    assertTrue("rsp is null and it shouldn't be", rsp != null);
    NamedList list = rsp.getValues();

    String extraction = (String) list.get("solr-word.pdf");
    assertTrue("extraction is null and it shouldn't be", extraction != null);
    assertTrue(extraction + " does not contain " + "solr-word", extraction.indexOf("solr-word") != -1);

    NamedList nl = (NamedList) list.get("solr-word.pdf_metadata");
    assertTrue("nl is null and it shouldn't be", nl != null);
    Object title = nl.get("title");
    assertTrue("title is null and it shouldn't be", title != null);
    assertTrue(extraction.indexOf("<?xml") != -1);

    rsp = loadLocal("solr-word.pdf", ExtractingParams.EXTRACT_ONLY, "true",
            ExtractingParams.EXTRACT_FORMAT, ExtractingDocumentLoader.TEXT_FORMAT);
    assertTrue("rsp is null and it shouldn't be", rsp != null);
    list = rsp.getValues();

    extraction = (String) list.get("solr-word.pdf");
    assertTrue("extraction is null and it shouldn't be", extraction != null);
    assertTrue(extraction + " does not contain " + "solr-word", extraction.indexOf("solr-word") != -1);
    assertTrue(extraction.indexOf("<?xml") == -1);

    nl = (NamedList) list.get("solr-word.pdf_metadata");
    assertTrue("nl is null and it shouldn't be", nl != null);
    title = nl.get("title");
    assertTrue("title is null and it shouldn't be", title != null);



  }

  @Test
  public void testXPath() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);
    SolrQueryResponse rsp = loadLocal("example.html",
            ExtractingParams.XPATH_EXPRESSION, "/xhtml:html/xhtml:body/xhtml:a/descendant:node()",
            ExtractingParams.EXTRACT_ONLY, "true"
    );
    assertTrue("rsp is null and it shouldn't be", rsp != null);
    NamedList list = rsp.getValues();
    String val = (String) list.get("example.html");
    val = val.trim();
    assertTrue(val + " is not equal to " + "linkNews", val.equals("linkNews") == true);//there are two <a> tags, and they get collapesd
  }

  /** test arabic PDF extraction is functional */
  @Test
  public void testArabicPDF() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) 
      h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);

    loadLocal("arabic.pdf", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
        "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
        "fmap.Creation-Date", "extractedDate",
        "fmap.AAPL:Keywords", "ignored_a",
        "fmap.xmpTPg:NPages", "ignored_a",
        "fmap.Author", "extractedAuthor",
        "fmap.content", "wdf_nocase",
       "literal.id", "one",
        "fmap.Last-Modified", "extractedDate");
    assertQ(req("wdf_nocase:السلم"), "//result[@numFound=0]");
    assertU(commit());
    assertQ(req("wdf_nocase:السلم"), "//result[@numFound=1]");
  }

  @Test
  public void testTikaExceptionHandling() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) 
      h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);

    try{
      loadLocal("password-is-solrcell.docx",
          "literal.id", "one");
      fail("TikaException is expected because of trying to extract text from password protected word file.");
    }
    catch(Exception expected){}
    assertU(commit());
    assertQ(req("*:*"), "//result[@numFound=0]");

    try{
      loadLocal("password-is-solrcell.docx", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
          "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
          "fmap.Creation-Date", "extractedDate",
          "fmap.AAPL:Keywords", "ignored_a",
          "fmap.xmpTPg:NPages", "ignored_a",
          "fmap.Author", "extractedAuthor",
          "fmap.content", "wdf_nocase",
          "literal.id", "one",
          "ignoreTikaException", "true",  // set ignore flag
          "fmap.Last-Modified", "extractedDate");
    }
    catch(Exception e){
      fail("TikaException should be ignored.");
    }
    assertU(commit());
    assertQ(req("*:*"), "//result[@numFound=1]");
  }

  SolrQueryResponse loadLocal(String filename, String... args) throws Exception {
    LocalSolrQueryRequest req = (LocalSolrQueryRequest) req(args);
    try {
      // TODO: stop using locally defined streams once stream.file and
      // stream.body work everywhere
      List<ContentStream> cs = new ArrayList<ContentStream>();
      cs.add(new ContentStreamBase.FileStream(getFile(filename)));
      req.setContentStreams(cs);
      return h.queryAndResponse("/update/extract", req);
    } finally {
      req.close();
    }
  }


}
