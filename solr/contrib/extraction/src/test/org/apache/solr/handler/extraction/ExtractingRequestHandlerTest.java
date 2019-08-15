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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.BufferingRequestProcessor;
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
    // Is the JDK/env affected by a known bug?
    final String tzDisplayName = TimeZone.getDefault().getDisplayName(false, TimeZone.SHORT, Locale.US);
    if (!tzDisplayName.matches("[A-Za-z]{3,}([+-]\\d\\d(:\\d\\d)?)?")) {
      assertTrue("Is some other JVM affected?  Or bad regex? TzDisplayName: " + tzDisplayName,
          System.getProperty("java.version").startsWith("11"));
      assumeTrue("SOLR-12759 JDK 11 (1st release) and Tika 1.x can result in extracting dates in a bad format.", false);
    }

    initCore("solrconfig.xml", "schema.xml", getFile("extraction/solr").getAbsolutePath());
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
    loadLocal("extraction/solr-word.pdf",
            "fmap.created", "extractedDate",
            "fmap.producer", "extractedProducer",
            "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
            "fmap.Creation-Date", "extractedDate",
            "uprefix", "ignored_",
            "fmap.Author", "extractedAuthor",
            "fmap.content", "extractedContent",
           "literal.id", "one",
            "fmap.Last-Modified", "extractedDate"
    );
    assertQ(req("title:solr-word"), "//*[@numFound='0']");
    assertU(commit());
    assertQ(req("title:solr-word"), "//*[@numFound='1']");


    loadLocal("extraction/simple.html", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
            "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
            "fmap.Author", "extractedAuthor",
            "fmap.language", "extractedLanguage",
            "literal.id", "two",
            "uprefix", "ignored_",
            "fmap.content", "extractedContent",
            "fmap.Last-Modified", "extractedDate"
    );
    assertQ(req("title:Welcome"), "//*[@numFound='0']");
    assertU(commit());
    assertQ(req("title:Welcome"), "//*[@numFound='1']");

    assertQ(req("extractedContent:distinctwords"),      "//*[@numFound='0']");
    assertQ(req("extractedContent:distinct"),           "//*[@numFound='1']");
    assertQ(req("extractedContent:words"),              "//*[@numFound='2']");
    assertQ(req("extractedContent:\"distinct words\""), "//*[@numFound='1']");

    loadLocal("extraction/simple.html",
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
    assertQ(req("+id:simple2 +t_content:serif"), "//*[@numFound='0']"); // make sure <style> content is excluded
    assertQ(req("+id:simple2 +t_content:blur"), "//*[@numFound='0']"); // make sure <script> content is excluded

    // make sure the fact there is an index-time boost does not fail the parsing
    loadLocal("extraction/simple.html",
      "literal.id","simple3",
      "uprefix", "t_",
      "lowernames", "true",
      "captureAttr", "true",  "fmap.a","t_href",
      "commit", "true"

      ,"boost.t_href", "100.0"
    );

    assertQ(req("t_href:http"), "//*[@numFound='2']");
    assertQ(req("t_href:http"), "//doc[2]/str[.='simple3']");
    assertQ(req("+id:simple3 +t_content_type:[* TO *]"), "//*[@numFound='1']");//test lowercase and then uprefix

    loadLocal("extraction/version_control.xml", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
            "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
            "fmap.Author", "extractedAuthor",
            "literal.id", "three",
            "uprefix", "ignored_",
            "fmap.content", "extractedContent",
            "fmap.language", "extractedLanguage",
            "fmap.Last-Modified", "extractedDate"
    );
    assertQ(req("stream_name:version_control.xml"), "//*[@numFound='0']");
    assertU(commit());
    assertQ(req("stream_name:version_control.xml"), "//*[@numFound='1']");

    loadLocal("extraction/word2003.doc", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
            "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
            "fmap.Author", "extractedAuthor",
            "literal.id", "four",
            "uprefix", "ignored_",
            "fmap.content", "extractedContent",
            "fmap.language", "extractedLanguage",
            "fmap.Last-Modified", "extractedDate"
    );
    assertQ(req("title:\"Word 2003 Title\""), "//*[@numFound='0']");
    // There is already a PDF file with this content:
    assertQ(req("extractedContent:\"This is a test of PDF and Word extraction in Solr, it is only a test\""), "//*[@numFound='1']");
    assertU(commit());
    assertQ(req("title:\"Word 2003 Title\""), "//*[@numFound='1']");
    // now 2 of them:
    assertQ(req("extractedContent:\"This is a test of PDF and Word extraction in Solr, it is only a test\""), "//*[@numFound='2']");

    // compressed file
    loadLocal("extraction/tiny.txt.gz", 
              "fmap.created", "extractedDate", 
              "fmap.producer", "extractedProducer",
              "fmap.creator", "extractedCreator", 
              "fmap.Keywords", "extractedKeywords",
              "fmap.Author", "extractedAuthor",
              "uprefix", "ignored_",
              "fmap.content", "extractedContent",
              "fmap.language", "extractedLanguage",
              "fmap.Last-Modified", "extractedDate",
              "literal.id", "tiny.txt.gz");
    assertU(commit());
    assertQ(req("id:tiny.txt.gz")
            , "//*[@numFound='1']"
            , "//*/arr[@name='stream_name']/str[.='tiny.txt.gz']"
            );

    // compressed file
    loadLocal("extraction/open-document.odt", 
              "uprefix", "ignored_",
              "fmap.content", "extractedContent",
              "literal.id", "open-document");
    assertU(commit());
    assertQ(req("extractedContent:\"Práctica sobre GnuPG\"")
            , "//*[@numFound='1']"
            , "//*/arr[@name='stream_name']/str[.='open-document.odt']"
            );
  }

  @Test
  public void testCapture() throws Exception {
    loadLocal("extraction/simple.html",
        "literal.id","capture1",
        "uprefix","t_",
        "capture","div",
        "fmap.div", "foo_t",
        "commit", "true"
    );
    assertQ(req("+id:capture1 +t_content:Solr"), "//*[@numFound='1']");
    assertQ(req("+id:capture1 +foo_t:\"here is some text in a div\""), "//*[@numFound='1']");

    loadLocal("extraction/simple.html",
        "literal.id", "capture2",
        "captureAttr", "true",
        "defaultField", "text",
        "fmap.div", "div_t",
        "fmap.a", "anchor_t",
        "capture", "div",
        "capture", "a",
        "commit", "true"
    );
    assertQ(req("+id:capture2 +text:Solr"), "//*[@numFound='1']");
    assertQ(req("+id:capture2 +div_t:\"here is some text in a div\""), "//*[@numFound='1']");
    assertQ(req("+id:capture2 +anchor_t:http\\://www.apache.org"), "//*[@numFound='1']");
    assertQ(req("+id:capture2 +anchor_t:link"), "//*[@numFound='1']");
  }

  @Test
  public void testDefaultField() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
    assertNotNull("handler is null and it shouldn't be", handler);

    try {
      ignoreException("unknown field 'a'");
      ignoreException("unknown field 'meta'");  // TODO: should this exception be happening?
      expectThrows(SolrException.class, () -> {
        loadLocal("extraction/simple.html",
            "literal.id", "simple2",
            "lowernames", "true",
            "captureAttr", "true",
            //"fmap.content_type", "abcxyz",
            "commit", "true"  // test immediate commit
        );
      });
    } finally {
      resetExceptionIgnores();
    }
    

    loadLocal("extraction/simple.html",
      "literal.id","simple2",
      ExtractingParams.DEFAULT_FIELD, "defaultExtr",//test that unmapped fields go to the text field when no uprefix is specified
      "lowernames", "true",
      "captureAttr", "true",
      //"fmap.content_type", "abcxyz",
      "commit", "true"  // test immediate commit
    );
    assertQ(req("id:simple2"), "//*[@numFound='1']");
    assertQ(req("defaultExtr:http\\:\\/\\/www.apache.org"), "//*[@numFound='1']");

    //Test when both uprefix and default are specified.
    loadLocal("extraction/simple.html",
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
    loadLocal("extraction/version_control.xml", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
            "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
            "fmap.Author", "extractedAuthor",
            "fmap.content", "extractedContent",
            "literal.id", "one",
            "uprefix", "ignored_",
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
      // TODO: original author did not specify why an exception should be thrown... how to fix?
      loadLocal("extraction/version_control.xml", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
          "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
          "fmap.Author", "extractedAuthor",
          "fmap.content", "extractedContent",
          "literal.id", "two",
          "fmap.language", "extractedLanguage",
          "literal.extractionLiteral", "one",
          "literal.extractionLiteral", "two",
          "fmap.X-Parsed-By", "ignored_parser",
          "fmap.Last-Modified", "extractedDate"
      );
      // TODO: original author did not specify why an exception should be thrown... how to fix?
      // assertTrue("Exception should have been thrown", false);
    } catch (SolrException e) {
      //nothing to see here, move along
    }

    loadLocal("extraction/version_control.xml", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
            "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
            "fmap.Author", "extractedAuthor",
            "fmap.content", "extractedContent",
            "literal.id", "three",
            "fmap.language", "extractedLanguage",
            "literal.extractionLiteral", "one",
            "fmap.X-Parsed-By", "ignored_parser",
            "fmap.Last-Modified", "extractedDate"
    );
    assertU(commit());
    assertQ(req("extractionLiteral:one"), "//*[@numFound='1']");

  }

  public void testLiteralDefaults() throws Exception {

    // sanity check config
    loadLocalFromHandler("/update/extract/lit-def",
                         "extraction/simple.html",
                         "literal.id", "lit-def-simple");
    assertU(commit());
    assertQ(req("q", "id:lit-def-simple")
            , "//*[@numFound='1']"
            , "count(//arr[@name='foo_s']/str)=1"
            , "//arr[@name='foo_s']/str[.='x']"
            , "count(//arr[@name='bar_s']/str)=1"
            , "//arr[@name='bar_s']/str[.='y']"
            , "count(//arr[@name='zot_s']/str)=1"
            , "//arr[@name='zot_s']/str[.='z']"
            ); 
    
    // override the default foo_s
    loadLocalFromHandler("/update/extract/lit-def",
                         "extraction/simple.html",
                         "literal.foo_s", "1111",
                         "literal.id", "lit-def-simple");
    assertU(commit());
    assertQ(req("q", "id:lit-def-simple")
            , "//*[@numFound='1']"
            , "count(//arr[@name='foo_s']/str)=1"
            , "//arr[@name='foo_s']/str[.='1111']"
            , "count(//arr[@name='bar_s']/str)=1"
            , "//arr[@name='bar_s']/str[.='y']"
            , "count(//arr[@name='zot_s']/str)=1"
            , "//arr[@name='zot_s']/str[.='z']"
            ); 

    // pre-pend the bar_s
    loadLocalFromHandler("/update/extract/lit-def",
                         "extraction/simple.html",
                         "literal.bar_s", "2222",
                         "literal.id", "lit-def-simple");
    assertU(commit());
    assertQ(req("q", "id:lit-def-simple")
            , "//*[@numFound='1']"
            , "count(//arr[@name='foo_s']/str)=1"
            , "//arr[@name='foo_s']/str[.='x']"
            , "count(//arr[@name='bar_s']/str)=2"
            , "//arr[@name='bar_s']/str[.='2222']"
            , "//arr[@name='bar_s']/str[.='y']"
            , "count(//arr[@name='zot_s']/str)=1"
            , "//arr[@name='zot_s']/str[.='z']"
            ); 

    // invariant zot_s can not be changed
    loadLocalFromHandler("/update/extract/lit-def",
                         "extraction/simple.html",
                         "literal.zot_s", "3333",
                         "literal.id", "lit-def-simple");
    assertU(commit());
    assertQ(req("q", "id:lit-def-simple")
            , "//*[@numFound='1']"
            , "count(//arr[@name='foo_s']/str)=1"
            , "//arr[@name='foo_s']/str[.='x']"
            , "count(//arr[@name='bar_s']/str)=1"
            , "//arr[@name='bar_s']/str[.='y']"
            , "count(//arr[@name='zot_s']/str)=1"
            , "//arr[@name='zot_s']/str[.='z']"
            ); 
    
  }

  @Test
  public void testPlainTextSpecifyingMimeType() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);

    // Load plain text specifying MIME type:
    loadLocal("extraction/version_control.txt", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
            "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
            "fmap.Author", "extractedAuthor",
            "literal.id", "one",
            "fmap.language", "extractedLanguage",
            "fmap.X-Parsed-By", "ignored_parser",
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
    loadLocal("extraction/version_control.txt", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
            "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
            "fmap.Author", "extractedAuthor",
            "literal.id", "one",
            "fmap.language", "extractedLanguage",
            "fmap.X-Parsed-By", "ignored_parser",
            "fmap.content", "extractedContent",
            ExtractingParams.RESOURCE_NAME, "extraction/version_control.txt"
    );
    assertQ(req("extractedContent:Apache"), "//*[@numFound='0']");
    assertU(commit());
    assertQ(req("extractedContent:Apache"), "//*[@numFound='1']");
  }

  @Test
  public void testCommitWithin() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);
    
    SolrQueryRequest req = req("literal.id", "one",
                               ExtractingParams.RESOURCE_NAME, "extraction/version_control.txt",
                               "commitWithin", "200"
                               );
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);

    ExtractingDocumentLoader loader = (ExtractingDocumentLoader) handler.newLoader(req, p);
    loader.load(req, rsp, new ContentStreamBase.FileStream(getFile("extraction/version_control.txt")),p);

    AddUpdateCommand add = p.addCommands.get(0);
    assertEquals(200, add.commitWithin);

    req.close();
  }

  // Note: If you load a plain text file specifying neither MIME type nor filename, extraction will silently fail. This is because Tika's
  // automatic MIME type detection will fail, and it will default to using an empty-string-returning default parser

  @SuppressWarnings("rawtypes")
  @Test
  public void testExtractOnly() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);
    SolrQueryResponse rsp = loadLocal("extraction/solr-word.pdf", ExtractingParams.EXTRACT_ONLY, "true");
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

    rsp = loadLocal("extraction/solr-word.pdf", ExtractingParams.EXTRACT_ONLY, "true",
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

  @SuppressWarnings("rawtypes")
  @Test
  public void testXPath() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);
    SolrQueryResponse rsp = loadLocal("extraction/example.html",
            ExtractingParams.XPATH_EXPRESSION, "/xhtml:html/xhtml:body/xhtml:a/descendant::node()",
            ExtractingParams.EXTRACT_ONLY, "true"
    );
    assertTrue("rsp is null and it shouldn't be", rsp != null);
    NamedList list = rsp.getValues();
    String val = (String) list.get("example.html");
    assertEquals("News", val.trim()); //there is only one matching <a> tag

    loadLocal("extraction/example.html",
        "literal.id", "example1",
        "captureAttr", "true",
        "defaultField", "text",
        "capture", "div",
        "fmap.div", "foo_t",
        "boost.foo_t", "3",
        "xpath", "/xhtml:html/xhtml:body/xhtml:div//node()",
        "commit", "true"
    );
    assertQ(req("+id:example1 +foo_t:\"here is some text in a div\""), "//*[@numFound='1']");
  }

  /** test arabic PDF extraction is functional */
  @Test
  public void testArabicPDF() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) 
      h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);

    loadLocal("extraction/arabic.pdf", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
        "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
        "fmap.Creation-Date", "extractedDate",
        "fmap.Author", "extractedAuthor",
        "uprefix", "ignored_",
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

    expectThrows(Exception.class, () -> {
      loadLocal("extraction/password-is-solrcell.docx", "literal.id", "one");
    });
    assertU(commit());
    assertQ(req("*:*"), "//result[@numFound=0]");

    try{
      loadLocal("extraction/password-is-solrcell.docx", "fmap.created", "extractedDate", "fmap.producer", "extractedProducer",
          "fmap.creator", "extractedCreator", "fmap.Keywords", "extractedKeywords",
          "fmap.Creation-Date", "extractedDate",
          "uprefix", "ignored_",
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
  
  @Test
  public void testWrongStreamType() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);

    expectThrows(Exception.class, () -> {
      // Load plain text specifying another mime type, should fail
      loadLocal("extraction/version_control.txt",
          "literal.id", "one",
          ExtractingParams.STREAM_TYPE, "application/pdf"
      );
    });

    expectThrows(Exception.class, () -> {
      // Load plain text specifying non existing mimetype, should fail
      loadLocal("extraction/version_control.txt",
          "literal.id", "one",
          ExtractingParams.STREAM_TYPE, "foo/bar"
      );
    });
  }

  public void testLiteralsOverride() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);
 
    assertQ(req("*:*"), "//*[@numFound='0']");

    // Here Tika should parse out a title for this document:
    loadLocal("extraction/solr-word.pdf", 
            "fmap.created", "extractedDate", 
            "fmap.producer", "extractedProducer",
            "fmap.creator", "extractedCreator", 
            "fmap.Keywords", "extractedKeywords",
            "fmap.Author", "extractedAuthor",
            "literal.id", "three",
            "fmap.content", "extractedContent",
            "fmap.language", "extractedLanguage",
            "fmap.Creation-Date", "extractedDate",
            "uprefix", "ignored_",
            "fmap.Last-Modified", "extractedDate");

    // Here the literal value should override the Tika-parsed title:
    loadLocal("extraction/solr-word.pdf",
            "literal.title", "wolf-man",
            "fmap.created", "extractedDate",
            "fmap.producer", "extractedProducer",
            "fmap.creator", "extractedCreator",
            "fmap.Keywords", "extractedKeywords",
            "fmap.Author", "extractedAuthor",
            "literal.id", "four",
            "fmap.content", "extractedContent",
            "fmap.language", "extractedLanguage",
            "fmap.Creation-Date", "extractedDate",
            "uprefix", "ignored_",
            "fmap.Last-Modified", "extractedDate");

    // Here we mimic the old behaviour where literals are added, not overridden
    loadLocal("extraction/solr-word.pdf",
            "literalsOverride", "false",
            // Trick - we first map the metadata-title to an ignored field before we replace with literal title
            "fmap.title", "ignored_a",
            "literal.title", "old-behaviour",
            "literal.extractedKeywords", "literalkeyword",
            "fmap.created", "extractedDate",
            "fmap.producer", "extractedProducer",
            "fmap.creator", "extractedCreator",
            "fmap.Keywords", "extractedKeywords",
            "fmap.Author", "extractedAuthor",
            "literal.id", "five",
            "fmap.content", "extractedContent",
            "fmap.language", "extractedLanguage",
            "fmap.Creation-Date", "extractedDate",
            "uprefix", "ignored_",
            "fmap.Last-Modified", "extractedDate");

    assertU(commit());

    assertQ(req("title:solr-word"), "//*[@numFound='1']");
    assertQ(req("title:wolf-man"), "//*[@numFound='1']");
    assertQ(req("extractedKeywords:(solr AND word AND pdf AND literalkeyword)"), "//*[@numFound='1']");
  }

  @Test
  public void testPdfWithImages() throws Exception {
    //Tests possibility to configure ParseContext (by example to extract embedded images from pdf)
    loadLocal("extraction/pdf-with-image.pdf",
        "fmap.created", "extractedDate",
        "fmap.producer", "extractedProducer",
        "fmap.creator", "extractedCreator",
        "fmap.Keywords", "extractedKeywords",
        "fmap.Creation-Date", "extractedDate",
        "uprefix", "ignored_",
        "fmap.Author", "extractedAuthor",
        "fmap.content", "wdf_nocase",
        "literal.id", "pdfWithImage",
        "resource.name", "pdf-with-image.pdf",
        "resource.password", "solrRules",
        "fmap.Last-Modified", "extractedDate");

    assertQ(req("wdf_nocase:\"embedded:image0.jpg\""), "//*[@numFound='0']");
    assertU(commit());
    assertQ(req("wdf_nocase:\"embedded:image0.jpg\""), "//*[@numFound='1']");
  }

  @Test
  public void testPasswordProtected() throws Exception {
    // PDF, Passwords from resource.password
    loadLocal("extraction/encrypted-password-is-solrRules.pdf",
        "fmap.created", "extractedDate", 
        "fmap.producer", "extractedProducer",
        "fmap.creator", "extractedCreator", 
        "fmap.Keywords", "extractedKeywords",
        "fmap.Creation-Date", "extractedDate",
        "uprefix", "ignored_",
        "fmap.Author", "extractedAuthor",
        "fmap.content", "wdf_nocase",
        "literal.id", "pdfpwliteral",
        "resource.name", "encrypted-password-is-solrRules.pdf",
        "resource.password", "solrRules",
        "fmap.Last-Modified", "extractedDate");

    // PDF, Passwords from passwords property file
    loadLocal("extraction/encrypted-password-is-solrRules.pdf",
        "fmap.created", "extractedDate", 
        "fmap.producer", "extractedProducer",
        "fmap.creator", "extractedCreator", 
        "fmap.Keywords", "extractedKeywords",
        "fmap.Creation-Date", "extractedDate",
        "uprefix", "ignored_",
        "fmap.Author", "extractedAuthor",
        "fmap.content", "wdf_nocase",
        "literal.id", "pdfpwfile",
        "resource.name", "encrypted-password-is-solrRules.pdf",
        "passwordsFile", "passwordRegex.properties", // Passwords-file
        "fmap.Last-Modified", "extractedDate");

    // DOCX, Explicit password
    loadLocal("extraction/password-is-Word2010.docx", 
        "fmap.created", "extractedDate", 
        "fmap.producer", "extractedProducer",
        "fmap.creator", "extractedCreator", 
        "fmap.Keywords", "extractedKeywords",
        "fmap.Creation-Date", "extractedDate",
        "fmap.Author", "extractedAuthor",
        "fmap.content", "wdf_nocase",
        "uprefix", "ignored_",
        "literal.id", "docxpwliteral",
        "resource.name", "password-is-Word2010.docx",
        "resource.password", "Word2010", // Explicit password
        "fmap.Last-Modified", "extractedDate");

    // DOCX, Passwords from file
    loadLocal("extraction/password-is-Word2010.docx", 
        "fmap.created", "extractedDate",
        "fmap.producer", "extractedProducer",
        "fmap.creator", "extractedCreator", 
        "fmap.Keywords", "extractedKeywords",
        "fmap.Creation-Date", "extractedDate",
        "uprefix", "ignored_",
        "fmap.Author", "extractedAuthor",
        "fmap.content", "wdf_nocase",
        "literal.id", "docxpwfile",
        "resource.name", "password-is-Word2010.docx",
        "passwordsFile", "passwordRegex.properties", // Passwords-file
        "fmap.Last-Modified", "extractedDate");
    
    assertU(commit());
    Thread.sleep(100);
    assertQ(req("wdf_nocase:\"This is a test of PDF\""), "//*[@numFound='2']");
    assertQ(req("wdf_nocase:\"Test password protected word doc\""), "//*[@numFound='2']");
  }
  
  SolrQueryResponse loadLocalFromHandler(String handler, String filename, 
                                         String... args) throws Exception {
                              
    LocalSolrQueryRequest req = (LocalSolrQueryRequest) req(args);
    try {
      // TODO: stop using locally defined streams once stream.file and
      // stream.body work everywhere
      List<ContentStream> cs = new ArrayList<>();
      cs.add(new ContentStreamBase.FileStream(getFile(filename)));
      req.setContentStreams(cs);
      return h.queryAndResponse(handler, req);
    } finally {
      req.close();
    }
  }

  SolrQueryResponse loadLocal(String filename, String... args) throws Exception {
    return loadLocalFromHandler("/update/extract", filename, args);
  }


}
