package org.apache.solr.handler;

import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.extraction.ExtractingParams;
import org.apache.solr.handler.extraction.ExtractingRequestHandler;

import java.util.List;
import java.util.ArrayList;
import java.io.File;


/**
 *
 *
 **/
public class ExtractingRequestHandlerTest extends AbstractSolrTestCase {
  @Override public String getSchemaFile() { return "schema.xml"; }
  @Override public String getSolrConfigFile() { return "solrconfig.xml"; }


  public void testExtraction() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);
    loadLocal("solr-word.pdf", "ext.map.created", "extractedDate", "ext.map.producer", "extractedProducer",
            "ext.map.creator", "extractedCreator", "ext.map.Keywords", "extractedKeywords",
            "ext.map.Author", "extractedAuthor",
            "ext.def.fl", "extractedContent",
            "ext.map.Last-Modified", "extractedDate"
    );
    assertQ(req("title:solr-word"),"//*[@numFound='0']");
    assertU(commit());
    assertQ(req("title:solr-word"),"//*[@numFound='1']");

    loadLocal("simple.html", "ext.map.created", "extractedDate", "ext.map.producer", "extractedProducer",
            "ext.map.creator", "extractedCreator", "ext.map.Keywords", "extractedKeywords",
            "ext.map.Author", "extractedAuthor",
            "ext.map.language", "extractedLanguage",
            "ext.def.fl", "extractedContent",
            "ext.map.Last-Modified", "extractedDate"
    );
    assertQ(req("title:Welcome"),"//*[@numFound='0']");
    assertU(commit());
    assertQ(req("title:Welcome"),"//*[@numFound='1']");

    loadLocal("version_control.xml", "ext.map.created", "extractedDate", "ext.map.producer", "extractedProducer",
            "ext.map.creator", "extractedCreator", "ext.map.Keywords", "extractedKeywords",
            "ext.map.Author", "extractedAuthor",
            "ext.def.fl", "extractedContent",
            "ext.map.Last-Modified", "extractedDate"
    );
    assertQ(req("stream_name:version_control.xml"),"//*[@numFound='0']");
    assertU(commit());
    assertQ(req("stream_name:version_control.xml"),"//*[@numFound='1']");
  }


  

  public void testPlainTextSpecifyingMimeType() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);

    // Load plain text specifying MIME type:
    loadLocal("version_control.txt", "ext.map.created", "extractedDate", "ext.map.producer", "extractedProducer",
            "ext.map.creator", "extractedCreator", "ext.map.Keywords", "extractedKeywords",
            "ext.map.Author", "extractedAuthor",
            "ext.map.language", "extractedLanguage",
            "ext.def.fl", "extractedContent",
	    ExtractingParams.STREAM_TYPE, "text/plain"
    );
    assertQ(req("extractedContent:Apache"),"//*[@numFound='0']");
    assertU(commit());
    assertQ(req("extractedContent:Apache"),"//*[@numFound='1']");
  }

  public void testPlainTextSpecifyingResourceName() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);

    // Load plain text specifying filename
    loadLocal("version_control.txt", "ext.map.created", "extractedDate", "ext.map.producer", "extractedProducer",
            "ext.map.creator", "extractedCreator", "ext.map.Keywords", "extractedKeywords",
            "ext.map.Author", "extractedAuthor",
            "ext.map.language", "extractedLanguage",
            "ext.def.fl", "extractedContent",
	    ExtractingParams.RESOURCE_NAME, "version_control.txt"
    );
    assertQ(req("extractedContent:Apache"),"//*[@numFound='0']");
    assertU(commit());
    assertQ(req("extractedContent:Apache"),"//*[@numFound='1']");
  }

  // Note: If you load a plain text file specifying neither MIME type nor filename, extraction will silently fail. This is because Tika's
  // automatic MIME type detection will fail, and it will default to using an empty-string-returning default parser


  public void testExtractOnly() throws Exception {
    ExtractingRequestHandler handler = (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
    assertTrue("handler is null and it shouldn't be", handler != null);
    SolrQueryResponse rsp = loadLocal("solr-word.pdf", ExtractingParams.EXTRACT_ONLY, "true");
    assertTrue("rsp is null and it shouldn't be", rsp != null);
    NamedList list = rsp.getValues();

    String extraction = (String) list.get("solr-word.pdf");
    assertTrue("extraction is null and it shouldn't be", extraction != null);
    assertTrue(extraction + " does not contain " + "solr-word", extraction.indexOf("solr-word") != -1);

  }

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


  SolrQueryResponse loadLocal(String filename, String... args) throws Exception {
    LocalSolrQueryRequest req =  (LocalSolrQueryRequest)req(args);

    // TODO: stop using locally defined streams once stream.file and
    // stream.body work everywhere
    List<ContentStream> cs = new ArrayList<ContentStream>();
    cs.add(new ContentStreamBase.FileStream(new File(filename)));
    req.setContentStreams(cs);
    return h.queryAndResponse("/update/extract", req);
  }


}
