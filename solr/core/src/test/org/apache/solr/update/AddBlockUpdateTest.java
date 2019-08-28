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
package org.apache.solr.update;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.handler.loader.XMLLoader;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.RefCounted;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class AddBlockUpdateTest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String child = "child_s";
  private static final String parent = "parent_s";
  private static final String type = "type_s";

  private final static AtomicInteger counter = new AtomicInteger();
  private static ExecutorService exe;
  private static boolean cachedMode;

  private static XMLInputFactory inputFactory;

  private RefCounted<SolrIndexSearcher> searcherRef;
  private SolrIndexSearcher _searcher;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws Exception {
    String oldCacheNamePropValue = System
        .getProperty("blockJoinParentFilterCache");
    System.setProperty("blockJoinParentFilterCache", (cachedMode = random()
        .nextBoolean()) ? "blockJoinParentFilterCache" : "don't cache");
    if (oldCacheNamePropValue != null) {
      System.setProperty("blockJoinParentFilterCache", oldCacheNamePropValue);
    }
    inputFactory = XMLInputFactory.newInstance();

    exe = // Executors.newSingleThreadExecutor();
    rarely() ? ExecutorUtil.newMDCAwareFixedThreadPool(atLeast(2), new DefaultSolrThreadFactory("AddBlockUpdateTest")) : ExecutorUtil
        .newMDCAwareCachedThreadPool(new DefaultSolrThreadFactory("AddBlockUpdateTest"));

    counter.set(0);
    initCore("solrconfig.xml", "schema15.xml");
  }

  @Before
  public void prepare() {
    // assertU("<rollback/>");
    assertU(delQ("*:*"));
    assertU(commit("expungeDeletes", "true"));
  }

  private Document getDocument() throws ParserConfigurationException {
    DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
    javax.xml.parsers.DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
    return docBuilder.newDocument();
  }

  private SolrIndexSearcher getSearcher() {
    if (_searcher == null) {
      searcherRef = h.getCore().getSearcher();
      _searcher = searcherRef.get();
    }
    return _searcher;
  }

  @After
  public void cleanup() {
    if (searcherRef != null || _searcher != null) {
      searcherRef.decref();
      searcherRef = null;
      _searcher = null;
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (null != exe) {
      exe.shutdownNow();
      exe = null;
    }
    inputFactory = null;
  }

  @Test
  public void testOverwrite() throws IOException{
    assertU(add(
      nest(doc("id","X", parent, "X"),
             doc(child,"a", "id", "66"),
             doc(child,"b", "id", "66"))));
    assertU(add(
      nest(doc("id","Y", parent, "Y"),
             doc(child,"a", "id", "66"),
             doc(child,"b", "id", "66"))));
    String overwritten = random().nextBoolean() ? "X": "Y";
    String dubbed = overwritten.equals("X") ? "Y":"X";

    assertU(add(
        nest(doc("id",overwritten, parent, overwritten),
               doc(child,"c","id", "66"),
               doc(child,"d","id", "66")), "overwrite", "true"));
    assertU(add(
        nest(doc("id",dubbed, parent, dubbed),
               doc(child,"c","id", "66"),
               doc(child,"d","id", "66")), "overwrite", "false"));

    assertU(commit());

    assertQ(req(parent+":"+overwritten, "//*[@numFound='1']"));
    assertQ(req(parent+":"+dubbed, "//*[@numFound='2']"));

    final SolrIndexSearcher searcher = getSearcher();
    assertSingleParentOf(searcher, one("ab"), dubbed);

    final TopDocs docs = searcher.search(join(one("cd")), 10);
    assertEquals(2, docs.totalHits.value);
    final String pAct = searcher.doc(docs.scoreDocs[0].doc).get(parent)+
                        searcher.doc(docs.scoreDocs[1].doc).get(parent);
    assertTrue(pAct.contains(dubbed) && pAct.contains(overwritten) && pAct.length()==2);

    assertQ(req("id:66", "//*[@numFound='6']"));
    assertQ(req(child+":(a b)", "//*[@numFound='2']"));
    assertQ(req(child+":(c d)", "//*[@numFound='4']"));
  }

  private static XmlDoc nest(XmlDoc parent, XmlDoc ... children){
    XmlDoc xmlDoc = new XmlDoc();
    xmlDoc.xml = parent.xml.replace("</doc>",
        Arrays.toString(children).replaceAll("[\\[\\]]", "")+"</doc>");
    return xmlDoc;
  }

  @Test
  public void testBasics() throws Exception {
    List<Document> blocks = new ArrayList<>(Arrays.asList(
        block("abcD"),
        block("efgH"),
        merge(block("ijkL"), block("mnoP")),
        merge(block("qrsT"), block("uvwX")),
        block("Y"),
        block("Z")));

    Collections.shuffle(blocks, random());

    log.trace("{}", blocks);

    for (Future<Void> f : exe.invokeAll(callables(blocks))) {
      f.get(); // exceptions?
    }

    assertU(commit());

    final SolrIndexSearcher searcher = getSearcher();
    // final String resp = h.query(req("q","*:*", "sort","_docid_ asc", "rows",
    // "10000"));
    // log.trace(resp);
    int parentsNum = "DHLPTXYZ".length();
    assertQ(req(parent + ":[* TO *]"), "//*[@numFound='" + parentsNum + "']");
    assertQ(req(child + ":[* TO *]"), "//*[@numFound='"
        + (('z' - 'a' + 1) - parentsNum) + "']");
    assertQ(req("*:*"), "//*[@numFound='" + ('z' - 'a' + 1) + "']");
    assertSingleParentOf(searcher, one("abc"), "D");
    assertSingleParentOf(searcher, one("efg"), "H");
    assertSingleParentOf(searcher, one("ijk"), "L");
    assertSingleParentOf(searcher, one("mno"), "P");
    assertSingleParentOf(searcher, one("qrs"), "T");
    assertSingleParentOf(searcher, one("uvw"), "X");

    assertQ(req("q",child+":(a b c)", "sort","_docid_ asc"),
        "//*[@numFound='3']", // assert physical order of children
      "//doc[1]/arr[@name='child_s']/str[text()='a']",
      "//doc[2]/arr[@name='child_s']/str[text()='b']",
      "//doc[3]/arr[@name='child_s']/str[text()='c']");
  }

  @Test
  public void testExceptionThrown() throws Exception {
    final String abcD = getStringFromDocument(block("abcD"));
    log.info(abcD);
    assertBlockU(abcD);

    Document docToFail = getDocument();
    Element root = docToFail.createElement("add");
    docToFail.appendChild(root);
    Element doc1 = docToFail.createElement("doc");
    root.appendChild(doc1);
    attachField(docToFail, doc1, "id", id());
    attachField(docToFail, doc1, parent, "Y");
    attachField(docToFail, doc1, "sample_i", "notanumber/ignore_exception");
    Element subDoc1 = docToFail.createElement("doc");
    doc1.appendChild(subDoc1);
    attachField(docToFail, subDoc1, "id", id());
    attachField(docToFail, subDoc1, child, "x");
    Element doc2 = docToFail.createElement("doc");
    root.appendChild(doc2);
    attachField(docToFail, doc2, "id", id());
    attachField(docToFail, doc2, parent, "W");

    assertFailedBlockU(getStringFromDocument(docToFail));

    assertBlockU(getStringFromDocument(block("efgH")));
    assertBlockU(commit());

    final SolrIndexSearcher searcher = getSearcher();
    assertQ(req("q","*:*","indent","true", "fl","id,parent_s,child_s"), "//*[@numFound='" + "abcDefgH".length() + "']");
    assertSingleParentOf(searcher, one("abc"), "D");
    assertSingleParentOf(searcher, one("efg"), "H");

    assertQ(req(child + ":x"), "//*[@numFound='0']");
    assertQ(req(parent + ":Y"), "//*[@numFound='0']");
    assertQ(req(parent + ":W"), "//*[@numFound='0']");
  }

  @Test
  public void testExceptionThrownChildDocWAnonymousChildren() throws Exception {
    SolrInputDocument document1 = sdoc("id", id(), parent, "X",
        "child1_s", sdoc("id", id(), "child_s", "y"),
        "child2_s", sdoc("id", id(), "child_s", "z"));

    SolrInputDocument exceptionChildDoc = (SolrInputDocument) document1.get("child1_s").getValue();
    addChildren("child", exceptionChildDoc, 0, false);

    thrown.expect(SolrException.class);
    final String expectedMessage = "Anonymous child docs can only hang from others or the root";
    thrown.expectMessage(expectedMessage);
    indexSolrInputDocumentsDirectly(document1);
  }

  @Test
  public void testSolrNestedFieldsList() throws Exception {
    final String id1 = id();
    List<SolrInputDocument> children1 = Arrays.asList(sdoc("id", id(), child, "y"), sdoc("id", id(), child, "z"));

    SolrInputDocument document1 = sdoc("id", id1, parent, "X",
        "children", children1);

    final String id2 = id();
    List<SolrInputDocument> children2 = Arrays.asList(sdoc("id", id(), child, "b"), sdoc("id", id(), child, "c"));

    SolrInputDocument document2 = sdoc("id", id2, parent, "A",
        "children", children2);

    indexSolrInputDocumentsDirectly(document1, document2);

    final SolrIndexSearcher searcher = getSearcher();
    assertJQ(req("q","*:*",
        "fl","*",
        "sort","id asc",
        "wt","json"),
        "/response/numFound==" + "XyzAbc".length());
    assertJQ(req("q",parent+":" + document2.getFieldValue(parent),
        "fl","*",
        "sort","id asc",
        "wt","json"),
        "/response/docs/[0]/id=='" + document2.getFieldValue("id") + "'");
    assertQ(req("q",child+":(y z b c)", "sort","_docid_ asc"),
        "//*[@numFound='" + "yzbc".length() + "']", // assert physical order of children
        "//doc[1]/arr[@name='child_s']/str[text()='y']",
        "//doc[2]/arr[@name='child_s']/str[text()='z']",
        "//doc[3]/arr[@name='child_s']/str[text()='b']",
        "//doc[4]/arr[@name='child_s']/str[text()='c']");
    assertSingleParentOf(searcher, one("bc"), "A");
    assertSingleParentOf(searcher, one("yz"), "X");
  }

  @Test
  public void testSolrNestedFieldsSingleVal() throws Exception {
    SolrInputDocument document1 = sdoc("id", id(), parent, "X",
        "child1_s", sdoc("id", id(), "child_s", "y"),
        "child2_s", sdoc("id", id(), "child_s", "z"));

    SolrInputDocument document2 = sdoc("id", id(), parent, "A",
        "child1_s", sdoc("id", id(), "child_s", "b"),
        "child2_s", sdoc("id", id(), "child_s", "c"));

    indexSolrInputDocumentsDirectly(document1, document2);

    final SolrIndexSearcher searcher = getSearcher();
    assertJQ(req("q","*:*",
        "fl","*",
        "sort","id asc",
        "wt","json"),
        "/response/numFound==" + "XyzAbc".length());
    assertJQ(req("q",parent+":" + document2.getFieldValue(parent),
        "fl","*",
        "sort","id asc",
        "wt","json"),
        "/response/docs/[0]/id=='" + document2.getFieldValue("id") + "'");
    assertQ(req("q",child+":(y z b c)", "sort","_docid_ asc"),
        "//*[@numFound='" + "yzbc".length() + "']", // assert physical order of children
        "//doc[1]/arr[@name='child_s']/str[text()='y']",
        "//doc[2]/arr[@name='child_s']/str[text()='z']",
        "//doc[3]/arr[@name='child_s']/str[text()='b']",
        "//doc[4]/arr[@name='child_s']/str[text()='c']");
    assertSingleParentOf(searcher, one("bc"), "A");
    assertSingleParentOf(searcher, one("yz"), "X");
  }

  @SuppressWarnings("serial")
  @Test
  public void testSolrJXML() throws Exception {
    UpdateRequest req = new UpdateRequest();

    List<SolrInputDocument> docs = new ArrayList<>();

    SolrInputDocument document1 = new SolrInputDocument() {
      {
        final String id = id();
        addField("id", id);
        addField("parent_s", "X");

        ArrayList<SolrInputDocument> ch1 = new ArrayList<>(
            Arrays.asList(new SolrInputDocument() {
              {
                addField("id", id());
                addField("child_s", "y");
              }
            }, new SolrInputDocument() {
              {
                addField("id", id());
                addField("child_s", "z");
              }
            }));

        Collections.shuffle(ch1, random());
        addChildDocuments(ch1);
      }
    };

    SolrInputDocument document2 = new SolrInputDocument() {
      {
        final String id = id();
        addField("id", id);
        addField("parent_s", "A");
        addChildDocument(new SolrInputDocument() {
          {
            addField("id", id());
            addField("child_s", "b");
          }
        });
        addChildDocument(new SolrInputDocument() {
          {
            addField("id", id());
            addField("child_s", "c");
          }
        });
      }
    };

    docs.add(document1);
    docs.add(document2);

    Collections.shuffle(docs, random());
    req.add(docs);

    RequestWriter requestWriter = new RequestWriter();
    OutputStream os = new ByteArrayOutputStream();
    requestWriter.write(req, os);
    assertBlockU(os.toString());
    assertU(commit());

    assertJQ(req("q","*:*",
        "fl","*",
        "sort","id asc",
        "wt","json"),
        "/response/numFound==" + 6);
    final SolrIndexSearcher searcher = getSearcher();
    assertSingleParentOf(searcher, one("yz"), "X");
    assertSingleParentOf(searcher, one("bc"), "A");
  }
  //This is the same as testSolrJXML above but uses the XMLLoader
  // to illustrate the structure of the XML documents
  @Test
  public void testXML() throws IOException, XMLStreamException {
    UpdateRequest req = new UpdateRequest();

    List<SolrInputDocument> docs = new ArrayList<>();

    String xml_doc1 =
    "<doc >" +
      "  <field name=\"id\">1</field>" +
      "  <field name=\"parent_s\">X</field>" +
         "<doc>  " +
         "  <field name=\"id\" >2</field>" +
         "  <field name=\"child_s\">y</field>" +
         "</doc>"+
         "<doc>  " +
         "  <field name=\"id\" >3</field>" +
         "  <field name=\"child_s\">z</field>" +
         "</doc>"+
    "</doc>";

    String xml_doc2 =
        "<doc >" +
          "  <field name=\"id\">4</field>" +
          "  <field name=\"parent_s\">A</field>" +
             "<doc>  " +
             "  <field name=\"id\" >5</field>" +
             "  <field name=\"child_s\">b</field>" +
             "</doc>"+
             "<doc>  " +
             "  <field name=\"id\" >6</field>" +
             "  <field name=\"child_s\">c</field>" +
             "</doc>"+
        "</doc>";


    XMLStreamReader parser =
      inputFactory.createXMLStreamReader( new StringReader( xml_doc1 ) );
    parser.next(); // read the START document...
    //null for the processor is all right here
    XMLLoader loader = new XMLLoader();
    SolrInputDocument document1 = loader.readDoc( parser );

    XMLStreamReader parser2 =
        inputFactory.createXMLStreamReader( new StringReader( xml_doc2 ) );
      parser2.next(); // read the START document...
      //null for the processor is all right here
      //XMLLoader loader = new XMLLoader();
      SolrInputDocument document2 = loader.readDoc( parser2 );

    docs.add(document1);
    docs.add(document2);

    Collections.shuffle(docs, random());
    req.add(docs);

    RequestWriter requestWriter = new RequestWriter();
    OutputStream os = new ByteArrayOutputStream();
    requestWriter.write(req, os);
    assertBlockU(os.toString());
    assertU(commit());

    final SolrIndexSearcher searcher = getSearcher();
    assertSingleParentOf(searcher, one("yz"), "X");
    assertSingleParentOf(searcher, one("bc"), "A");
  }

  @Test
  public void testXMLMultiLevelLabeledChildren() throws XMLStreamException {
    String xml_doc1 =
        "<doc >" +
            "  <field name=\"id\">1</field>" +
            "  <field name=\"empty_s\"></field>" +
            "  <field name=\"parent_s\">X</field>" +
            "  <field name=\"test\">" +
            "    <doc>  " +
            "      <field name=\"id\" >2</field>" +
            "      <field name=\"child_s\">y</field>" +
            "    </doc>" +
            "    <doc>  " +
            "      <field name=\"id\" >3</field>" +
            "      <field name=\"child_s\">z</field>" +
            "    </doc>" +
            "  </field> " +
            "</doc>";

    String xml_doc2 =
        "<doc >" +
            "  <field name=\"id\">4</field>" +
            "  <field name=\"parent_s\">A</field>" +
            "  <field name=\"test\">" +
            "    <doc>  " +
            "      <field name=\"id\" >5</field>" +
            "      <field name=\"child_s\">b</field>" +
            "      <field name=\"grandChild\">" +
            "        <doc>  " +
            "          <field name=\"id\" >7</field>" +
            "          <field name=\"child_s\">d</field>" +
            "        </doc>" +
            "      </field>" +
            "    </doc>" +
            "  </field>" +
            "  <field name=\"test\">" +
            "    <doc>  " +
            "      <field name=\"id\" >6</field>" +
            "      <field name=\"child_s\">c</field>" +
            "    </doc>" +
            "  </field> " +
            "</doc>";

    XMLStreamReader parser =
        inputFactory.createXMLStreamReader(new StringReader(xml_doc1));
    parser.next(); // read the START document...
    //null for the processor is all right here
    XMLLoader loader = new XMLLoader();
    SolrInputDocument document1 = loader.readDoc(parser);

    XMLStreamReader parser2 =
        inputFactory.createXMLStreamReader(new StringReader(xml_doc2));
    parser2.next(); // read the START document...
    //null for the processor is all right here
    //XMLLoader loader = new XMLLoader();
    SolrInputDocument document2 = loader.readDoc(parser2);

    assertFalse(document1.hasChildDocuments());
    assertEquals(document1.toString(), sdoc("id", "1", "empty_s", "", "parent_s", "X", "test",
        sdocs(sdoc("id", "2", "child_s", "y"), sdoc("id", "3", "child_s", "z"))).toString());

    assertFalse(document2.hasChildDocuments());
    assertEquals(document2.toString(), sdoc("id", "4", "parent_s", "A", "test",
        sdocs(sdoc("id", "5", "child_s", "b", "grandChild", Collections.singleton(sdoc("id", "7", "child_s", "d"))),
            sdoc("id", "6", "child_s", "c"))).toString());
  }

  @Test
  public void testXMLLabeledChildren() throws IOException, XMLStreamException {
    UpdateRequest req = new UpdateRequest();

    List<SolrInputDocument> docs = new ArrayList<>();

    String xml_doc1 =
        "<doc >" +
            "  <field name=\"id\">1</field>" +
            "  <field name=\"empty_s\"></field>" +
            "  <field name=\"parent_s\">X</field>" +
            "  <field name=\"test\">" +
            "    <doc>  " +
            "      <field name=\"id\" >2</field>" +
            "      <field name=\"child_s\">y</field>" +
            "    </doc>"+
            "    <doc>  " +
            "      <field name=\"id\" >3</field>" +
            "      <field name=\"child_s\">z</field>" +
            "    </doc>" +
            "  </field> " +
            "</doc>";

    String xml_doc2 =
        "<doc >" +
            "  <field name=\"id\">4</field>" +
            "  <field name=\"parent_s\">A</field>" +
            "  <field name=\"test\">" +
            "    <doc>  " +
            "      <field name=\"id\" >5</field>" +
            "      <field name=\"child_s\">b</field>" +
            "    </doc>"+
            "  </field>" +
            "  <field name=\"test\">" +
            "    <doc>  " +
            "      <field name=\"id\" >6</field>" +
            "      <field name=\"child_s\">c</field>" +
            "    </doc>" +
            "  </field> " +
            "</doc>";

    XMLStreamReader parser =
        inputFactory.createXMLStreamReader( new StringReader( xml_doc1 ) );
    parser.next(); // read the START document...
    //null for the processor is all right here
    XMLLoader loader = new XMLLoader();
    SolrInputDocument document1 = loader.readDoc( parser );

    XMLStreamReader parser2 =
        inputFactory.createXMLStreamReader( new StringReader( xml_doc2 ) );
    parser2.next(); // read the START document...
    //null for the processor is all right here
    //XMLLoader loader = new XMLLoader();
    SolrInputDocument document2 = loader.readDoc( parser2 );

    assertFalse(document1.hasChildDocuments());
    assertEquals(document1.toString(), sdoc("id", "1", "empty_s", "", "parent_s", "X", "test",
        sdocs(sdoc("id", "2", "child_s", "y"), sdoc("id", "3", "child_s", "z"))).toString());

    assertFalse(document2.hasChildDocuments());
    assertEquals(document2.toString(), sdoc("id", "4", "parent_s", "A", "test",
        sdocs(sdoc("id", "5", "child_s", "b"), sdoc("id", "6", "child_s", "c"))).toString());

    docs.add(document1);
    docs.add(document2);

    Collections.shuffle(docs, random());
    req.add(docs);

    RequestWriter requestWriter = new RequestWriter();
    OutputStream os = new ByteArrayOutputStream();
    requestWriter.write(req, os);
    assertBlockU(os.toString());
    assertU(commit());

    final SolrIndexSearcher searcher = getSearcher();
    assertSingleParentOf(searcher, one("yz"), "X");
    assertSingleParentOf(searcher, one("bc"), "A");
  }

  @Test
  public void testJavaBinCodecNestedRelation() throws IOException {
    SolrInputDocument topDocument = new SolrInputDocument();
    topDocument.addField("parent_f1", "v1");
    topDocument.addField("parent_f2", "v2");

    int childsNum = atLeast(10);
    Map<String, SolrInputDocument> children = new HashMap<>(childsNum);
    for(int i = 0; i < childsNum; ++i) {
      SolrInputDocument child = new SolrInputDocument();
      child.addField("key", (i + 5) * atLeast(4));
      String childKey = String.format(Locale.ROOT, "child%d", i);
      topDocument.addField(childKey, child);
      children.put(childKey, child);
    }

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      jbc.marshal(topDocument, os);
    }
    byte[] buffer = os.toByteArray();
    //now read the Object back
    SolrInputDocument result;
    try (JavaBinCodec jbc = new JavaBinCodec(); InputStream is = new ByteArrayInputStream(buffer)) {
      result = (SolrInputDocument) jbc.unmarshal(is);
    }

    assertTrue(compareSolrInputDocument(topDocument, result));
  }


  @Test
  public void testJavaBinCodec() throws IOException { //actually this test must be in other test class
    SolrInputDocument topDocument = new SolrInputDocument();
    topDocument.addField("parent_f1", "v1");
    topDocument.addField("parent_f2", "v2");

    int childsNum = atLeast(10);
    for (int index = 0; index < childsNum; ++index) {
      addChildren("child", topDocument, index, false);
    }

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      jbc.marshal(topDocument, os);
    }
    byte[] buffer = os.toByteArray();
    //now read the Object back
    SolrInputDocument result;
    try (JavaBinCodec jbc = new JavaBinCodec(); InputStream is = new ByteArrayInputStream(buffer)) {
      result = (SolrInputDocument) jbc.unmarshal(is);
    }
    assertEquals(2, result.size());
    assertEquals("v1", result.getFieldValue("parent_f1"));
    assertEquals("v2", result.getFieldValue("parent_f2"));

    List<SolrInputDocument> resultChilds = result.getChildDocuments();
    int resultChildsSize = resultChilds == null ? 0 : resultChilds.size();
    assertEquals(childsNum, resultChildsSize);

    for (int childIndex = 0; childIndex < childsNum; ++childIndex) {
      SolrInputDocument child = resultChilds.get(childIndex);
      for (int fieldNum = 0; fieldNum < childIndex; ++fieldNum) {
        assertEquals(childIndex + "value" + fieldNum, child.getFieldValue(childIndex + "child" + fieldNum));
      }

      List<SolrInputDocument> grandChilds = child.getChildDocuments();
      int grandChildsSize = grandChilds == null ? 0 : grandChilds.size();

      assertEquals(childIndex * 2, grandChildsSize);
      for (int grandIndex = 0; grandIndex < childIndex * 2; ++grandIndex) {
        SolrInputDocument grandChild = grandChilds.get(grandIndex);
        assertFalse(grandChild.hasChildDocuments());
        for (int fieldNum = 0; fieldNum < grandIndex; ++fieldNum) {
          assertEquals(grandIndex + "value" + fieldNum, grandChild.getFieldValue(grandIndex + "grand" + fieldNum));
        }
      }
    }
  }

  private void addChildren(String prefix, SolrInputDocument topDocument, int childIndex, boolean lastLevel) {
    SolrInputDocument childDocument = new SolrInputDocument();
    for (int index = 0; index < childIndex; ++index) {
      childDocument.addField(childIndex + prefix + index, childIndex + "value"+ index);
    }

    if (!lastLevel) {
      for (int i = 0; i < childIndex * 2; ++i) {
        addChildren("grand", childDocument, i, true);
      }
    }
    topDocument.addChildDocument(childDocument);
  }

  /**
   * on the given abcD it generates one parent doc, taking D from the tail and
   * two subdocs relaitons ab and c uniq ids are supplied also
   *
   * <pre>
   * {@code
   * <add>
   *  <doc>
   *    <field name="parent_s">D</field>
   *    <doc>
   *        <field name="child_s">a</field>
   *        <field name="type_s">1</field>
   *    </doc>
   *    <doc>
   *        <field name="child_s">b</field>
   *        <field name="type_s">1</field>
   *    </doc>
   *    <doc>
   *        <field name="child_s">c</field>
   *        <field name="type_s">2</field>
   *    </doc>
   *  </doc>
   * </add>
   * }
   * </pre>
   * */
  private Document block(String string) throws ParserConfigurationException {
    Document document = getDocument();
    Element root = document.createElement("add");
    document.appendChild(root);
    Element doc = document.createElement("doc");
    root.appendChild(doc);

    if (string.length() > 0) {
      // last character is a top parent
      attachField(document, doc, parent,
          String.valueOf(string.charAt(string.length() - 1)));
      attachField(document, doc, "id", id());

      // add subdocs
      int type = 1;
      for (int i = 0; i < string.length() - 1; i += 2) {
        String relation = string.substring(i,
            Math.min(i + 2, string.length() - 1));
        attachSubDocs(document, doc, relation, type);
        type++;
      }
    }

    return document;
  }

  private void attachSubDocs(Document document, Element parent, String relation, int typeValue) {
    for (int j = 0; j < relation.length(); j++) {
      Element doc = document.createElement("doc");
      parent.appendChild(doc);
      attachField(document, doc, child, String.valueOf(relation.charAt(j)));
      attachField(document, doc, "id", id());
      attachField(document, doc, type, String.valueOf(typeValue));
    }
  }

  private void indexSolrInputDocumentsDirectly(SolrInputDocument ... docs) throws IOException {
    SolrQueryRequest coreReq = new LocalSolrQueryRequest(h.getCore(), new ModifiableSolrParams());
    AddUpdateCommand updateCmd = new AddUpdateCommand(coreReq);
    for (SolrInputDocument doc: docs) {
      updateCmd.solrDoc = doc;
      h.getCore().getUpdateHandler().addDoc(updateCmd);
      updateCmd.clear();
    }
    assertU(commit());
  }

  /**
   * Merges two documents like
   *
   * <pre>
   * {@code <add>...</add> + <add>...</add> = <add>... + ...</add>}
   * </pre>
   *
   * @param doc1
   *          first document
   * @param doc2
   *          second document
   * @return merged document
   */
  private Document merge(Document doc1, Document doc2) {
    NodeList doc2ChildNodes = doc2.getDocumentElement().getChildNodes();
    for(int i = 0; i < doc2ChildNodes.getLength(); i++) {
      Node doc2ChildNode = doc2ChildNodes.item(i);
      doc1.getDocumentElement().appendChild(doc1.importNode(doc2ChildNode, true));
      doc2.getDocumentElement().removeChild(doc2ChildNode);
    }

    return doc1;
  }

  private void attachField(Document document, Element root, String fieldName, String value) {
    Element field = document.createElement("field");
    field.setAttribute("name", fieldName);
    field.setTextContent(value);
    root.appendChild(field);
  }

  private static String id() {
    return "" + counter.incrementAndGet();
  }

  private String one(String string) {
    return "" + string.charAt(random().nextInt(string.length()));
  }

  protected void assertSingleParentOf(final SolrIndexSearcher searcher,
      final String childTerm, String parentExp) throws IOException {
    final TopDocs docs = searcher.search(join(childTerm), 10);
    assertEquals(1, docs.totalHits.value);
    final String pAct = searcher.doc(docs.scoreDocs[0].doc).get(parent);
    assertEquals(parentExp, pAct);
  }

  protected ToParentBlockJoinQuery join(final String childTerm) {
    return new ToParentBlockJoinQuery(
        new TermQuery(new Term(child, childTerm)), new QueryBitSetProducer(
            new TermRangeQuery(parent, null, null, false, false)), ScoreMode.None);
  }

  private Collection<? extends Callable<Void>> callables(List<Document> blocks) {
    final List<Callable<Void>> rez = new ArrayList<>();
    for (Document block : blocks) {
      final String msg = getStringFromDocument(block);
      if (msg.length() > 0) {
        rez.add(() -> {
          assertBlockU(msg);
          return null;
        });
        if (rarely()) {
          rez.add(() -> {
            assertBlockU(commit());
            return null;
          });
        }
      }
    }
    return rez;
  }

  private void assertBlockU(final String msg) {
    assertBlockU(msg, "0");
  }

  private void assertFailedBlockU(final String msg) {
    expectThrows(Exception.class, () -> assertBlockU(msg, "1"));
  }

  private void assertBlockU(final String msg, String expected) {
    try {
      String res = h.checkUpdateStatus(msg, expected);
      if (res != null) {
        fail("update was not successful: " + res + " expected: " + expected);
      }
    } catch (SAXException e) {
      throw new RuntimeException("Invalid XML", e);
    }
  }

  public static String getStringFromDocument(Document doc) {
    try (StringWriter writer = new StringWriter()){
      TransformerFactory tf = TransformerFactory.newInstance();
      Transformer transformer = tf.newTransformer();
      transformer.transform(new DOMSource(doc), new StreamResult(writer));
      return writer.toString();
    } catch (TransformerException | IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
