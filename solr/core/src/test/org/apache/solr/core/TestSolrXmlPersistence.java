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

package org.apache.solr.core;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import com.google.common.base.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.TestHarness;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class TestSolrXmlPersistence extends SolrTestCaseJ4 {

  private File solrHomeDirectory = new File(TEMP_DIR, this.getClass().getName());

  @Rule
  public TestRule solrTestRules =
      RuleChain.outerRule(new SystemPropertiesRestoreRule());


  private CoreContainer init(String solrXmlString, String... subDirs) throws Exception {

    createTempDir();
    solrHomeDirectory = dataDir;

    for (String s : subDirs) {
      copyMinConf(new File(solrHomeDirectory, s));
    }

    File solrXml = new File(solrHomeDirectory, "solr.xml");
    FileUtils.write(solrXml, solrXmlString, IOUtils.CHARSET_UTF_8.toString());

    final CoreContainer cores = createCoreContainer(solrHomeDirectory.getAbsolutePath(), solrXmlString);
    return cores;
  }


  // take a solr.xml with system vars in <solr>, <cores> and <core> and <core/properties> tags that have system
  // variables defined. Insure that after persisting solr.xml, they're all still there as ${} syntax.
  // Also insure that nothing extra crept in.
  @Test
  public void testSystemVars() throws Exception {
    //Set these system props in order to insure that we don't write out the values rather than the ${} syntax.
    System.setProperty("solr.zkclienttimeout", "93");
    System.setProperty("solrconfig", "solrconfig.xml");
    System.setProperty("schema", "schema.xml");
    System.setProperty("zkHostSet", "localhost:9983");

    CoreContainer cc = init(SOLR_XML_LOTS_SYSVARS, "SystemVars1", "SystemVars2");
    try {
      origMatchesPersist(cc, SOLR_XML_LOTS_SYSVARS);
    } finally {
      cc.shutdown();
      if (solrHomeDirectory.exists()) {
        FileUtils.deleteDirectory(solrHomeDirectory);
      }
    }
  }

  @Test
  public void testReload() throws Exception {
    // Whether the core is transient or not can make a difference.
    doReloadTest("SystemVars2");
    doReloadTest("SystemVars1");

  }

  private void doReloadTest(String which) throws Exception {

    CoreContainer cc = init(SOLR_XML_LOTS_SYSVARS, "SystemVars1", "SystemVars2");
    try {
      final CoreAdminHandler admin = new CoreAdminHandler(cc);
      SolrQueryResponse resp = new SolrQueryResponse();
      admin.handleRequestBody
          (req(CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.RELOAD.toString(),
              CoreAdminParams.CORE, which),
              resp);
      assertNull("Exception on reload", resp.getException());

      origMatchesPersist(cc, SOLR_XML_LOTS_SYSVARS);
    } finally {
      cc.shutdown();
      if (solrHomeDirectory.exists()) {
        FileUtils.deleteDirectory(solrHomeDirectory);
      }
    }

  }

  @Test
  public void testRename() throws Exception {
    doTestRename("SystemVars1");
    doTestRename("SystemVars2");
  }

  private void doTestRename(String which) throws Exception {
    CoreContainer cc = init(SOLR_XML_LOTS_SYSVARS, "SystemVars1", "SystemVars2");
    SolrXMLCoresLocator.NonPersistingLocator locator
        = (SolrXMLCoresLocator.NonPersistingLocator) cc.getCoresLocator();

    try {
      final CoreAdminHandler admin = new CoreAdminHandler(cc);
      SolrQueryResponse resp = new SolrQueryResponse();
      admin.handleRequestBody
          (req(CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.RENAME.toString(),
              CoreAdminParams.CORE, which,
              CoreAdminParams.OTHER, "RenamedCore"),
              resp);
      assertNull("Exception on rename", resp.getException());

      // OK, Assure that if I change everything that has been renamed with the original value for the core, it matches
      // the old list
      String[] persistList = getAllNodes();
      String[] expressions = new String[persistList.length];

      for (int idx = 0; idx < persistList.length; ++idx) {
        expressions[idx] = persistList[idx].replaceAll("RenamedCore", which);
      }

      //assertXmlFile(origXml, expressions);
      TestHarness.validateXPath(SOLR_XML_LOTS_SYSVARS, expressions);

      // Now the other way, If I replace the original name in the original XML file with "RenamedCore", does it match
      // what was persisted?
      persistList = getAllNodes(SOLR_XML_LOTS_SYSVARS);
      expressions = new String[persistList.length];
      for (int idx = 0; idx < persistList.length; ++idx) {
        // /solr/cores/core[@name='SystemVars1' and @collection='${collection:collection1}']
        expressions[idx] = persistList[idx].replace("@name='" + which + "'", "@name='RenamedCore'");
      }

      TestHarness.validateXPath(locator.xml, expressions);

    } finally {
      cc.shutdown();
      if (solrHomeDirectory.exists()) {
        FileUtils.deleteDirectory(solrHomeDirectory);
      }
    }
  }

  @Test
  public void testSwap() throws Exception {
    doTestSwap("SystemVars1", "SystemVars2");
    doTestSwap("SystemVars2", "SystemVars1");
  }

  private void doTestSwap(String from, String to) throws Exception {
    CoreContainer cc = init(SOLR_XML_LOTS_SYSVARS, "SystemVars1", "SystemVars2");
    try {
      final CoreAdminHandler admin = new CoreAdminHandler(cc);
      SolrQueryResponse resp = new SolrQueryResponse();
      admin.handleRequestBody
          (req(CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.SWAP.toString(),
              CoreAdminParams.CORE, from,
              CoreAdminParams.OTHER, to),
              resp);
      assertNull("Exception on swap", resp.getException());

      String[] persistList = getAllNodes();
      String[] expressions = new String[persistList.length];

      // Now manually change the names back and it should match exactly to the original XML.
      for (int idx = 0; idx < persistList.length; ++idx) {
        String fromName = "@name='" + from + "'";
        String toName = "@name='" + to + "'";
        if (persistList[idx].contains(fromName)) {
          expressions[idx] = persistList[idx].replace(fromName, toName);
        } else {
          expressions[idx] = persistList[idx].replace(toName, fromName);
        }
      }

      //assertXmlFile(origXml, expressions);
      TestHarness.validateXPath(SOLR_XML_LOTS_SYSVARS, expressions);

    } finally {
      cc.shutdown();
      if (solrHomeDirectory.exists()) {
        FileUtils.deleteDirectory(solrHomeDirectory);
      }
    }
  }

  @Test
  public void testMinimalXml() throws Exception {
    CoreContainer cc = init(SOLR_XML_MINIMAL, "SystemVars1");
    try {
      cc.shutdown();
      origMatchesPersist(cc, SOLR_XML_MINIMAL);
    } finally {
      cc.shutdown();
      if (solrHomeDirectory.exists()) {
        FileUtils.deleteDirectory(solrHomeDirectory);
      }
    }
  }

  private void origMatchesPersist(CoreContainer cc, String originalSolrXML) throws Exception  {
    String[] expressions = getAllNodes(originalSolrXML);
    SolrXMLCoresLocator.NonPersistingLocator locator
        = (SolrXMLCoresLocator.NonPersistingLocator) cc.getCoresLocator();

    TestHarness.validateXPath(locator.xml, expressions);
  }

  @Test
  public void testUnloadCreate() throws Exception {
    doTestUnloadCreate("SystemVars1");
    doTestUnloadCreate("SystemVars2");
  }

  private void doTestUnloadCreate(String which) throws Exception {
    CoreContainer cc = init(SOLR_XML_LOTS_SYSVARS, "SystemVars1", "SystemVars2");
    try {
      final CoreAdminHandler admin = new CoreAdminHandler(cc);

      SolrQueryResponse resp = new SolrQueryResponse();
      admin.handleRequestBody
          (req(CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.UNLOAD.toString(),
              CoreAdminParams.CORE, which),
              resp);
      assertNull("Exception on unload", resp.getException());

      //origMatchesPersist(cc, new File(solrHomeDirectory, "unloadcreate1.solr.xml"));

      String instPath = new File(solrHomeDirectory, which).getAbsolutePath();
      admin.handleRequestBody
          (req(CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.CREATE.toString(),
              CoreAdminParams.INSTANCE_DIR, instPath,
              CoreAdminParams.NAME, which),
              resp);
      assertNull("Exception on create", resp.getException());

      String[] persistList = getAllNodes();
      String[] expressions = new String[persistList.length];

      // Now manually change the names back and it should match exactly to the original XML.
      for (int idx = 0; idx < persistList.length; ++idx) {
        String name = "@name='" + which + "'";

        if (persistList[idx].contains(name)) {
          if (persistList[idx].contains("@schema='schema.xml'")) {
            expressions[idx] = persistList[idx].replace("schema.xml", "${schema:schema.xml}");
          } else if (persistList[idx].contains("@config='solrconfig.xml'")) {
            expressions[idx] = persistList[idx].replace("solrconfig.xml", "${solrconfig:solrconfig.xml}");
          } else if (persistList[idx].contains("@instanceDir=")) {
            expressions[idx] = persistList[idx].replaceFirst("instanceDir\\='.*?'", "instanceDir='" + which + "/'");
          } else {
            expressions[idx] = persistList[idx];
          }
        } else {
          expressions[idx] = persistList[idx];
        }
      }

      //assertXmlFile(origXml, expressions);
      TestHarness.validateXPath(SOLR_XML_LOTS_SYSVARS, expressions);

    } finally {
      cc.shutdown();
      if (solrHomeDirectory.exists()) {
        FileUtils.deleteDirectory(solrHomeDirectory);
      }
    }
  }

  @Test
  public void testCreatePersistCore() throws Exception {
    // Template for creating a core.
    CoreContainer cc = init(SOLR_XML_LOTS_SYSVARS, "SystemVars1", "SystemVars2", "props1", "props2");
    SolrXMLCoresLocator.NonPersistingLocator locator
        = (SolrXMLCoresLocator.NonPersistingLocator) cc.getCoresLocator();

    try {
      final CoreAdminHandler admin = new CoreAdminHandler(cc);
      // create a new core (using CoreAdminHandler) w/ properties

      SolrQueryResponse resp = new SolrQueryResponse();
      admin.handleRequestBody
          (req(CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.CREATE.toString(),
              CoreAdminParams.NAME, "props1",
              CoreAdminParams.TRANSIENT, "true",
              CoreAdminParams.LOAD_ON_STARTUP, "true",
              CoreAdminParams.PROPERTY_PREFIX + "prefix1", "valuep1",
              CoreAdminParams.PROPERTY_PREFIX + "prefix2", "valueP2",
              "wt", "json", // need to insure that extra parameters are _not_ preserved (actually happened).
              "qt", "admin/cores"),
              resp);
      assertNull("Exception on create", resp.getException());

      String instPath2 = new File(solrHomeDirectory, "props2").getAbsolutePath();
      admin.handleRequestBody
          (req(CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.CREATE.toString(),
              CoreAdminParams.INSTANCE_DIR, instPath2,
              CoreAdminParams.NAME, "props2",
              CoreAdminParams.PROPERTY_PREFIX + "prefix2_1", "valuep2_1",
              CoreAdminParams.PROPERTY_PREFIX + "prefix2_2", "valueP2_2",
              CoreAdminParams.CONFIG, "solrconfig.xml",
              CoreAdminParams.DATA_DIR, "./dataDirTest",
              CoreAdminParams.SCHEMA, "schema.xml"),
              resp);
      assertNull("Exception on create", resp.getException());

      // Everything that was in the original XML file should be in the persisted one.
      TestHarness.validateXPath(locator.xml, getAllNodes(SOLR_XML_LOTS_SYSVARS));

      // And the params for the new core should be in the persisted file.
      TestHarness.validateXPath
          (
              locator.xml,
              "/solr/cores/core[@name='props1']/property[@name='prefix1' and @value='valuep1']"
              , "/solr/cores/core[@name='props1']/property[@name='prefix2' and @value='valueP2']"
              , "/solr/cores/core[@name='props1' and @transient='true']"
              , "/solr/cores/core[@name='props1' and @loadOnStartup='true']"
              , "/solr/cores/core[@name='props1' and @instanceDir='props1" + File.separator + "']"
              , "/solr/cores/core[@name='props2']/property[@name='prefix2_1' and @value='valuep2_1']"
              , "/solr/cores/core[@name='props2']/property[@name='prefix2_2' and @value='valueP2_2']"
              , "/solr/cores/core[@name='props2' and @config='solrconfig.xml']"
              , "/solr/cores/core[@name='props2' and @schema='schema.xml']"
              , "/solr/cores/core[@name='props2' and not(@loadOnStartup)]"
              , "/solr/cores/core[@name='props2' and not(@transient)]"
              , "/solr/cores/core[@name='props2' and @instanceDir='" + instPath2 + "']"
              , "/solr/cores/core[@name='props2' and @dataDir='./dataDirTest']"
          );

    } finally {
      cc.shutdown();
      if (solrHomeDirectory.exists()) {
        FileUtils.deleteDirectory(solrHomeDirectory);
      }

    }
  }

  @Test
  public void testPersist() throws Exception {

    String defXml = FileUtils.readFileToString(
        new File(SolrTestCaseJ4.TEST_HOME(), "solr.xml"),
        Charsets.UTF_8.toString());
    final CoreContainer cores = init(defXml, "collection1");
    SolrXMLCoresLocator.NonPersistingLocator locator
        = (SolrXMLCoresLocator.NonPersistingLocator) cores.getCoresLocator();

    String instDir = null;
    {
      SolrCore template = null;
      try {
        template = cores.getCore("collection1");
        instDir = template.getCoreDescriptor().getRawInstanceDir();
      } finally {
        if (null != template) template.close();
      }
    }

    final File instDirFile = new File(cores.getSolrHome(), instDir);
    assertTrue("instDir doesn't exist: " + instDir, instDirFile.exists());

    // sanity check the basic persistence of the default init
    TestHarness.validateXPath(locator.xml,
        "/solr[@persistent='true']",
        "/solr/cores[@defaultCoreName='collection1' and not(@transientCacheSize)]",
        "/solr/cores/core[@name='collection1' and @instanceDir='" + instDir +
            "' and @transient='false' and @loadOnStartup='true' ]",
        "1=count(/solr/cores/core)");

    // create some new cores and sanity check the persistence

    final File dataXfile = new File(solrHomeDirectory, "dataX");
    final String dataX = dataXfile.getAbsolutePath();
    assertTrue("dataXfile mkdirs failed: " + dataX, dataXfile.mkdirs());

    final File instYfile = new File(solrHomeDirectory, "instY");
    FileUtils.copyDirectory(instDirFile, instYfile);

    // :HACK: dataDir leaves off trailing "/", but instanceDir uses it
    final String instY = instYfile.getAbsolutePath() + "/";

    final CoreDescriptor xd = buildCoreDescriptor(cores, "X", instDir)
        .withDataDir(dataX).build();

    final CoreDescriptor yd = new CoreDescriptor(cores, "Y", instY);

    SolrCore x = null;
    SolrCore y = null;
    try {
      x = cores.create(xd);
      y = cores.create(yd);
      cores.register(x, false);
      cores.register(y, false);

      assertEquals("cores not added?", 3, cores.getCoreNames().size());

      TestHarness.validateXPath(locator.xml,
          "/solr[@persistent='true']",
          "/solr/cores[@defaultCoreName='collection1']",
          "/solr/cores/core[@name='collection1' and @instanceDir='" + instDir
              + "']", "/solr/cores/core[@name='X' and @instanceDir='" + instDir
              + "' and @dataDir='" + dataX + "']",
          "/solr/cores/core[@name='Y' and @instanceDir='" + instY + "']",
          "3=count(/solr/cores/core)");

      // Test for saving implicit properties, we should not do this.
      TestHarness.validateXPath(locator.xml,
          "/solr/cores/core[@name='X' and not(@solr.core.instanceDir) and not (@solr.core.configName)]");

      // delete a core, check persistence again
      assertNotNull("removing X returned null", cores.remove("X"));

      TestHarness.validateXPath(locator.xml, "/solr[@persistent='true']",
          "/solr/cores[@defaultCoreName='collection1']",
          "/solr/cores/core[@name='collection1' and @instanceDir='" + instDir + "']",
          "/solr/cores/core[@name='Y' and @instanceDir='" + instY + "']",
          "2=count(/solr/cores/core)");

    } finally {
      // y is closed by the container, but
      // x has been removed from the container
      if (x != null) {
        try {
          x.close();
        } catch (Exception e) {
          log.error("", e);
        }
      }
      cores.shutdown();
    }
  }


  private String[] getAllNodes(InputStream is) throws ParserConfigurationException, IOException, SAXException {
    List<String> expressions = new ArrayList<String>(); // XPATH and value for all elements in the indicated XML
    DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
        .newInstance();
    DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
    Document document = docBuilder.parse(is);

    Node root = document.getDocumentElement();
    gatherNodes(root, expressions, "");
    return expressions.toArray(new String[expressions.size()]);
  }

  private String[] getAllNodes() throws ParserConfigurationException, IOException, SAXException {
    return getAllNodes(new FileInputStream(new File(solrHomeDirectory, "solr.xml")));
  }

  private String[] getAllNodes(String xmlString) throws ParserConfigurationException, IOException, SAXException {
    return getAllNodes(new ByteArrayInputStream(xmlString.getBytes(Charsets.UTF_8)));
  }

  /*
  private void assertSolrXmlFile(String... xpathExpressions) throws IOException, SAXException {
    assertXmlFile(new File(solrHomeDirectory, "solr.xml"), xpathExpressions);
  }
  */

  // Note this is pretty specialized for a solr.xml file because working with the DOM is such a pain.

  private static List<String> qualified = new ArrayList<String>() {{
    add("core");
    add("property");
    add("int");
    add("str");
    add("long");
    add("property");
  }};

  private static List<String> addText = new ArrayList<String>() {{
    add("int");
    add("str");
    add("long");
  }};

  // path is the path to parent node

  private void gatherNodes(Node node, List<String> expressions, String path) {

    String nodeName = node.getNodeName();
    String thisPath = path + "/" + nodeName;
    //Parent[@id='1']/Children/child[@name]
    // Add in the xpaths for verification of any attributes.
    NamedNodeMap attrs = node.getAttributes();
    String qualifier = "";
    if (attrs.getLength() > 0) {
      // Assemble the prefix for qualifying all of the attributes with the same name
      if (qualified.contains(nodeName)) {
        qualifier = "@name='" + node.getAttributes().getNamedItem("name").getTextContent() + "'";
      }

      for (int idx = 0; idx < attrs.getLength(); ++idx) {

        Node attr = attrs.item(idx);
        if (StringUtils.isNotBlank(qualifier) && "name".equals(attr.getNodeName())) {
          continue; // Already added "name" attribute in qualifier string.
        }
        if (StringUtils.isNotBlank(qualifier)) {
          // Create [@name="stuff" and @attrib="value"] fragment
          expressions.add(thisPath +
              "[" + qualifier + " and @" + attr.getNodeName() + "='" + attr.getTextContent() + "']");

        } else {
          // Create [@attrib="value"] fragment
          expressions.add(thisPath +
              "[" + qualifier + " @" + attr.getNodeName() + "='" + attr.getTextContent() + "']");
        }
      }
    }
    // Now add the text for special nodes
    // a[normalize-space(text())='somesite']
    if (addText.contains(nodeName)) {
      expressions.add(thisPath + "[" + qualifier + " and text()='" + node.getTextContent() + "']");
    }
    // Now collect all the child element nodes.
    NodeList nodeList = node.getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {

      Node currentNode = nodeList.item(i);
      if (currentNode.getNodeType() == Node.ELEMENT_NODE) {
        if (StringUtils.isNotBlank(qualifier)) {
          gatherNodes(currentNode, expressions, thisPath + "[" + qualifier + "]");
        } else {
          gatherNodes(currentNode, expressions, thisPath);
        }
      }
    }
  }

  public static String SOLR_XML_LOTS_SYSVARS =
      "<solr persistent=\"${solr.xml.persist:false}\" coreLoadThreads=\"12\" sharedLib=\"${something:.}\" >\n" +
          "  <logging class=\"${logclass:log4j.class}\" enabled=\"{logenable:true}\">\n" +
          "     <watcher size=\"${watchSize:13}\" threshold=\"${logThresh:54}\" />\n" +
          "  </logging>\n" +
          "  <cores adminPath=\"/admin/cores\" defaultCoreName=\"SystemVars1\" host=\"127.0.0.1\" \n" +
          "       hostPort=\"${hostPort:8983}\" hostContext=\"${hostContext:solr}\" \n" +
          "       zkClientTimeout=\"${solr.zkclienttimeout:30000}\" \n" +
          "       shareSchema=\"${shareSchema:false}\" distribUpdateConnTimeout=\"${distribUpdateConnTimeout:15000}\" \n" +
          "       distribUpdateSoTimeout=\"${distribUpdateSoTimeout:120000}\" \n" +
          "       leaderVoteWait=\"${leadVoteWait:32}\" managementPath=\"${manpath:/var/lib/path}\" transientCacheSize=\"${tranSize:128}\"> \n" +
          "     <core name=\"SystemVars1\" instanceDir=\"SystemVars1/\" shard=\"${shard:32}\" \n" +
          "          collection=\"${collection:collection1}\" config=\"${solrconfig:solrconfig.xml}\" \n" +
          "          schema=\"${schema:schema.xml}\" ulogDir=\"${ulog:./}\" roles=\"${myrole:boss}\" \n" +
          "          dataDir=\"${data:./}\" loadOnStartup=\"${onStart:true}\" transient=\"${tran:true}\" \n" +
          "          coreNodeName=\"${coreNode:utterlyridiculous}\" \n" +
          "       >\n" +
          "     </core>\n" +
          "     <core name=\"SystemVars2\" instanceDir=\"SystemVars2/\" shard=\"${shard:32}\" \n" +
          "          collection=\"${collection:collection2}\" config=\"${solrconfig:solrconfig.xml}\" \n" +
          "          coreNodeName=\"${coreNodeName:}\" schema=\"${schema:schema.xml}\">\n" +
          "      <property name=\"collection\" value=\"{collection:collection2}\"/>\n" +
          "      <property name=\"schema\" value=\"${schema:schema.xml}\"/>\n" +
          "      <property name=\"coreNodeName\" value=\"EricksCore\"/>\n" +
          "     </core>\n" +
          "     <shardHandlerFactory name=\"${shhandler:shardHandlerFactory}\" class=\"${handlefac:HttpShardHandlerFactory}\">\n" +
          "         <int name=\"socketTimeout\">${socketTimeout:120000}</int> \n" +
          "         <int name=\"connTimeout\">${connTimeout:15000}</int> \n" +
          "         <str name=\"arbitraryName\">${arbitrarySysValue:foobar}</str>\n" +
          "     </shardHandlerFactory> \n" +
          "   </cores>\n" +
          "</solr>";


  private static String SOLR_XML_MINIMAL =
          "<solr >\n" +
          "  <cores> \n" +
          "     <core name=\"SystemVars1\" instanceDir=\"SystemVars1/\" />\n" +
          "   </cores>\n" +
          "</solr>";

}
