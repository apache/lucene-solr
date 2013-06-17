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
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
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
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestSolrXmlPersistence extends SolrTestCaseJ4 {

  private final File solrHomeDirectory = new File(TEMP_DIR, this.getClass().getName());

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-minimal.xml", "schema-tiny.xml");
  }

  @Rule
  public TestRule solrTestRules =
      RuleChain.outerRule(new SystemPropertiesRestoreRule());


  private CoreContainer init(String solrXmlString, String... subDirs) throws Exception {
    if (solrHomeDirectory.exists()) {
      FileUtils.deleteDirectory(solrHomeDirectory);
    }

    for (String s : subDirs) {
      copyMinConf(new File(solrHomeDirectory, s));
    }

    File solrXml = new File(solrHomeDirectory, "solr.xml");
    FileUtils.write(solrXml, solrXmlString, IOUtils.CHARSET_UTF_8.toString());
    final CoreContainer cores = new CoreContainer(solrHomeDirectory.getAbsolutePath());
    cores.load(solrHomeDirectory.getAbsolutePath(), solrXml);

    cores.setPersistent(false);
    return cores;
  }


  // take a solr.xml with system vars in <solr>, <cores> and <core> and <core/properties> tags that have system
  // variables defined. Insure that after persisting solr.xml, they're all still there as ${} syntax.
  // Also insure that nothing extra crept in.
  @Test
  public void testSystemVars() throws Exception {
    //Set these system props in order to insure that we don't write out the values rather than the ${} syntax.
    System.setProperty("solr.zkclienttimeout", "93");
    System.setProperty("solrconfig", "solrconfig-minimal.xml");
    System.setProperty("schema", "schema-tiny.xml");
    System.setProperty("zkHostSet", "localhost:9983");

    CoreContainer cc = init(SOLR_XML_LOTS_SYSVARS, "SystemVars1", "SystemVars2");
    try {

      // This seems odd, but it's just a little self check to see if the comparison strings are being created correctly
      origMatchesPersist(cc, new File(solrHomeDirectory, "solr_copy.xml"));

      // Is everything in the persisted file identical to the original?
      final File persistXml = new File(solrHomeDirectory, "sysvars.solr.xml");
      // Side effect here is that the new file is persisted and available later.
      origMatchesPersist(cc, persistXml);

      // Is everything in the original contained in the persisted one?
      assertXmlFile(persistXml, getAllNodes(new File(solrHomeDirectory, "solr.xml")));

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

      origMatchesPersist(cc, new File(solrHomeDirectory, "reload1.solr.xml"));

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

      File persistXml = new File(solrHomeDirectory, "rename.solr.xml");
      File origXml = new File(solrHomeDirectory, "solr.xml");

      // OK, Assure that if I change everything that has been renamed with the original value for the core, it matches
      // the old list
      cc.persistFile(persistXml);
      String[] persistList = getAllNodes(persistXml);
      String[] expressions = new String[persistList.length];

      for (int idx = 0; idx < persistList.length; ++idx) {
        expressions[idx] = persistList[idx].replaceAll("RenamedCore", which);
      }

      assertXmlFile(origXml, expressions);

      // Now the other way, If I replace the original name in the original XML file with "RenamedCore", does it match
      // what was persisted?
      persistList = getAllNodes(origXml);
      for (int idx = 0; idx < persistList.length; ++idx) {
        // /solr/cores/core[@name='SystemVars1' and @collection='${collection:collection1}']
        expressions[idx] = persistList[idx].replace("@name='" + which + "'", "@name='RenamedCore'");
      }

      assertXmlFile(persistXml, expressions);
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

      File persistXml = new File(solrHomeDirectory, "rename.solr.xml");
      File origXml = new File(solrHomeDirectory, "solr.xml");

      cc.persistFile(persistXml);
      String[] persistList = getAllNodes(persistXml);
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

      assertXmlFile(origXml, expressions);

    } finally {
      cc.shutdown();
      if (solrHomeDirectory.exists()) {
        FileUtils.deleteDirectory(solrHomeDirectory);
      }
    }
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

      origMatchesPersist(cc, new File(solrHomeDirectory, "unloadcreate1.solr.xml"));

      String instPath = new File(solrHomeDirectory, which).getAbsolutePath();
      admin.handleRequestBody
          (req(CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.CREATE.toString(),
              CoreAdminParams.INSTANCE_DIR, instPath,
              CoreAdminParams.CONFIG, "solrconfig-minimal.xml",
              CoreAdminParams.SCHEMA, "schema-tiny.xml",
              CoreAdminParams.NAME, which),
              resp);
      assertNull("Exception on create", resp.getException());

      File persistXml = new File(solrHomeDirectory, "rename.solr.xml");
      File origXml = new File(solrHomeDirectory, "solr.xml");

      cc.persistFile(persistXml);
      String[] persistList = getAllNodes(persistXml);
      String[] expressions = new String[persistList.length];

      // Now manually change the names back and it should match exactly to the original XML.
      for (int idx = 0; idx < persistList.length; ++idx) {
        String name = "@name='" + which + "'";

        if (persistList[idx].contains(name)) {
          if (persistList[idx].contains("@schema='schema-tiny.xml'")) {
            expressions[idx] = persistList[idx].replace("schema-tiny.xml", "${schema:schema-tiny.xml}");
          } else if (persistList[idx].contains("@config='solrconfig-minimal.xml'")) {
            expressions[idx] = persistList[idx].replace("solrconfig-minimal.xml", "${solrconfig:solrconfig-minimal.xml}");
          } else if (persistList[idx].contains("@instanceDir=")) {
            expressions[idx] = persistList[idx].replaceFirst("instanceDir\\='.*?'", "instanceDir='" + which + "'");
          } else {
            expressions[idx] = persistList[idx];
          }
        } else {
          expressions[idx] = persistList[idx];
        }
      }

      assertXmlFile(origXml, expressions);


    } finally {
      cc.shutdown();
      if (solrHomeDirectory.exists()) {
        FileUtils.deleteDirectory(solrHomeDirectory);
      }
    }
  }

  private void origMatchesPersist(CoreContainer cc, File persistXml) throws IOException, SAXException, ParserConfigurationException {
    cc.persistFile(persistXml);
    // Is everything that's in the original file persisted?
    String[] expressions = getAllNodes(persistXml);
    assertXmlFile(new File(solrHomeDirectory, "solr.xml"), expressions);
  }

  @Test
  public void testCreatePersistCore() throws Exception {
    // Template for creating a core.
    CoreContainer cc = init(SOLR_XML_LOTS_SYSVARS, "SystemVars1", "SystemVars2", "props1", "props2");
    try {
      final CoreAdminHandler admin = new CoreAdminHandler(cc);
      // create a new core (using CoreAdminHandler) w/ properties
      String instPath1 = new File(solrHomeDirectory, "props1").getAbsolutePath();
      SolrQueryResponse resp = new SolrQueryResponse();
      admin.handleRequestBody
          (req(CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.CREATE.toString(),
              CoreAdminParams.INSTANCE_DIR, instPath1,
              CoreAdminParams.NAME, "props1",
              CoreAdminParams.TRANSIENT, "true",
              CoreAdminParams.LOAD_ON_STARTUP, "true",
              CoreAdminParams.PROPERTY_PREFIX + "prefix1", "valuep1",
              CoreAdminParams.PROPERTY_PREFIX + "prefix2", "valueP2",
              CoreAdminParams.CONFIG, "solrconfig-minimal.xml",
              CoreAdminParams.SCHEMA, "schema-tiny.xml"),
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
              CoreAdminParams.CONFIG, "solrconfig-minimal.xml",
              CoreAdminParams.DATA_DIR, "./dataDirTest",
              CoreAdminParams.SCHEMA, "schema-tiny.xml"),
              resp);
      assertNull("Exception on create", resp.getException());

      // Everything that was in the original XML file should be in the persisted one.
      final File persistXml = new File(solrHomeDirectory, "persist_create_core.solr.xml");
      cc.persistFile(persistXml);
      String[] expressions = getAllNodes(new File(solrHomeDirectory, "solr.xml"));
      assertXmlFile(persistXml, expressions);


      // And the params for the new core should be in the persisted file.
      assertXmlFile
          (persistXml
              , "/solr/cores/core[@name='props1']/property[@name='prefix1' and @value='valuep1']"
              , "/solr/cores/core[@name='props1']/property[@name='prefix2' and @value='valueP2']"
              , "/solr/cores/core[@name='props1' and @config='solrconfig-minimal.xml']"
              , "/solr/cores/core[@name='props1' and @schema='schema-tiny.xml']"
              , "/solr/cores/core[@name='props1' and @transient='true']"
              , "/solr/cores/core[@name='props1' and @loadOnStartup='true']"
              , "/solr/cores/core[@name='props2']/property[@name='prefix2_1' and @value='valuep2_1']"
              , "/solr/cores/core[@name='props2']/property[@name='prefix2_2' and @value='valueP2_2']"
              , "/solr/cores/core[@name='props2' and @config='solrconfig-minimal.xml']"
              , "/solr/cores/core[@name='props2' and @schema='schema-tiny.xml']"
              , "/solr/cores/core[@name='props2' and not(@loadOnStartup)]"
              , "/solr/cores/core[@name='props2' and not(@transient)]"
              , "/solr/cores/core[@name='props2' and @dataDir='./dataDirTest']"
          );

    } finally {
      cc.shutdown();
      if (solrHomeDirectory.exists()) {
        FileUtils.deleteDirectory(solrHomeDirectory);
      }

    }
    // / insure that after you create a core and persist solr.xml the created core has
    // all expected and no extraneous values, both attribs and <property> tags.
    // How to create this core with sysprops?
  }

  private String[] getAllNodes(File xmlFile) throws ParserConfigurationException, IOException, SAXException {
    List<String> expressions = new ArrayList<String>(); // XPATH and value for all elements in the indicated XML
    DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
        .newInstance();
    DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
    Document document = docBuilder.parse(xmlFile);

    Node root = document.getDocumentElement();
    gatherNodes(root, expressions, "");
    return expressions.toArray(new String[expressions.size()]);
  }


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

  private static String SOLR_XML_LOTS_SYSVARS =
      "<solr persistent=\"${solr.xml.persist:false}\" coreLoadThreads=\"12\" sharedLib=\"${something:.}\" >\n" +
          "  <logging class=\"${logclass:log4j.class}\" enabled=\"{logenable:true}\">\n" +
          "     <watcher size=\"{watchSize:13}\" threshold=\"${logThresh:54}\" />\n" +
          "  </logging>\n" +
          "  <shardHandlerFactory name=\"${shhandler:shardHandlerFactory}\" class=\"${handlefac:HttpShardHandlerFactory}\">\n" +
          "     <int name=\"socketTimeout\">${socketTimeout:120000}</int> \n" +
          "     <int name=\"connTimeout\">${connTimeout:15000}</int> \n" +
          "  </shardHandlerFactory> \n" +
          "  <cores adminPath=\"/admin/cores\" defaultCoreName=\"SystemVars1\" host=\"127.0.0.1\" \n" +
          "       hostPort=\"${hostPort:8983}\" hostContext=\"${hostContext:solr}\" \n" +
          "       zkClientTimeout=\"${solr.zkclienttimeout:30000}\" \n" +
          "       shareSchema=\"${shareSchema:false}\" distribUpdateConnTimeout=\"${distribUpdateConnTimeout:15000}\" \n" +
          "       distribUpdateSoTimeout=\"${distribUpdateSoTimeout:120000}\" \n" +
          "       leaderVoteWait=\"${leadVoteWait:32}\" managementPath=\"${manpath:/var/lib/path}\" transientCacheSize=\"${tranSize:128}\"> \n" +
          "     <core name=\"SystemVars1\" instanceDir=\"SystemVars1\" shard=\"${shard:32}\" \n" +
          "          collection=\"${collection:collection1}\" config=\"${solrconfig:solrconfig-minimal.xml}\" \n" +
          "          schema=\"${schema:schema-tiny.xml}\" ulogDir=\"${ulog:./}\" roles=\"${myrole:boss}\" \n" +
          "          dataDir=\"${data:./}\" loadOnStartup=\"${onStart:true}\" transient=\"${tran:true}\" \n" +
          "          coreNodeName=\"${coreNode:utterlyridiculous}\" \n" +
          "       >\n" +
          "     </core>\n" +
          "     <core name=\"SystemVars2\" instanceDir=\"SystemVars2\" shard=\"${shard:32}\" \n" +
          "          collection=\"${collection:collection2}\" config=\"${solrconfig:solrconfig-minimal.xml}\" \n" +
          "          coreNodeName=\"${coreNodeName:}\" schema=\"${schema:schema-tiny.xml}\">\n" +
          "      <property name=\"collection\" value=\"{collection:collection2}\"/>\n" +
          "      <property name=\"schema\" value=\"${schema:schema-tiny.xml}\"/>\n" +
          "      <property name=\"coreNodeName\" value=\"EricksCore\"/>\n" +
          "     </core>\n" +
          "   </cores>\n" +
          "</solr>";


}
