package org.apache.solr.core;

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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.core.SolrXMLSerializer.SolrCoreXMLDef;
import org.apache.solr.core.SolrXMLSerializer.SolrXMLDef;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;


public class TestSolrXMLSerializer extends LuceneTestCase {
  private static final XPathFactory xpathFactory = XPathFactory.newInstance();
  private static final String defaultCoreNameKey = "defaultCoreName";
  private static final String defaultCoreNameVal = "collection1";
  private static final String peristentKey = "persistent";
  private static final String persistentVal = "true";
  private static final String sharedLibKey = "sharedLib";
  private static final String sharedLibVal = "true";
  private static final String adminPathKey = "adminPath";
  private static final String adminPathVal = "/admin";
  private static final String shareSchemaKey = "admin";
  private static final String shareSchemaVal = "true";
  private static final String instanceDirKey = "instanceDir";
  private static final String instanceDirVal = "core1";
  
  @Test
  public void basicUsageTest() throws Exception {
    SolrXMLSerializer serializer = new SolrXMLSerializer();
    
    SolrXMLDef solrXMLDef = getTestSolrXMLDef(defaultCoreNameKey,
        defaultCoreNameVal, peristentKey, persistentVal, sharedLibKey,
        sharedLibVal, adminPathKey, adminPathVal, shareSchemaKey,
        shareSchemaVal, instanceDirKey, instanceDirVal);
    
    Writer w = new StringWriter();
    try {
      serializer.persist(w, solrXMLDef);
    } finally {
      w.close();
    }
    
    assertResults(((StringWriter) w).getBuffer().toString().getBytes("UTF-8"));
    
    // again with default file
    File tmpFile = File.createTempFile("solr", ".xml", TEMP_DIR);
    
    serializer.persistFile(tmpFile, solrXMLDef);

    assertResults(FileUtils.readFileToString(tmpFile, "UTF-8").getBytes("UTF-8"));
  }

  private void assertResults(byte[] bytes)
      throws ParserConfigurationException, UnsupportedEncodingException,
      IOException, SAXException, XPathExpressionException {
    DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    BufferedInputStream is = new BufferedInputStream(new ByteArrayInputStream(bytes));
    Document document;
    try {
//      is.mark(0);
//      System.out.println("SolrXML:" + IOUtils.toString(is, "UTF-8"));
//      is.reset();
      document = builder.parse(is);
    } finally {
      is.close();
    }
    
    assertTrue(exists("/solr[@" + peristentKey + "='" + persistentVal + "']", document));
    assertTrue(exists("/solr[@" + sharedLibKey + "='" + sharedLibVal + "']", document));
    assertTrue(exists("/solr/cores[@" + defaultCoreNameKey + "='" + defaultCoreNameVal + "']", document));
    assertTrue(exists("/solr/cores[@" + adminPathKey + "='" + adminPathVal + "']", document));
    assertTrue(exists("/solr/cores/core[@" + instanceDirKey + "='" + instanceDirVal + "']", document));
  }

  private SolrXMLDef getTestSolrXMLDef(String defaultCoreNameKey,
      String defaultCoreNameVal, String peristentKey, String persistentVal,
      String sharedLibKey, String sharedLibVal, String adminPathKey,
      String adminPathVal, String shareSchemaKey, String shareSchemaVal,
      String instanceDirKey, String instanceDirVal) {
    // <solr attrib="value">
    Map<String,String> rootSolrAttribs = new HashMap<String,String>();
    rootSolrAttribs.put(sharedLibKey, sharedLibVal);
    rootSolrAttribs.put(peristentKey, persistentVal);
    
    // <solr attrib="value"> <cores attrib="value">
    Map<String,String> coresAttribs = new HashMap<String,String>();
    coresAttribs.put(adminPathKey, adminPathVal);
    coresAttribs.put(shareSchemaKey, shareSchemaVal);
    coresAttribs.put(defaultCoreNameKey, defaultCoreNameVal);
    
    SolrXMLDef solrXMLDef = new SolrXMLDef();
    
    // <solr attrib="value"> <cores attrib="value"> <core attrib="value">
    List<SolrCoreXMLDef> solrCoreXMLDefs = new ArrayList<SolrCoreXMLDef>();
    SolrCoreXMLDef coreDef = new SolrCoreXMLDef();
    Map<String,String> coreAttribs = new HashMap<String,String>();
    coreAttribs.put(instanceDirKey, instanceDirVal);
    coreDef.coreAttribs = coreAttribs ;
    coreDef.coreProperties = new Properties();
    solrCoreXMLDefs.add(coreDef);
    
    solrXMLDef.coresDefs = solrCoreXMLDefs ;
    Properties containerProperties = new Properties();
    solrXMLDef.containerProperties = containerProperties ;
    solrXMLDef.solrAttribs = rootSolrAttribs;
    solrXMLDef.coresAttribs = coresAttribs;
    return solrXMLDef;
  }
  
  public static boolean exists(String xpathStr, Node node)
      throws XPathExpressionException {
    XPath xpath = xpathFactory.newXPath();
    return (Boolean) xpath.evaluate(xpathStr, node, XPathConstants.BOOLEAN);
  }
}
