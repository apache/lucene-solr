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

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.io.IOUtils;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.XMLErrorLogger;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.util.SystemIdResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Wrapper around an XML DOM object to provide convenient accessors to it.  Intended for XML config files.
 */
public class XmlConfigFile { // formerly simply "Config"
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final XMLErrorLogger xmllog = new XMLErrorLogger(log);

  static final XPathFactory xpathFactory = XPathFactory.newInstance();

  private final Document doc;
  private final Document origDoc; // with unsubstituted properties
  private final String prefix;
  private final String name;
  private final SolrResourceLoader loader;
  private int zkVersion = -1;

  /**
   * Builds a config from a resource name with no xpath prefix.
   */
  public XmlConfigFile(SolrResourceLoader loader, String name) throws ParserConfigurationException, IOException, SAXException
  {
    this( loader, name, null, null );
  }

  public XmlConfigFile(SolrResourceLoader loader, String name, InputSource is, String prefix) throws ParserConfigurationException, IOException, SAXException
  {
    this(loader, name, is, prefix, true);
  }
  /**
   * Builds a config:
   * <p>
   * Note that the 'name' parameter is used to obtain a valid input stream if no valid one is provided through 'is'.
   * If no valid stream is provided, a valid SolrResourceLoader instance should be provided through 'loader' so
   * the resource can be opened (@see SolrResourceLoader#openResource); if no SolrResourceLoader instance is provided, a default one
   * will be created.
   * </p>
   * <p>
   * Consider passing a non-null 'name' parameter in all use-cases since it is used for logging &amp; exception reporting.
   * </p>
   * @param loader the resource loader used to obtain an input stream if 'is' is null
   * @param name the resource name used if the input stream 'is' is null
   * @param is the resource as a SAX InputSource
   * @param prefix an optional prefix that will be prepended to all non-absolute xpath expressions
   */
  public XmlConfigFile(SolrResourceLoader loader, String name, InputSource is, String prefix, boolean substituteProps) throws ParserConfigurationException, IOException, SAXException
  {
    if( loader == null ) {
      loader = new SolrResourceLoader(SolrResourceLoader.locateSolrHome());
    }
    this.loader = loader;
    this.name = name;
    this.prefix = (prefix != null && !prefix.endsWith("/"))? prefix + '/' : prefix;
    try {
      javax.xml.parsers.DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

      if (is == null) {
        InputStream in = loader.openConfig(name);
        if (in instanceof ZkSolrResourceLoader.ZkByteArrayInputStream) {
          zkVersion = ((ZkSolrResourceLoader.ZkByteArrayInputStream) in).getStat().getVersion();
          log.debug("loaded config {} with version {} ",name,zkVersion);
        }
        is = new InputSource(in);
        is.setSystemId(SystemIdResolver.createSystemIdFromResourceName(name));
      }

      // only enable xinclude, if a SystemId is available
      if (is.getSystemId() != null) {
        try {
          dbf.setXIncludeAware(true);
          dbf.setNamespaceAware(true);
        } catch(UnsupportedOperationException e) {
          log.warn(name + " XML parser doesn't support XInclude option");
        }
      }
      
      final DocumentBuilder db = dbf.newDocumentBuilder();
      db.setEntityResolver(new SystemIdResolver(loader));
      db.setErrorHandler(xmllog);
      try {
        doc = db.parse(is);
        origDoc = copyDoc(doc);
      } finally {
        // some XML parsers are broken and don't close the byte stream (but they should according to spec)
        IOUtils.closeQuietly(is.getByteStream());
      }
      if (substituteProps) {
        DOMUtil.substituteProperties(doc, getSubstituteProperties());
      }
    } catch (ParserConfigurationException | SAXException | TransformerException e)  {
      SolrException.log(log, "Exception during parsing file: " + name, e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /*
     * Assert that assertCondition is true.
     * If not, prints reason as log warning.
     * If failCondition is true, then throw exception instead of warning
     */
  public static void assertWarnOrFail(String reason, boolean assertCondition, boolean failCondition) {
    if (assertCondition) {
      return;
    } else if (failCondition) {
      throw new SolrException(SolrException.ErrorCode.FORBIDDEN, reason);
    } else {
      log.warn(reason);
    }
  }

  protected Properties getSubstituteProperties() {
    return loader.getCoreProperties();
  }

  public XmlConfigFile(SolrResourceLoader loader, String name, Document doc) {
    this.prefix = null;
    this.doc = doc;
    try {
      this.origDoc = copyDoc(doc);
    } catch (TransformerException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    this.name = name;
    this.loader = loader;
  }

  
  private static Document copyDoc(Document doc) throws TransformerException {
    TransformerFactory tfactory = TransformerFactory.newInstance();
    Transformer tx = tfactory.newTransformer();
    DOMSource source = new DOMSource(doc);
    DOMResult result = new DOMResult();
    tx.transform(source, result);
    return (Document) result.getNode();
  }
  
  /**
   * @since solr 1.3
   */
  public SolrResourceLoader getResourceLoader()
  {
    return loader;
  }

  /**
   * @since solr 1.3
   */
  public String getResourceName() {
    return name;
  }

  public String getName() {
    return name;
  }
  
  public Document getDocument() {
    return doc;
  }

  public XPath getXPath() {
    return xpathFactory.newXPath();
  }

  private String normalize(String path) {
    return (prefix==null || path.startsWith("/")) ? path : prefix+path;
  }
  
  public void substituteProperties() {
    DOMUtil.substituteProperties(doc, getSubstituteProperties());
  }


  public Object evaluate(String path, QName type) {
    XPath xpath = xpathFactory.newXPath();
    try {
      String xstr=normalize(path);

      // TODO: instead of prepending /prefix/, we could do the search rooted at /prefix...
      Object o = xpath.evaluate(xstr, doc, type);
      return o;

    } catch (XPathExpressionException e) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,"Error in xpath:" + path +" for " + name,e);
    }
  }

  public Node getNode(String path, boolean errifMissing) {
    return getNode(path, doc, errifMissing);
  }

  public Node getUnsubstitutedNode(String path, boolean errIfMissing) {
    return getNode(path, origDoc, errIfMissing);
  }

  public Node getNode(String path, Document doc, boolean errIfMissing) {
    XPath xpath = xpathFactory.newXPath();
    String xstr = normalize(path);

    try {
      NodeList nodes = (NodeList)xpath.evaluate(xstr, doc, 
                                                XPathConstants.NODESET);
      if (nodes==null || 0 == nodes.getLength() ) {
        if (errIfMissing) {
          throw new RuntimeException(name + " missing "+path);
        } else {
          log.trace(name + " missing optional " + path);
          return null;
        }
      }
      if ( 1 < nodes.getLength() ) {
        throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
                                 name + " contains more than one value for config path: " + path);
      }
      Node nd = nodes.item(0);
      log.trace(name + ":" + path + "=" + nd);
      return nd;

    } catch (XPathExpressionException e) {
      SolrException.log(log,"Error in xpath",e);
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,"Error in xpath:" + xstr + " for " + name,e);
    } catch (SolrException e) {
      throw(e);
    } catch (Exception e) {
      SolrException.log(log,"Error in xpath",e);
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,"Error in xpath:" + xstr+ " for " + name,e);
    }
  }

  public NodeList getNodeList(String path, boolean errIfMissing) {
    XPath xpath = xpathFactory.newXPath();
    String xstr = normalize(path);

    try {
      NodeList nodeList = (NodeList)xpath.evaluate(xstr, doc, XPathConstants.NODESET);

      if (null == nodeList) {
        if (errIfMissing) {
          throw new RuntimeException(name + " missing "+path);
        } else {
          log.trace(name + " missing optional " + path);
          return null;
        }
      }

      log.trace(name + ":" + path + "=" + nodeList);
      return nodeList;

    } catch (XPathExpressionException e) {
      SolrException.log(log,"Error in xpath",e);
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,"Error in xpath:" + xstr + " for " + name,e);
    } catch (SolrException e) {
      throw(e);
    } catch (Exception e) {
      SolrException.log(log,"Error in xpath",e);
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,"Error in xpath:" + xstr+ " for " + name,e);
    }
  }

  /**
   * Returns the set of attributes on the given element that are not among the given knownAttributes,
   * or null if all attributes are known.
   */
  public Set<String> getUnknownAttributes(Element element, String... knownAttributes) {
    Set<String> knownAttributeSet = new HashSet<>(Arrays.asList(knownAttributes));
    Set<String> unknownAttributeSet = null;
    NamedNodeMap attributes = element.getAttributes();
    for (int i = 0 ; i < attributes.getLength() ; ++i) {
      final String attributeName = attributes.item(i).getNodeName();
      if ( ! knownAttributeSet.contains(attributeName)) {
        if (null == unknownAttributeSet) {
          unknownAttributeSet = new HashSet<>();
        }
        unknownAttributeSet.add(attributeName);
      }
    }
    return unknownAttributeSet;
  }

  /**
   * Logs an error and throws an exception if any of the element(s) at the given elementXpath
   * contains an attribute name that is not among knownAttributes. 
   */
  public void complainAboutUnknownAttributes(String elementXpath, String... knownAttributes) {
    SortedMap<String,SortedSet<String>> problems = new TreeMap<>();
    NodeList nodeList = getNodeList(elementXpath, false);
    for (int i = 0 ; i < nodeList.getLength() ; ++i) {
      Element element = (Element)nodeList.item(i);
      Set<String> unknownAttributes = getUnknownAttributes(element, knownAttributes);
      if (null != unknownAttributes) {
        String elementName = element.getNodeName();
        SortedSet<String> allUnknownAttributes = problems.get(elementName);
        if (null == allUnknownAttributes) {
          allUnknownAttributes = new TreeSet<>();
          problems.put(elementName, allUnknownAttributes);
        }
        allUnknownAttributes.addAll(unknownAttributes);
      }
    }
    if (problems.size() > 0) {
      StringBuilder message = new StringBuilder();
      for (Map.Entry<String,SortedSet<String>> entry : problems.entrySet()) {
        if (message.length() > 0) {
          message.append(", ");
        }
        message.append('<');
        message.append(entry.getKey());
        for (String attributeName : entry.getValue()) {
          message.append(' ');
          message.append(attributeName);
          message.append("=\"...\"");
        }
        message.append('>');
      }
      message.insert(0, "Unknown attribute(s) on element(s): ");
      String msg = message.toString();
      SolrException.log(log, msg);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg);
    }
  }

  public String getVal(String path, boolean errIfMissing) {
    Node nd = getNode(path,errIfMissing);
    if (nd==null) return null;

    String txt = DOMUtil.getText(nd);

    log.debug(name + ' '+path+'='+txt);
    return txt;
  }


  public String get(String path) {
    return getVal(path,true);
  }

  public String get(String path, String def) {
    String val = getVal(path, false);
    if (val == null || val.length() == 0) {
      return def;
    }
    return val;
  }

  public int getInt(String path) {
    return Integer.parseInt(getVal(path, true));
  }

  public int getInt(String path, int def) {
    String val = getVal(path, false);
    return val!=null ? Integer.parseInt(val) : def;
  }

  public boolean getBool(String path) {
    return Boolean.parseBoolean(getVal(path, true));
  }

  public boolean getBool(String path, boolean def) {
    String val = getVal(path, false);
    return val!=null ? Boolean.parseBoolean(val) : def;
  }

  public float getFloat(String path) {
    return Float.parseFloat(getVal(path, true));
  }

  public float getFloat(String path, float def) {
    String val = getVal(path, false);
    return val!=null ? Float.parseFloat(val) : def;
  }

  public double getDouble(String path){
     return Double.parseDouble(getVal(path, true));
   }

  public double getDouble(String path, double def) {
    String val = getVal(path, false);
    return val != null ? Double.parseDouble(val) : def;
  }

  /**If this config is loaded from zk the version is relevant other wise -1 is returned
   */
  public int getZnodeVersion(){
    return zkVersion;
  }

  public XmlConfigFile getOriginalConfig() {
    return new XmlConfigFile(loader, null, origDoc);
  }

}
