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

import net.sf.saxon.Configuration;
import net.sf.saxon.dom.DocumentOverNodeInfo;
import net.sf.saxon.event.Sender;
import net.sf.saxon.lib.ParseOptions;
import net.sf.saxon.om.NamePool;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.tree.tiny.TinyDocumentImpl;
import net.sf.saxon.xpath.XPathFactoryImpl;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.ParWork;
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

import javax.xml.namespace.QName;
import javax.xml.transform.sax.SAXSource;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
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

/**
 * Wrapper around an XML DOM object to provide convenient accessors to it.  Intended for XML config files.
 */
public class XmlConfigFile { // formerly simply "Config"
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final XMLErrorLogger xmllog = new XMLErrorLogger(log);

  protected final static ThreadLocal<XPath> THREAD_LOCAL_XPATH = new ThreadLocal<>();

  public static final XPathFactoryImpl xpathFactory = new XPathFactoryImpl();

  public static Configuration conf = null;

  private static NamePool pool = null;

  static  {
    try {
      conf = Configuration.newConfiguration();
      conf.setValidation(false);
      conf.setXIncludeAware(true);
      conf.setExpandAttributeDefaults(true);
      pool = new NamePool();
      conf.setNamePool(pool);

      xpathFactory.setConfiguration(conf);
    } catch (Exception e) {
      log.error("", e);
    }
  }

  protected final String prefix;
  private final String name;
  private final SolrResourceLoader loader;

  private Document doc;
  private final Properties substituteProperties;
  private final TinyDocumentImpl tree;
  private int zkVersion = -1;

  public static XPath getXpath() {
    XPath xPath = THREAD_LOCAL_XPATH.get();
    if (xPath == null) {
      xPath = XmlConfigFile.xpathFactory.newXPath();
      THREAD_LOCAL_XPATH.set(xPath);
    }
    return xPath;
  }

  /**
   * Builds a config from a resource name with no xpath prefix.  Does no property substitution.
   */
  public XmlConfigFile(SolrResourceLoader loader, String name)
      throws IOException {
    this( loader, name, null, null);
  }

  /**
   * Builds a config.  Does no property substitution.
   */
  public XmlConfigFile(SolrResourceLoader loader, String name, InputSource is, String prefix)
      throws IOException {
    this(loader, name, is, prefix, null);
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
   * @param substituteProps optional property substitution
   */
  public XmlConfigFile(SolrResourceLoader loader, String name, InputSource is, String prefix, Properties substituteProps)
      throws  IOException {
    if( loader == null ) {
      loader = new SolrResourceLoader(SolrPaths.locateSolrHome());
    }
    this.loader = loader;
    this.name = name;
    this.prefix = (prefix != null && !prefix.endsWith("/"))? prefix + '/' : prefix;

      if (is == null) {
        if (name == null || name.length() == 0) {
          throw new IllegalArgumentException("Null or empty name:" + name);
        }
        InputStream in = loader.openResource(name);
        if (in instanceof ZkSolrResourceLoader.ZkByteArrayInputStream) {
          zkVersion = ((ZkSolrResourceLoader.ZkByteArrayInputStream) in).getStat().getVersion();
          log.debug("loaded config {} with version {} ",name,zkVersion);
        }
        is = new InputSource(in);
        is.setSystemId(SystemIdResolver.createSystemIdFromResourceName(name));
      }

    try {
      SAXSource source = new SAXSource(is);
      Configuration conf2 = Configuration.newConfiguration();
      conf2.setValidation(false);
      conf2.setXIncludeAware(true);
      conf2.setExpandAttributeDefaults(true);
      conf2.setNamePool(pool);
      conf2.setDocumentNumberAllocator(conf.getDocumentNumberAllocator());

      SolrTinyBuilder builder = new SolrTinyBuilder(conf2.makePipelineConfiguration(), substituteProps);
      builder.setStatistics(conf2.getTreeStatistics().SOURCE_DOCUMENT_STATISTICS);

      ParseOptions parseOptions = new ParseOptions();
      if (is.getSystemId() != null) {
        parseOptions.setEntityResolver(loader.getSysIdResolver());
      }
      parseOptions.setXIncludeAware(true);

      parseOptions.setPleaseCloseAfterUse(true);
      parseOptions.setSchemaValidationMode(0);

      Sender.send(source, builder, parseOptions);
      TinyDocumentImpl docTree = (TinyDocumentImpl) builder.getCurrentRoot();
      builder.reset();

      this.tree = docTree;
      doc = (Document) DocumentOverNodeInfo.wrap(docTree);

      this.substituteProperties = substituteProps;
    } catch ( XPathException e) {
      throw new RuntimeException(e);
    } finally {
      // some XML parsers are broken and don't close the byte stream (but they should according to spec)
      ParWork.close(is.getByteStream());
    }

  }

    /*
     * Assert that assertCondition is true.
     * If not, prints reason as log warning.
     * If failCondition is true, then throw exception instead of warning
     */
    public static void assertWarnOrFail (String reason,boolean assertCondition,
    boolean failCondition){
      if (assertCondition) {
        return;
      } else if (failCondition) {
        throw new SolrException(SolrException.ErrorCode.FORBIDDEN, reason);
      } else {
        log.warn(reason);
      }
    }

    /** Returns non-null props to substitute.  Param is the base/default set, also non-null. */
    protected Properties getSubstituteProperties () {
      return this.substituteProperties;
    }

    /**
     * @since solr 1.3
     */
    public SolrResourceLoader getResourceLoader () {
      return loader;
    }

    /**
     * @since solr 1.3
     */
    public String getResourceName () {
      return name;
    }

    public String getName () {
      return name;
    }

    public Document getDocument () {
      return doc;
    }

  public NodeInfo getTreee () {
    return tree;
  }

    String normalize(String path){
      return (prefix == null || path.startsWith("/")) ? path : prefix + path;
    }

    public Object evaluate (String path, QName type){
      try {
        String xstr = normalize(path);

        // TODO: instead of prepending /prefix/, we could do the search rooted at /prefix...
        Object o = getXpath().evaluate(xstr, doc, type);
        return o;

      } catch (XPathExpressionException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Error in xpath:" + path + " for " + name, e);
      }
    }

    public String getPrefix() {
      return prefix;
    }

    // nocommit
    public Node getNode (String expression, boolean errifMissing){
      String path = normalize(expression);
      try {
        return getNode(getXpath().compile(path), path, doc, errifMissing);
      } catch (XPathExpressionException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
    }

    public Node getNode (XPathExpression expression,  String path, boolean errifMissing){
      return getNode(expression, path, doc, errifMissing);
    }

    public Node getNode (XPathExpression expression, String path, Document doc, boolean errIfMissing){
      //String xstr = normalize(path);

      try {
        NodeList nodes = (NodeList) expression
            .evaluate(doc, XPathConstants.NODESET);
        if (nodes == null || 0 == nodes.getLength()) {
          if (errIfMissing) {
            throw new RuntimeException(name + " missing " + path);
          } else {
            log.trace("{} missing optional {}", name, path);
            return null;
          }
        }
        if (1 < nodes.getLength()) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              name + " contains more than one value for config path: " + path);
        }
        Node nd = nodes.item(0);
        log.trace("{}:{}={}", name, expression, nd);
        return nd;

      } catch (XPathExpressionException e) {
        SolrException.log(log, "Error in xpath", e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Error in xpath:" + path + " for " + name, e);
      } catch (SolrException e) {
        throw (e);
      } catch (Exception e) {
        ParWork.propegateInterrupt(e);
        SolrException.log(log, "Error in xpath", e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Error in xpath:" + path + " for " + name, e);
      }
    }

    // TODO: more precompiled expressions
    public NodeList getNodeList (String path, boolean errIfMissing){
      String xstr = normalize(path);

      try {
        NodeList nodeList = (NodeList) getXpath()
            .evaluate(xstr, doc, XPathConstants.NODESET);

        if (null == nodeList) {
          if (errIfMissing) {
            throw new RuntimeException(name + " missing " + path);
          } else {
            log.trace("{} missing optional {}", name, path);
            return null;
          }
        }

        log.trace("{}:{}={}", name, path, nodeList);
        return nodeList;

      } catch (XPathExpressionException e) {
        SolrException.log(log, "Error in xpath", e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Error in xpath:" + xstr + " for " + name, e);
      } catch (SolrException e) {
        throw (e);
      } catch (Exception e) {
        ParWork.propegateInterrupt(e);
        SolrException.log(log, "Error in xpath", e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Error in xpath:" + xstr + " for " + name, e);
      }
    }

    /**
     * Returns the set of attributes on the given element that are not among the given knownAttributes,
     * or null if all attributes are known.
     */
    public Set<String> getUnknownAttributes (Element element, String...
    knownAttributes){
      Set<String> knownAttributeSet = new HashSet<>(
          Arrays.asList(knownAttributes));
      Set<String> unknownAttributeSet = null;
      NamedNodeMap attributes = element.getAttributes();
      for (int i = 0; i < attributes.getLength(); ++i) {
        final String attributeName = attributes.item(i).getNodeName();
        if (!knownAttributeSet.contains(attributeName)) {
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
    public void complainAboutUnknownAttributes (String elementXpath, String...
    knownAttributes){
      SortedMap<String,SortedSet<String>> problems = new TreeMap<>();
      NodeList nodeList = getNodeList(elementXpath, false);
      for (int i = 0; i < nodeList.getLength(); ++i) {
        Element element = (Element) nodeList.item(i);
        Set<String> unknownAttributes = getUnknownAttributes(element,
            knownAttributes);
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

    // nocommit
    public String getVal (String expression, boolean errIfMissing){
      String xstr = normalize(expression);
      try {
        return getVal(getXpath().compile(xstr), expression, errIfMissing);
      } catch (XPathExpressionException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
    }

    public String getVal (XPathExpression expression, String path, boolean errIfMissing){
      Node nd = getNode(expression, path, errIfMissing);
      if (nd == null) return null;

      String txt = DOMUtil.getText(nd);

      log.debug("{} {}={}", name, expression, txt);
      return txt;
    }

    public String get (XPathExpression expression, String path){
      return getVal(expression, path, true);
    }

    public String get (XPathExpression expression,  String path, String def){
      String val = getVal(expression, path, false);
      if (val == null || val.length() == 0) {
        return def;
      }
      return val;
    }

    public int getInt (XPathExpression expression, String path){
      return Integer.parseInt(getVal(expression, path, true));
    }

    public int getInt (XPathExpression expression,  String path, int def){
      String val = getVal(expression, path, false);
      return val != null ? Integer.parseInt(val) : def;
    }

    public boolean getBool (XPathExpression expression, String path){
      return Boolean.parseBoolean(getVal(expression, path, true));
    }

    public boolean getBool (XPathExpression expression, String path, boolean def){
      String val = getVal(expression, path, false);
      return val != null ? Boolean.parseBoolean(val) : def;
    }

    public float getFloat (XPathExpression expression, String path){
      return Float.parseFloat(getVal(expression, path, true));
    }

    public float getFloat (XPathExpression expression, String path, float def){
      String val = getVal(expression, path, false);
      return val != null ? Float.parseFloat(val) : def;
    }

    public double getDouble (XPathExpression expression, String path){
      return Double.parseDouble(getVal(expression, path, true));
    }

    public double getDouble (XPathExpression expression, String path, double def){
      String val = getVal(expression, path, false);
      return val != null ? Double.parseDouble(val) : def;
    }

    /**If this config is loaded from zk the version is relevant other wise -1 is returned
     */
    public int getZnodeVersion () {
      return zkVersion;
    }

  }
