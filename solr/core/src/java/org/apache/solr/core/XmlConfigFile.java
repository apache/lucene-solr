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

import net.sf.saxon.event.PipelineConfiguration;
import net.sf.saxon.event.Sender;
import net.sf.saxon.lib.ParseOptions;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.tree.tiny.TinyDocumentImpl;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.StopWatch;
import org.apache.solr.common.util.XMLErrorLogger;
import org.apache.solr.util.DOMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
// MRM MRM TODO: - figure out where to put and what to do with the config files that were in _default/lang
public class XmlConfigFile { // formerly simply "Config"
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final XMLErrorLogger xmllog = new XMLErrorLogger(log);

  protected final String prefix;
  private final String name;
  protected final SolrResourceLoader loader;

  private final Properties substituteProperties;
  protected final TinyDocumentImpl tree;

  private int zkVersion = -1;

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
    this.loader = loader;
    this.name = name;
    this.prefix = (prefix != null && !prefix.endsWith("/")) ? prefix + '/' : prefix;

    StopWatch parseXmlFile = StopWatch.getStopWatch(name + "-parseXmlFile");
    if (is == null) {
      if (name == null || name.length() == 0) {
        throw new IllegalArgumentException("Null or empty name:" + name);
      }

      InputStream in = null;

      in = loader.openResource(name);

      if (in instanceof ZkSolrResourceLoader.ZkByteArrayInputStream) {

        zkVersion = ((ZkSolrResourceLoader.ZkByteArrayInputStream) in).getStat().getVersion();
        if (log.isDebugEnabled()) {
          log.debug("loaded config {} with version {} ", name, zkVersion);
        }
      }

      is = new InputSource(in);
      //   is.setSystemId(SystemIdResolver.createSystemIdFromResourceName(name));
    }

    try {
      this.tree = parseXml(loader, is, substituteProps);

      this.substituteProperties = substituteProps;
    } catch (XPathException e) {
      throw new RuntimeException(e);
    } finally {
      // some XML parsers are broken and don't close the byte stream (but they should according to spec)
      IOUtils.closeQuietly(is.getByteStream());
      parseXmlFile.done();
    }

  }

  public static TinyDocumentImpl parseXml(SolrResourceLoader loader, InputSource is, Properties substituteProps) throws XPathException {
    SAXSource source = new SAXSource(is);
    source.setXMLReader(loader.getXmlReader());
    PipelineConfiguration plc = loader.getConf().makePipelineConfiguration();
    //      if (is.getSystemId() != null) {
    //     plc.setURIResolver(loader.getSysIdResolver().asURIResolver());
    //      }
    TinyDocumentImpl docTree;
    SolrTinyBuilder builder = new SolrTinyBuilder(plc, substituteProps);
    try {
      //builder.setStatistics(conf2.getTreeStatistics().SOURCE_DOCUMENT_STATISTICS);
      builder.open();
      ParseOptions po = plc.getParseOptions();
      if (is.getSystemId() != null) {
        po.setEntityResolver(loader.getSysIdResolver());
      } else {
        po.setEntityResolver(null);
      }
      // Set via conf already
      //   po.setXIncludeAware(true);
      //  po.setCheckEntityReferences(false);
      // po.setExpandAttributeDefaults(false);
      po.setPleaseCloseAfterUse(true);
      Sender.send(source, builder, po);
      docTree = (TinyDocumentImpl) builder.getCurrentRoot();
    } catch (Exception e) {
      log.error("Exception handling xml doc", e);
      throw e;
    }  finally {
      //builder.close();
      //builder.reset();
    }

    return docTree;
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

  public TinyDocumentImpl getTree() {
    return tree;
  }

    String normalize(String path){
      return (prefix == null || path.startsWith("/")) ? path : prefix + path;
    }

//    public Object evaluate (String path, QName type){
//      try {
//        String xstr = normalize(path);
//
//        // TODO: instead of prepending /prefix/, we could do the search rooted at /prefix...
//        Object o = getXpath().evaluate(xstr, doc, type);
//        return o;
//
//      } catch (XPathExpressionException e) {
//        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
//            "Error in xpath:" + path + " for " + name, e);
//      }
//    }

  public Object evaluate(TinyDocumentImpl tree, String path, QName type) {
    try {
      String xstr = normalize(path);
      XPath xPath = getResourceLoader().getXPath();
      // TODO: instead of prepending /prefix/, we could do the search rooted at /prefix...
      Object o = xPath.evaluate(xstr, tree, type);
      return o;

    } catch (XPathExpressionException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error in xpath:" + path + " for " + name, e);
    }
  }

    public String getPrefix() {
      return prefix;
    }

    public NodeInfo getNode(String expression, boolean errifMissing){
      String path = normalize(expression);
      try {
        XPath xPath = getResourceLoader().getXPath();
        return getNode(xPath.compile(path), path, tree, errifMissing);
      } catch (XPathExpressionException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
    }

    public NodeInfo getNode(XPathExpression expression,  String path, boolean errifMissing){
      return getNode(expression, path, tree, errifMissing);
    }

    public NodeInfo getNode(XPathExpression expression, String path, TinyDocumentImpl doc, boolean errIfMissing){
//      if (expression == null) {
//        throw new IllegalArgumentException("null expression");
//      }
//      if (doc == null) {
//        throw new IllegalArgumentException("null doc");
//      }
      //String xstr = normalize(path);

      try {
        ArrayList<NodeInfo> nodes = (ArrayList) expression
            .evaluate(doc, XPathConstants.NODESET);
        if (nodes == null || 0 == nodes.size()) {
          if (errIfMissing) {
            throw new RuntimeException(name + " missing " + path);
          } else {
            log.trace("{} missing optional {}", name, path);
            return null;
          }
        }
        if (1 < nodes.size()) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              name + " contains more than one value for config path: " + path);
        }
        NodeInfo nd = nodes.get(0);
        if (log.isTraceEnabled()) log.trace("{}:{}={}", name, expression, nd);
        return nd;

      } catch (XPathExpressionException e) {
        SolrException.log(log, "Error in xpath", e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Error in xpath:" + path + " for " + name, e);
      } catch (SolrException e) {
        throw (e);
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        SolrException.log(log, "Error in xpath", e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Error in xpath:" + path + " for " + name, e);
      }
    }

    // TODO: more precompiled expressions
    public List<NodeInfo> getNodeList (String path, boolean errIfMissing){
      String xstr = normalize(path);

      try {
        XPath xPath = getResourceLoader().getXPath();
        ArrayList nodeList = (ArrayList) xPath
            .evaluate(xstr, tree, XPathConstants.NODESET);

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
        ParWork.propagateInterrupt(e);
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
      List<NodeInfo> nodeList = getNodeList(elementXpath, false);
      for (int i = 0; i < nodeList.size(); ++i) {
        Element element = (Element) nodeList.get(i);
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

    // MRM TODO: expression not precompiled
    public String getVal (String expression, boolean errIfMissing){
      String xstr = normalize(expression);
      try {
        XPath xPath = getResourceLoader().getXPath();
        return getVal(xPath.compile(xstr), expression, errIfMissing);
      } catch (XPathExpressionException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
    }

    public String getVal (XPathExpression expression, String path, boolean errIfMissing){
      NodeInfo nd = getNode(expression, path, errIfMissing);
      if (nd == null) return null;

      String txt = DOMUtil.getText(nd);

      if (log.isDebugEnabled()) log.debug("{} {}={}", name, expression, txt);
      return txt;
    }

    public String get(XPathExpression expression, String path){
      return getVal(expression, path, true);
    }

    public String get(XPathExpression expression,  String path, String def){
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
      if (val != null && val.equals("-1")) {
        return def;
      }
      return val != null ? Integer.parseInt(val) : def;
    }

    public boolean getBool (XPathExpression expression, String path){
      return Boolean.parseBoolean(getVal(expression, path, true));
    }

    public boolean getBool (XPathExpression expression, String path, boolean def){
      String val = getVal(expression, path, false);
      if (val != null && val.equals("-1")) {
        return def;
      }
      return val != null ? Boolean.parseBoolean(val) : def;
    }

    public float getFloat (XPathExpression expression, String path){
      return Float.parseFloat(getVal(expression, path, true));
    }

    public float getFloat (XPathExpression expression, String path, float def){
      String val = getVal(expression, path, false);
      if (val != null && val.equals("-1")) {
        return def;
      }
      return val != null ? Float.parseFloat(val) : def;
    }

    public double getDouble (XPathExpression expression, String path){
      return Double.parseDouble(getVal(expression, path, true));
    }

    public double getDouble (XPathExpression expression, String path, double def){
      String val = getVal(expression, path, false);
      if (val != null && val.equals("-1")) {
        return def;
      }
      return val != null ? Double.parseDouble(val) : def;
    }

    /**If this config is loaded from zk the version is relevant other wise -1 is returned
     */
    public int getZnodeVersion () {
      return zkVersion;
    }


}
