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

package org.apache.solr.core;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.DOMUtil;
import javax.xml.parsers.*;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.namespace.QName;
import java.io.*;
import java.util.List;
import java.util.logging.Logger;

/**
 * @version $Id$
 */
public class Config {
  public static final Logger log = Logger.getLogger(Config.class.getName());

  static final XPathFactory xpathFactory = XPathFactory.newInstance();

  private final Document doc;
  private final String prefix;
  private final String name;
  private final SolrResourceLoader loader;

  @Deprecated
  public Config(String name, InputStream is, String prefix) throws ParserConfigurationException, IOException, SAXException 
  {
    this( null, name, is, prefix );
  }

  /**
   * Builds a config from a resource name with no xpath prefix.
   * @param loader
   * @param name
   * @throws javax.xml.parsers.ParserConfigurationException
   * @throws java.io.IOException
   * @throws org.xml.sax.SAXException
   */
  public Config(SolrResourceLoader loader, String name) throws ParserConfigurationException, IOException, SAXException 
  {
    this( loader, name, null, null );
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
   * Consider passing a non-null 'name' parameter in all use-cases since it is used for logging & exception reporting.
   * </p>
   * @param loader the resource loader used to obtain an input stream if 'is' is null
   * @param name the resource name used if the input stream 'is' is null
   * @param is the resource as a stream
   * @param prefix an optional prefix that will be preprended to all non-absolute xpath expressions
   * @throws javax.xml.parsers.ParserConfigurationException
   * @throws java.io.IOException
   * @throws org.xml.sax.SAXException
   */
  public Config(SolrResourceLoader loader, String name, InputStream is, String prefix) throws ParserConfigurationException, IOException, SAXException 
  {
    if( loader == null ) {
      loader = new SolrResourceLoader( null );
    }
    this.loader = loader;
    this.name = name;
    this.prefix = (prefix != null && !prefix.endsWith("/"))? prefix + '/' : prefix;
    InputStream lis = is;
    try {
      if (lis == null) {
        lis = loader.openConfig(name);
      }
      javax.xml.parsers.DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      doc = builder.parse(lis);

    	DOMUtil.substituteSystemProperties(doc);
    } catch( SolrException e ){
    	SolrException.log(log,"Error in "+name,e);
    	throw e;
    } finally {
      // if this opens the resource, it also closes it
      if (lis != is)  lis.close();
    }
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


  public Object evaluate(String path, QName type) {
    XPath xpath = xpathFactory.newXPath();
    try {
      String xstr=normalize(path);

      // TODO: instead of prepending /prefix/, we could do the search rooted at /prefix...
      Object o = xpath.evaluate(xstr, doc, type);
      return o;

    } catch (XPathExpressionException e) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,"Error in xpath:" + path +" for " + name,e,false);
    }
  }

  public Node getNode(String path, boolean errIfMissing) {
   XPath xpath = xpathFactory.newXPath();
   Node nd = null;
   String xstr = normalize(path);

    try {
      nd = (Node)xpath.evaluate(xstr, doc, XPathConstants.NODE);

      if (nd==null) {
        if (errIfMissing) {
          throw new RuntimeException(name + " missing "+path);
        } else {
          log.fine(name + " missing optional " + path);
          return null;
        }
      }

      log.finest(name + ":" + path + "=" + nd);
      return nd;

    } catch (XPathExpressionException e) {
      SolrException.log(log,"Error in xpath",e);
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,"Error in xpath:" + xstr + " for " + name,e,false);
    } catch (SolrException e) {
      throw(e);
    } catch (Throwable e) {
      SolrException.log(log,"Error in xpath",e);
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,"Error in xpath:" + xstr+ " for " + name,e,false);
    }
  }

  public String getVal(String path, boolean errIfMissing) {
    Node nd = getNode(path,errIfMissing);
    if (nd==null) return null;

    String txt = DOMUtil.getText(nd);

    log.fine(name + ' '+path+'='+txt);
    return txt;

    /******
    short typ = nd.getNodeType();
    if (typ==Node.ATTRIBUTE_NODE || typ==Node.TEXT_NODE) {
      return nd.getNodeValue();
    }
    return nd.getTextContent();
    ******/
  }


  public String get(String path) {
    return getVal(path,true);
  }

  public String get(String path, String def) {
    String val = getVal(path, false);
    return val!=null ? val : def;
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
     return val!=null ? Double.parseDouble(val) : def;
   }

  // The following functions were moved to ResourceLoader
  //-----------------------------------------------------------------------------
  
  @Deprecated
  public String getConfigDir() {
    return loader.getConfigDir();
  }

  @Deprecated
  public InputStream openResource(String resource) {
    return loader.openResource(resource);
  }

  @Deprecated
  public List<String> getLines(String resource) throws IOException {
    return loader.getLines(resource);
  }

  @Deprecated
  public Class findClass(String cname, String... subpackages) {
    return loader.findClass(cname, subpackages);
  }

  @Deprecated
  public Object newInstance(String cname, String ... subpackages) {
    return loader.newInstance(cname, subpackages);
  }
  
  @Deprecated
  public String getInstanceDir() {
    return loader.getInstanceDir();
  }
}
