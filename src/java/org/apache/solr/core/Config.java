/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrException;

import javax.xml.parsers.*;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.namespace.QName;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author yonik
 * @version $Id$
 */
public class Config {
  public static final Logger log = Logger.getLogger(SolrCore.class.getName());

  static final XPathFactory xpathFactory = XPathFactory.newInstance();

  private Document doc;
  private String prefix;
  private String name;

  public Config(String name, InputStream is, String prefix) throws ParserConfigurationException, IOException, SAXException {
    this.name = name;
    this.prefix = prefix;
    if (prefix!=null && !prefix.endsWith("/")) prefix += '/';

    javax.xml.parsers.DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    doc = builder.parse(is);
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
      throw new SolrException(500,"Error in xpath:" + path +" for " + name,e,false);
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
      throw new SolrException(500,"Error in xpath:" + xstr + " for " + name,e,false);
    } catch (SolrException e) {
      throw(e);
    } catch (Throwable e) {
      SolrException.log(log,"Error in xpath",e);
      throw new SolrException(500,"Error in xpath:" + xstr+ " for " + name,e,false);
    }
  }

  public String getVal(String path, boolean errIfMissing) {
    Node nd = getNode(path,errIfMissing);
    if (nd==null) return null;

    // should do the right thing for both attributes and elements.
    // Oops, when running in Resin, I get an unsupported operation
    // exception... need to use Sun default (apache)
    String txt = nd.getTextContent();
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


  //
  // classloader related functions
  //

  private static final String project = "solr";
  private static final String base = "org.apache" + "." + project;
  private static final String[] packages = {"","analysis.","schema.","search.","update.","core.","request.","util."};

  public static Class findClass(String cname, String... subpackages) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    if (subpackages.length==0) subpackages = packages;

    // first try cname == full name
    try {
      return Class.forName(cname, true, loader);
    } catch (ClassNotFoundException e) {
      String newName=cname;
      if (newName.startsWith(project)) {
        newName = cname.substring(project.length()+1);
      }
      for (String subpackage : subpackages) {
        try {
          String name = base + '.' + subpackage + newName;
          log.finest("Trying class name " + name);
          return Class.forName(name, true, loader);
        } catch (ClassNotFoundException e1) {
          // ignore... assume first exception is best.
        }
      }

      throw new SolrException(500, "Error loading class '" + cname + "'", e, false);
    }
  }

  public static Object newInstance(String cname, String... subpackages) {
    Class clazz = findClass(cname,subpackages);
    try {
      return clazz.newInstance();
    } catch (Exception e) {
      throw new SolrException(500,"Error instantiating class " + clazz, e, false);
    }
  }

  private static String instanceDir; // solr home directory
  private static String normalizeDir(String path) {
    if (path==null) return null;
    if ( !(path.endsWith("/") || path.endsWith("\\")) ) {
      path+='/';
    }
    return path;
  }

  public static void setInstanceDir(String dir) {
    instanceDir = normalizeDir(dir);
    log.info("Solr home set to '" + instanceDir + "'");
  }

  public static String getInstanceDir() {
    if (instanceDir==null) {
      String prop = project + ".solr.home";
      instanceDir = normalizeDir(System.getProperty(prop));
      if (instanceDir==null) {
        instanceDir=project + '/';
        log.info("Solr home defaulted to '" + instanceDir + "'");
      } else {
        log.info("Solr home set to '" + instanceDir + "' from system property " + prop);
      }
    }
    return instanceDir;
  }

  // The directory where solr will look for config files by default.
  // defaults to "./solr/conf/"
  static String getConfigDir() {
    return getInstanceDir() + "conf/";
  }

  public static InputStream openResource(String resource) {
    InputStream is=null;

    try {
      File f = new File(resource);
      if (!f.isAbsolute()) {
        // try $CWD/solrconf/
        f = new File(getConfigDir() + resource);
      }
      if (f.isFile() && f.canRead()) {
        return new FileInputStream(f);
      } else {
        // try $CWD
        f = new File(resource);
        if (f.isFile() && f.canRead()) {
          return new FileInputStream(f);
        }
      }

      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      is = loader.getResourceAsStream(resource);
    } catch (Exception e) {
      throw new RuntimeException("Error opening " + resource, e);
    }
    if (is==null) {
      throw new RuntimeException("Can't find resource '" + resource + "' in classpath or '" + getConfigDir() + "', cwd="+System.getProperty("user.dir"));
    }
    return is;
  }

  /**
   * Returns a list of non-blank non-comment lines with whitespace trimmed from front and back.
   * @param resource
   * @return
   * @throws IOException
   */
  public static List<String> getLines(String resource) throws IOException {
    BufferedReader input = null;
    try {
      // todo - allow configurable charset?
      input = new BufferedReader(new InputStreamReader(openResource(resource), "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    ArrayList<String> lines = new ArrayList<String>();
    for (String word=null; (word=input.readLine())!=null;) {
      // skip comments
      if (word.startsWith("#")) continue;
      word=word.trim();
      // skip blank lines
      if (word.length()==0) continue;
      lines.add(word);
    }
    return lines;
  }


}
