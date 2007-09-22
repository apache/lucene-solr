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
import org.apache.solr.core.SolrCore;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.NoInitialContextException;
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
import java.net.URLClassLoader;
import java.net.URL;
import java.net.MalformedURLException;

/**
 * @version $Id$
 */
public class Config {
  public static final Logger log = Logger.getLogger(SolrCore.class.getName());

  static final XPathFactory xpathFactory = XPathFactory.newInstance();

  private final String instanceDir; // solr home directory
  private final Document doc;
  private final String prefix;
  private final String name;

  @Deprecated
  public Config(String name, InputStream is, String prefix) throws ParserConfigurationException, IOException, SAXException 
  {
    this( Config.locateInstanceDir(), name, is, prefix );
  }

  public Config(String instanceDir, String name) throws ParserConfigurationException, IOException, SAXException 
  {
    this( instanceDir, name, null, null );
  }
  
  public Config(String instanceDir, String name, InputStream is, String prefix) throws ParserConfigurationException, IOException, SAXException 
  {
    if( instanceDir == null ) {
      instanceDir = Config.locateInstanceDir();
    }
    
    this.instanceDir = normalizeDir(instanceDir);
    log.info("Solr home set to '" + instanceDir + "'");
    classLoader = null;
    
    this.name = name;
    this.prefix = prefix;
    if (prefix!=null && !prefix.endsWith("/")) prefix += '/';
    InputStream lis = is;
    try {
      if (lis == null)
        lis = openResource(name);
      
      javax.xml.parsers.DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      doc = builder.parse(lis);

    	DOMUtil.substituteSystemProperties(doc);
    } catch( SolrException e ){
    	SolrException.log(log,"Error in "+name,e);
    	throw e;
    } finally {
      // if this opens the resource, it also closes it
      if (lis != is)
        lis.close();
    }
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


  // The directory where solr will look for config files by default.
  // defaults to "./solr/conf/"
  public String getConfigDir() {
    return instanceDir + "conf/";
  }
  
  public InputStream openResource(String resource) {
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
      
      ClassLoader loader = getClassLoader();
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
   * Accesses a resource by name and returns the (non comment) lines
   * containing data.
   *
   * <p>
   * A comment line is any line that starts with the character "#"
   * </p>
   *
   * @param resource
   * @return a list of non-blank non-comment lines with whitespace trimmed
   * from front and back.
   * @throws IOException
   */
  public List<String> getLines(String resource) throws IOException {
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
  
  //
  // classloader related functions
  //

  private static final String project = "solr";
  private static final String base = "org.apache" + "." + project;
  private static final String[] packages = {"","analysis.","schema.","handler.","search.","update.","core.","request.","update.processor.","util."};

  public Class findClass(String cname, String... subpackages) {
    ClassLoader loader = getClassLoader();
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

      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "Error loading class '" + cname + "'", e, false);
    }
  }

  public Object newInstance(String cname, String... subpackages) {
    Class clazz = findClass(cname,subpackages);
    if( clazz == null ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
          "Can not find class: "+cname + " in " + getClassLoader(), false);
    }
    try {
      return clazz.newInstance();
    } 
    catch (Exception e) {
      e.printStackTrace();
      
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
          "Error instantiating class: '" + clazz.getName()+"'", e, false );
    }
  }

  private static String normalizeDir(String path) {
    if (path==null) return null;
    if ( !(path.endsWith("/") || path.endsWith("\\")) ) {
      path+='/';
    }
    return path;
  }

  public String getInstanceDir() {
    return instanceDir;
  }

  public static String locateInstanceDir() {
    String home = null;
    // Try JNDI
    try {
      Context c = new InitialContext();
      home = (String)c.lookup("java:comp/env/solr/home");
      log.info("Using JNDI solr.home: "+home );
    } catch (NoInitialContextException e) {
      log.info("JNDI not configured for Solr (NoInitialContextEx)");
    } catch (NamingException e) {
      log.info("No /solr/home in JNDI");
    } catch( RuntimeException ex ) {
      log.warning("Odd RuntimeException while testing for JNDI: " + ex.getMessage());
    } 
    
    // Now try system property
    if( home == null ) {
      String prop = project + ".solr.home";
      home = normalizeDir(System.getProperty(prop));
      if( home != null ) {
        log.info("using system property solr.home: " + home );
      }
    }
    
    // if all else fails, try 
    if( home == null ) {
      home = project + '/';
      log.info("Solr home defaulted to '" + home + "' (could not find system property or JNDI)");
    }
    return normalizeDir( home );
  }

  /** Singleton classloader loading resources specified in any configs */
  private ClassLoader classLoader = null;

  /**
   * Returns the singleton classloader to be use when loading resources
   * specified in any configs.
   *
   * <p>
   * This loader will delegate to the context classloader when possible,
   * otherwise it will attempt to resolve resources useing any jar files
   * found in the "lib/" directory in the "Solr Home" directory.
   * <p>
   */
  private ClassLoader getClassLoader() {
    if (null == classLoader) {
      // NB5.5/win32/1.5_10: need to go thru local var or classLoader is not set!
      ClassLoader loader = Thread.currentThread().getContextClassLoader();

      File f = new File(instanceDir + "lib/");
      if (f.canRead() && f.isDirectory()) {
        File[] jarFiles = f.listFiles();
        URL[] jars = new URL[jarFiles.length];
        try {
          for (int j = 0; j < jarFiles.length; j++) {
            jars[j] = jarFiles[j].toURI().toURL();
            log.info("Adding '" + jars[j].toString() + "' to Solr classloader");
          }
          loader = URLClassLoader.newInstance(jars, loader);
        } catch (MalformedURLException e) {
          SolrException.log(log,"Can't construct solr lib class loader", e);
        }
      }
      classLoader = loader;
    }
    return classLoader;
  }

  /**
   * @return the XML filename
   */
  public String getName() {
    return name;
  }
}
