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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.NoInitialContextException;

import org.apache.solr.analysis.TokenFilterFactory;
import org.apache.solr.analysis.TokenizerFactory;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.QueryResponseWriter;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.schema.FieldType;
import org.apache.solr.util.plugin.ResourceLoaderAware;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * @since solr 1.3
 */ 
public class SolrResourceLoader implements ResourceLoader
{
  public static final Logger log = Logger.getLogger(SolrResourceLoader.class.getName());

  static final String project = "solr";
  static final String base = "org.apache" + "." + project;
  static final String[] packages = {"","analysis.","schema.","handler.","search.","update.","core.","request.","update.processor.","util."};

  private final ClassLoader classLoader;
  private final String instanceDir;
  
  private final List<SolrCoreAware> waitingForCore = new ArrayList<SolrCoreAware>();
  private final List<ResourceLoaderAware> waitingForResources = new ArrayList<ResourceLoaderAware>();

  /**
   * <p>
   * This loader will delegate to the context classloader when possible,
   * otherwise it will attempt to resolve resources using any jar files
   * found in the "lib/" directory in the specified instance directory.
   * If the instance directory is not specified (=null), SolrResourceLoader#locateInstanceDir will provide one.
   * <p>
   */
  public SolrResourceLoader( String instanceDir, ClassLoader parent )
  {
    if( instanceDir == null ) {
      this.instanceDir = SolrResourceLoader.locateInstanceDir();
    } else{
      this.instanceDir = normalizeDir(instanceDir);
    }
    log.info("Solr home set to '" + this.instanceDir + "'");
    this.classLoader = createClassLoader(new File(this.instanceDir + "lib/"), parent);
  }
    
  static ClassLoader createClassLoader(File f, ClassLoader loader) {
    if( loader == null ) {
      loader = Thread.currentThread().getContextClassLoader();
    }
    if (f.canRead() && f.isDirectory()) {
      File[] jarFiles = f.listFiles();
      URL[] jars = new URL[jarFiles.length];
      try {
        for (int j = 0; j < jarFiles.length; j++) {
          jars[j] = jarFiles[j].toURI().toURL();
          log.info("Adding '" + jars[j].toString() + "' to Solr classloader");
        }
        return URLClassLoader.newInstance(jars, loader);
      } catch (MalformedURLException e) {
        SolrException.log(log,"Can't construct solr lib class loader", e);
      }
    }
    log.info("Reusing parent classloader");
    return loader;
  }

  public SolrResourceLoader( String instanceDir )
  {
    this( instanceDir, null );
  }
  
  /** Ensures a directory name allways ends with a '/'. */
  public  static String normalizeDir(String path) {
    return ( path != null && (!(path.endsWith("/") || path.endsWith("\\"))) )? path + '/' : path;
  }

  public String getConfigDir() {
    return instanceDir + "conf/";
  }

  /** Opens a schema resource by its name.
   * Override this method to customize loading schema resources.
   *@return the stream for the named schema
   */
  public InputStream openSchema(String name) {
    return openResource(name);
  }
  
  /** Opens a config resource by its name.
   * Override this method to customize loading config resources.
   *@return the stream for the named configuration
   */
  public InputStream openConfig(String name) {
    return openResource(name);
  }
  
  /** Opens any resource by its name.
   * By default, this will look in multiple locations to load the resource:
   * $configDir/$resource (if resource is not absolute)
   * $CWD/$resource
   * otherwise, it will look for it in any jar accessible through the class loader.
   * Override this method to customize loading resources.
   *@return the stream for the named resource
   */
  public InputStream openResource(String resource) {
    InputStream is=null;
    try {
      File f0 = new File(resource);
      File f = f0;
      if (!f.isAbsolute()) {
        // try $CWD/$configDir/$resource
        f = new File(getConfigDir() + resource);
      }
      if (f.isFile() && f.canRead()) {
        return new FileInputStream(f);
      } else if (f != f0) { // no success with $CWD/$configDir/$resource
        if (f0.isFile() && f0.canRead())
          return new FileInputStream(f0);
      }
      // delegate to the class loader (looking into $INSTANCE_DIR/lib jars)
      is = classLoader.getResourceAsStream(resource);
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
      // TODO - allow configurable charset?
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

  public Class findClass(String cname, String... subpackages) {
    if (subpackages.length==0) subpackages = packages;
  
    // first try cname == full name
    try {
      return Class.forName(cname, true, classLoader);
    } catch (ClassNotFoundException e) {
      String newName=cname;
      if (newName.startsWith(project)) {
        newName = cname.substring(project.length()+1);
      }
      for (String subpackage : subpackages) {
        try {
          String name = base + '.' + subpackage + newName;
          log.finest("Trying class name " + name);
          return Class.forName(name, true, classLoader);
        } catch (ClassNotFoundException e1) {
          // ignore... assume first exception is best.
        }
      }
  
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "Error loading class '" + cname + "'", e, false);
    }
  }

  public Object newInstance(String cname, String ... subpackages) {
    Class clazz = findClass(cname,subpackages);
    if( clazz == null ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
          "Can not find class: "+cname + " in " + classLoader, false);
    }
    
    Object obj = null;
    try {
      obj = clazz.newInstance();
    } 
    catch (Exception e) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
          "Error instantiating class: '" + clazz.getName()+"'", e, false );
    }
    
    if( obj instanceof SolrCoreAware ) {
      assertAwareCompatibility( SolrCoreAware.class, obj );
      waitingForCore.add( (SolrCoreAware)obj );
    }
    if( obj instanceof ResourceLoaderAware ) {
      assertAwareCompatibility( ResourceLoaderAware.class, obj );
      waitingForResources.add( (ResourceLoaderAware)obj );
    }
    return obj;
  }
  
  /**
   * Tell all {@link SolrCoreAware} instances about the SolrCore
   */
  public void inform(SolrCore core) 
  {
    for( SolrCoreAware aware : waitingForCore ) {
      aware.inform( core );
    }
    waitingForCore.clear();
  }
  
  /**
   * Tell all {@link ResourceLoaderAware} instances about the loader
   */
  public void inform( ResourceLoader loader ) 
  {
    for( ResourceLoaderAware aware : waitingForResources ) {
      aware.inform( loader );
    }
    waitingForResources.clear();
  }
  /**
   * Determines the instanceDir from the environment.
   * Tries JNDI (java:comp/env/solr/home) then system property (solr.solr.home);
   * if both fail, defaults to solr/
   * @return the instance directory name
   */
  /**
   * Finds the instanceDir based on looking up the value in one of three places:
   * <ol>
   *  <li>JNDI: via java:comp/env/solr/home</li>
   *  <li>The system property solr.solr.home</li>
   *  <li>Look in the current working directory for a solr/ directory</li> 
   * </ol>
   *
   * The return value is normalized.  Normalization essentially means it ends in a trailing slash.
   * @return A normalized instanceDir
   *
   * @see #normalizeDir(String) 
   */
  public static String locateInstanceDir() {
    String home = null;
    // Try JNDI
    try {
      Context c = new InitialContext();
      home = (String)c.lookup("java:comp/env/"+project+"/home");
      log.info("Using JNDI solr.home: "+home );
    } catch (NoInitialContextException e) {
      log.info("JNDI not configured for "+project+" (NoInitialContextEx)");
    } catch (NamingException e) {
      log.info("No /"+project+"/home in JNDI");
    } catch( RuntimeException ex ) {
      log.warning("Odd RuntimeException while testing for JNDI: " + ex.getMessage());
    } 
    
    // Now try system property
    if( home == null ) {
      String prop = project + ".solr.home";
      home = System.getProperty(prop);
      if( home != null ) {
        log.info("using system property "+prop+": " + home );
      }
    }
    
    // if all else fails, try 
    if( home == null ) {
      home = project + '/';
      log.info(project + " home defaulted to '" + home + "' (could not find system property or JNDI)");
    }
    return normalizeDir( home );
  }

  public String getInstanceDir() {
    return instanceDir;
  }
  
  /**
   * Keep a list of classes that are allowed to implement each 'Aware' interface
   */
  private static final Map<Class, Class[]> awareCompatibility;
  static {
    awareCompatibility = new HashMap<Class, Class[]>();
    awareCompatibility.put( 
      SolrCoreAware.class, new Class[] {
        SolrRequestHandler.class,
        QueryResponseWriter.class,
        SearchComponent.class
      }
    );

    awareCompatibility.put( 
      ResourceLoaderAware.class, new Class[] {
        TokenFilterFactory.class,
        TokenizerFactory.class,
        FieldType.class
      }
    );
  }

  /**
   * Utility function to throw an exception if the class is invalid
   */
  void assertAwareCompatibility( Class aware, Object obj )
  {
    Class[] valid = awareCompatibility.get( aware );
    if( valid == null ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
          "Unknown Aware interface: "+aware );
    }
    for( Class v : valid ) {
      if( v.isInstance( obj ) ) {
        return;
      }
    }
    StringBuilder builder = new StringBuilder();
    builder.append( "Invalid 'Aware' object: " ).append( obj );
    builder.append( " -- ").append( aware.getName() );
    builder.append(  " must be an instance of: " );
    for( Class v : valid ) {
      builder.append( "[" ).append( v.getName() ).append( "] ") ;
    }
    throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, builder.toString() );
  }
  
}