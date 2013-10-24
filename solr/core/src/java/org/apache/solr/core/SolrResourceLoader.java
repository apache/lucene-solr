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

import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.analysis.util.WordlistLoader;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.ManagedIndexSchemaFactory;
import org.apache.solr.schema.SimilarityFactory;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.FileUtils;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.NoInitialContextException;
import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @since solr 1.3
 */ 
public class SolrResourceLoader implements ResourceLoader,Closeable
{
  public static final Logger log = LoggerFactory.getLogger(SolrResourceLoader.class);

  static final String project = "solr";
  static final String base = "org.apache" + "." + project;
  static final String[] packages = {"","analysis.","schema.","handler.","search.","update.","core.","response.","request.","update.processor.","util.", "spelling.", "handler.component.", "handler.dataimport." };

  protected URLClassLoader classLoader;
  private final String instanceDir;
  private String dataDir;
  
  private final List<SolrCoreAware> waitingForCore = Collections.synchronizedList(new ArrayList<SolrCoreAware>());
  private final List<SolrInfoMBean> infoMBeans = Collections.synchronizedList(new ArrayList<SolrInfoMBean>());
  private final List<ResourceLoaderAware> waitingForResources = Collections.synchronizedList(new ArrayList<ResourceLoaderAware>());
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  //TODO: Solr5. Remove this completely when you obsolete putting <core> tags in solr.xml (See Solr-4196)
  private final Properties coreProperties;

  private volatile boolean live;

  /**
   * <p>
   * This loader will delegate to the context classloader when possible,
   * otherwise it will attempt to resolve resources using any jar files
   * found in the "lib/" directory in the specified instance directory.
   * </p>
   *
   * @param instanceDir - base directory for this resource loader, if null locateSolrHome() will be used.
   * @see #locateSolrHome
   */
  public SolrResourceLoader( String instanceDir, ClassLoader parent, Properties coreProperties )
  {
    if( instanceDir == null ) {
      this.instanceDir = SolrResourceLoader.locateSolrHome();
      log.info("new SolrResourceLoader for deduced Solr Home: '{}'", 
               this.instanceDir);
    } else{
      this.instanceDir = normalizeDir(instanceDir);
      log.info("new SolrResourceLoader for directory: '{}'",
               this.instanceDir);
    }
    
    this.classLoader = createClassLoader(null, parent);
    addToClassLoader("./lib/", null, true);
    reloadLuceneSPI();
    this.coreProperties = coreProperties;
  }

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
    this(instanceDir, parent, null);
  }

  /**
   * Adds every file/dir found in the baseDir which passes the specified Filter
   * to the ClassLoader used by this ResourceLoader.  This method <b>MUST</b>
   * only be called prior to using this ResourceLoader to get any resources, otherwise
   * it's behavior will be non-deterministic. You also have to {link @reloadLuceneSPI}
   * before using this ResourceLoader.
   * 
   * <p>This method will quietly ignore missing or non-directory <code>baseDir</code>
   *  folder. 
   *
   * @param baseDir base directory whose children (either jars or directories of
   *                classes) will be in the classpath, will be resolved relative
   *                the instance dir.
   * @param filter The filter files must satisfy, if null all files will be accepted.
   * @param quiet  Be quiet if baseDir does not point to a directory or if no file is 
   *               left after applying the filter. 
   */
  void addToClassLoader(final String baseDir, final FileFilter filter, boolean quiet) {
    File base = FileUtils.resolvePath(new File(getInstanceDir()), baseDir);
    if (base != null && base.exists() && base.isDirectory()) {
      File[] files = base.listFiles(filter);
      if (files == null || files.length == 0) {
        if (!quiet) {
          log.warn("No files added to classloader from lib: "
                   + baseDir + " (resolved as: " + base.getAbsolutePath() + ").");
        }
      } else {
        this.classLoader = replaceClassLoader(classLoader, base, filter);
      }
    } else {
      if (!quiet) {
        log.warn("Can't find (or read) directory to add to classloader: "
            + baseDir + " (resolved as: " + base.getAbsolutePath() + ").");
      }
    }
  }
  
  /**
   * Reloads all Lucene SPI implementations using the new classloader.
   * This method must be called after {@link #addToClassLoader(String, FileFilter, boolean)}
   * and {@link #addToClassLoader(String,FileFilter,boolean)} before using
   * this ResourceLoader.
   */
  void reloadLuceneSPI() {
    // Codecs:
    PostingsFormat.reloadPostingsFormats(this.classLoader);
    DocValuesFormat.reloadDocValuesFormats(this.classLoader);
    Codec.reloadCodecs(this.classLoader);
    // Analysis:
    CharFilterFactory.reloadCharFilters(this.classLoader);
    TokenFilterFactory.reloadTokenFilters(this.classLoader);
    TokenizerFactory.reloadTokenizers(this.classLoader);
  }
  
  private static URLClassLoader replaceClassLoader(final URLClassLoader oldLoader,
                                                   final File base,
                                                   final FileFilter filter) {
    if (null != base && base.canRead() && base.isDirectory()) {
      File[] files = base.listFiles(filter);
      
      if (null == files || 0 == files.length) return oldLoader;
      
      URL[] oldElements = oldLoader.getURLs();
      URL[] elements = new URL[oldElements.length + files.length];
      System.arraycopy(oldElements, 0, elements, 0, oldElements.length);
      
      for (int j = 0; j < files.length; j++) {
        try {
          URL element = files[j].toURI().normalize().toURL();
          log.info("Adding '" + element.toString() + "' to classloader");
          elements[oldElements.length + j] = element;
        } catch (MalformedURLException e) {
          SolrException.log(log, "Can't add element to classloader: " + files[j], e);
        }
      }
      ClassLoader oldParent = oldLoader.getParent();
      IOUtils.closeWhileHandlingException(oldLoader); // best effort
      return URLClassLoader.newInstance(elements, oldParent);
    }
    // are we still here?
    return oldLoader;
  }
  
  /**
   * Convenience method for getting a new ClassLoader using all files found
   * in the specified lib directory.
   */
  static URLClassLoader createClassLoader(final File libDir, ClassLoader parent) {
    if ( null == parent ) {
      parent = Thread.currentThread().getContextClassLoader();
    }
    return replaceClassLoader(URLClassLoader.newInstance(new URL[0], parent),
                              libDir, null);
  }
  
  public SolrResourceLoader( String instanceDir )
  {
    this( instanceDir, null, null );
  }
  
  /** Ensures a directory name always ends with a '/'. */
  public  static String normalizeDir(String path) {
    return ( path != null && (!(path.endsWith("/") || path.endsWith("\\"))) )? path + File.separator : path;
  }
  
  public String[] listConfigDir() {
    File configdir = new File(getConfigDir());
    if( configdir.exists() && configdir.isDirectory() ) {
      return configdir.list();
    } else {
      return new String[0];
    }
  }

  public String getConfigDir() {
    return instanceDir + "conf" + File.separator;
  }
  
  public String getDataDir()    {
    return dataDir;
  }

  public Properties getCoreProperties() {
    return coreProperties;
  }

  /**
   * EXPERT
   * <p/>
   * The underlying class loader.  Most applications will not need to use this.
   * @return The {@link ClassLoader}
   */
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  /** Opens a schema resource by its name.
   * Override this method to customize loading schema resources.
   *@return the stream for the named schema
   */
  public InputStream openSchema(String name) throws IOException {
    return openResource(name);
  }
  
  /** Opens a config resource by its name.
   * Override this method to customize loading config resources.
   *@return the stream for the named configuration
   */
  public InputStream openConfig(String name) throws IOException {
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
  @Override
  public InputStream openResource(String resource) throws IOException {
    InputStream is=null;
    try {
      File f0 = new File(resource), f = f0;
      if (!f.isAbsolute()) {
        // try $CWD/$configDir/$resource
        f = new File(getConfigDir() + resource).getAbsoluteFile();
      }
      boolean found = f.isFile() && f.canRead();
      if (!found) { // no success with $CWD/$configDir/$resource
        f = f0.getAbsoluteFile();
        found = f.isFile() && f.canRead();
      }
      // check that we don't escape instance dir
      if (found) {
        if (!Boolean.parseBoolean(System.getProperty("solr.allow.unsafe.resourceloading", "false"))) {
          final URI instanceURI = new File(getInstanceDir()).getAbsoluteFile().toURI().normalize();
          final URI fileURI = f.toURI().normalize();
          if (instanceURI.relativize(fileURI) == fileURI) {
            // no URI relativize possible, so they don't share same base folder
            throw new IOException("For security reasons, SolrResourceLoader cannot load files from outside the instance's directory: " + f +
                "; if you want to override this safety feature and you are sure about the consequences, you can pass the system property "+
                "-Dsolr.allow.unsafe.resourceloading=true to your JVM");
          }
        }
        // relativize() returned a relative, new URI, so we are fine!
        return new FileInputStream(f);
      }
      // Delegate to the class loader (looking into $INSTANCE_DIR/lib jars).
      // We need a ClassLoader-compatible (forward-slashes) path here!
      is = classLoader.getResourceAsStream(resource.replace(File.separatorChar, '/'));
      // This is a hack just for tests (it is not done in ZKResourceLoader)!
      // -> the getConfigDir's path must not be absolute!
      if (is == null && System.getProperty("jetty.testMode") != null && !new File(getConfigDir()).isAbsolute()) {
        is = classLoader.getResourceAsStream((getConfigDir() + resource).replace(File.separatorChar, '/'));
      }
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      throw new IOException("Error opening " + resource, e);
    }
    if (is==null) {
      throw new IOException("Can't find resource '" + resource + "' in classpath or '" + new File(getConfigDir()).getAbsolutePath() + "'");
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
   * @return a list of non-blank non-comment lines with whitespace trimmed
   * from front and back.
   * @throws IOException If there is a low-level I/O error.
   */
  public List<String> getLines(String resource) throws IOException {
    return getLines(resource, UTF_8);
  }

  /**
   * Accesses a resource by name and returns the (non comment) lines containing
   * data using the given character encoding.
   *
   * <p>
   * A comment line is any line that starts with the character "#"
   * </p>
   *
   * @param resource the file to be read
   * @return a list of non-blank non-comment lines with whitespace trimmed
   * @throws IOException If there is a low-level I/O error.
   */
  public List<String> getLines(String resource,
      String encoding) throws IOException {
    return getLines(resource, Charset.forName(encoding));
  }


  public List<String> getLines(String resource, Charset charset) throws IOException{
    try {
      return WordlistLoader.getLines(openResource(resource), charset);
    } catch (CharacterCodingException ex) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
         "Error loading resource (wrong encoding?): " + resource, ex);
    }
  }

  /*
   * A static map of short class name to fully qualified class name 
   */
  private static final Map<String, String> classNameCache = new ConcurrentHashMap<String, String>();

  // Using this pattern, legacy analysis components from previous Solr versions are identified and delegated to SPI loader:
  private static final Pattern legacyAnalysisPattern = 
      Pattern.compile("((\\Q"+base+".analysis.\\E)|(\\Q"+project+".\\E))([\\p{L}_$][\\p{L}\\p{N}_$]+?)(TokenFilter|Filter|Tokenizer|CharFilter)Factory");

  @Override
  public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
    return findClass(cname, expectedType, empty);
  }
  
  /**
   * This method loads a class either with it's FQN or a short-name (solr.class-simplename or class-simplename).
   * It tries to load the class with the name that is given first and if it fails, it tries all the known
   * solr packages. This method caches the FQN of a short-name in a static map in-order to make subsequent lookups
   * for the same class faster. The caching is done only if the class is loaded by the webapp classloader and it
   * is loaded using a shortname.
   *
   * @param cname The name or the short name of the class.
   * @param subpackages the packages to be tried if the cname starts with solr.
   * @return the loaded class. An exception is thrown if it fails
   */
  public <T> Class<? extends T> findClass(String cname, Class<T> expectedType, String... subpackages) {
    if (subpackages == null || subpackages.length == 0 || subpackages == packages) {
      subpackages = packages;
      String  c = classNameCache.get(cname);
      if(c != null) {
        try {
          return Class.forName(c, true, classLoader).asSubclass(expectedType);
        } catch (ClassNotFoundException e) {
          //this is unlikely
          log.error("Unable to load cached class-name :  "+ c +" for shortname : "+cname + e);
        }

      }
    }
    Class<? extends T> clazz = null;
    
    // first try legacy analysis patterns, now replaced by Lucene's Analysis package:
    final Matcher m = legacyAnalysisPattern.matcher(cname);
    if (m.matches()) {
      final String name = m.group(4);
      log.trace("Trying to load class from analysis SPI using name='{}'", name);
      try {
        if (CharFilterFactory.class.isAssignableFrom(expectedType)) {
          return clazz = CharFilterFactory.lookupClass(name).asSubclass(expectedType);
        } else if (TokenizerFactory.class.isAssignableFrom(expectedType)) {
          return clazz = TokenizerFactory.lookupClass(name).asSubclass(expectedType);
        } else if (TokenFilterFactory.class.isAssignableFrom(expectedType)) {
          return clazz = TokenFilterFactory.lookupClass(name).asSubclass(expectedType);
        } else {
          log.warn("'{}' looks like an analysis factory, but caller requested different class type: {}", cname, expectedType.getName());
        }
      } catch (IllegalArgumentException ex) { 
        // ok, we fall back to legacy loading
      }
    }
    
    // first try cname == full name
    try {
      return Class.forName(cname, true, classLoader).asSubclass(expectedType);
    } catch (ClassNotFoundException e) {
      String newName=cname;
      if (newName.startsWith(project)) {
        newName = cname.substring(project.length()+1);
      }
      for (String subpackage : subpackages) {
        try {
          String name = base + '.' + subpackage + newName;
          log.trace("Trying class name " + name);
          return clazz = Class.forName(name,true,classLoader).asSubclass(expectedType);
        } catch (ClassNotFoundException e1) {
          // ignore... assume first exception is best.
        }
      }
  
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "Error loading class '" + cname + "'", e);
    }finally{
      //cache the shortname vs FQN if it is loaded by the webapp classloader  and it is loaded
      // using a shortname
      if ( clazz != null &&
              clazz.getClassLoader() == SolrResourceLoader.class.getClassLoader() &&
              !cname.equals(clazz.getName()) &&
              (subpackages.length == 0 || subpackages == packages)) {
        //store in the cache
        classNameCache.put(cname, clazz.getName());
      }
    }
  }
  
  static final String empty[] = new String[0];
  
  @Override
  public <T> T newInstance(String name, Class<T> expectedType) {
    return newInstance(name, expectedType, empty);
  }

  public <T> T newInstance(String cname, Class<T> expectedType, String ... subpackages) {
    Class<? extends T> clazz = findClass(cname, expectedType, subpackages);
    if( clazz == null ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
          "Can not find class: "+cname + " in " + classLoader);
    }
    
    T obj = null;
    try {
      obj = clazz.newInstance();
    } 
    catch (Exception e) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
          "Error instantiating class: '" + clazz.getName()+"'", e);
    }

    if (!live) {
      if( obj instanceof SolrCoreAware ) {
        assertAwareCompatibility( SolrCoreAware.class, obj );
        waitingForCore.add( (SolrCoreAware)obj );
      }
      if (org.apache.solr.util.plugin.ResourceLoaderAware.class.isInstance(obj)) {
        log.warn("Class [{}] uses org.apache.solr.util.plugin.ResourceLoaderAware " +
            "which is deprecated. Change to org.apache.lucene.analysis.util.ResourceLoaderAware.", cname);
      }
      if( obj instanceof ResourceLoaderAware ) {
        assertAwareCompatibility( ResourceLoaderAware.class, obj );
        waitingForResources.add( (ResourceLoaderAware)obj );
      }
      if (obj instanceof SolrInfoMBean){
        //TODO: Assert here?
        infoMBeans.add((SolrInfoMBean) obj);
      }
    }
    return obj;
  }

  public CoreAdminHandler newAdminHandlerInstance(final CoreContainer coreContainer, String cname, String ... subpackages) {
    Class<? extends CoreAdminHandler> clazz = findClass(cname, CoreAdminHandler.class, subpackages);
    if( clazz == null ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
          "Can not find class: "+cname + " in " + classLoader);
    }
    
    CoreAdminHandler obj = null;
    try {
      Constructor<? extends CoreAdminHandler> ctor = clazz.getConstructor(CoreContainer.class);
      obj = ctor.newInstance(coreContainer);
    } 
    catch (Exception e) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
          "Error instantiating class: '" + clazz.getName()+"'", e);
    }

    if (!live) {
      //TODO: Does SolrCoreAware make sense here since in a multi-core context
      // which core are we talking about ?
      if (org.apache.solr.util.plugin.ResourceLoaderAware.class.isInstance(obj)) {
        log.warn("Class [{}] uses org.apache.solr.util.plugin.ResourceLoaderAware " +
            "which is deprecated. Change to org.apache.lucene.analysis.util.ResourceLoaderAware.", cname);
      }
      if( obj instanceof ResourceLoaderAware ) {
        assertAwareCompatibility( ResourceLoaderAware.class, obj );
        waitingForResources.add( (ResourceLoaderAware)obj );
      }
    }

    return obj;
  }

 

  public <T> T newInstance(String cName, Class<T> expectedType, String [] subPackages, Class[] params, Object[] args){
    Class<? extends T> clazz = findClass(cName, expectedType, subPackages);
    if( clazz == null ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
          "Can not find class: "+cName + " in " + classLoader);
    }

    T obj = null;
    try {

      Constructor<? extends T> constructor = clazz.getConstructor(params);
      obj = constructor.newInstance(args);
    }
    catch (Exception e) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
          "Error instantiating class: '" + clazz.getName()+"'", e);
    }

    if (!live) {
      if( obj instanceof SolrCoreAware ) {
        assertAwareCompatibility( SolrCoreAware.class, obj );
        waitingForCore.add( (SolrCoreAware)obj );
      }
      if (org.apache.solr.util.plugin.ResourceLoaderAware.class.isInstance(obj)) {
        log.warn("Class [{}] uses org.apache.solr.util.plugin.ResourceLoaderAware " +
            "which is deprecated. Change to org.apache.lucene.analysis.util.ResourceLoaderAware.", cName);
      }
      if( obj instanceof ResourceLoaderAware ) {
        assertAwareCompatibility( ResourceLoaderAware.class, obj );
        waitingForResources.add( (ResourceLoaderAware)obj );
      }
      if (obj instanceof SolrInfoMBean){
        //TODO: Assert here?
        infoMBeans.add((SolrInfoMBean) obj);
      }
    }

    return obj;
  }

  
  /**
   * Tell all {@link SolrCoreAware} instances about the SolrCore
   */
  public void inform(SolrCore core) 
  {
    this.dataDir = core.getDataDir();

    // make a copy to avoid potential deadlock of a callback calling newInstance and trying to
    // add something to waitingForCore.
    SolrCoreAware[] arr;

    while (waitingForCore.size() > 0) {
      synchronized (waitingForCore) {
        arr = waitingForCore.toArray(new SolrCoreAware[waitingForCore.size()]);
        waitingForCore.clear();
      }

      for( SolrCoreAware aware : arr) {
        aware.inform( core );
      }
    }

    // this is the last method to be called in SolrCore before the latch is released.
    live = true;
  }
  
  /**
   * Tell all {@link ResourceLoaderAware} instances about the loader
   */
  public void inform( ResourceLoader loader ) throws IOException
  {

     // make a copy to avoid potential deadlock of a callback adding to the list
    ResourceLoaderAware[] arr;

    while (waitingForResources.size() > 0) {
      synchronized (waitingForResources) {
        arr = waitingForResources.toArray(new ResourceLoaderAware[waitingForResources.size()]);
        waitingForResources.clear();
      }

      for( ResourceLoaderAware aware : arr) {
        aware.inform(loader);
      }
    }
  }

  /**
   * Register any {@link org.apache.solr.core.SolrInfoMBean}s
   * @param infoRegistry The Info Registry
   */
  public void inform(Map<String, SolrInfoMBean> infoRegistry) {
    // this can currently happen concurrently with requests starting and lazy components
    // loading.  Make sure infoMBeans doesn't change.

    SolrInfoMBean[] arr;
    synchronized (infoMBeans) {
      arr = infoMBeans.toArray(new SolrInfoMBean[infoMBeans.size()]);
      waitingForResources.clear();
    }


    for (SolrInfoMBean bean : arr) {
      try {
        infoRegistry.put(bean.getName(), bean);
      } catch (Throwable t) {
        log.warn("could not register MBean '" + bean.getName() + "'.", t);
      }
    }
  }
  
  /**
   * Determines the solrhome from the environment.
   * Tries JNDI (java:comp/env/solr/home) then system property (solr.solr.home);
   * if both fail, defaults to solr/
   * @return the instance directory name
   */
  /**
   * Finds the solrhome based on looking up the value in one of three places:
   * <ol>
   *  <li>JNDI: via java:comp/env/solr/home</li>
   *  <li>The system property solr.solr.home</li>
   *  <li>Look in the current working directory for a solr/ directory</li> 
   * </ol>
   *
   * The return value is normalized.  Normalization essentially means it ends in a trailing slash.
   * @return A normalized solrhome
   * @see #normalizeDir(String)
   */
  public static String locateSolrHome() {

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
      log.warn("Odd RuntimeException while testing for JNDI: " + ex.getMessage());
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
        CodecFactory.class,
        ManagedIndexSchemaFactory.class,
        QueryResponseWriter.class,
        SearchComponent.class,
        ShardHandlerFactory.class,
        SimilarityFactory.class,
        SolrRequestHandler.class,
        UpdateRequestProcessorFactory.class
      }
    );

    awareCompatibility.put(
      ResourceLoaderAware.class, new Class[] {
        CharFilterFactory.class,
        TokenFilterFactory.class,
        TokenizerFactory.class,
        QParserPlugin.class,
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

  @Override
  public void close() throws IOException {
    IOUtils.close(classLoader);
  }
}
