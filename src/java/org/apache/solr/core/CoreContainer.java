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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.common.util.XML;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.schema.IndexSchema;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


/**
 * @version $Id$
 * @since solr 1.3
 */
public class CoreContainer 
{
  protected static Logger log = Logger.getLogger(CoreContainer.class.getName());
  
  protected final Map<String, CoreDescriptor> cores = new LinkedHashMap<String, CoreDescriptor>();
  protected boolean persistent = false;
  protected String adminPath = null;
  protected String managementPath = null;
  protected CoreAdminHandler coreAdminHandler = null;
  protected File configFile = null;
  protected String libDir = null;
  protected ClassLoader libLoader = null;
  protected SolrResourceLoader loader = null;
  protected java.lang.ref.WeakReference<SolrCore> adminCore = null;
  
  public CoreContainer() {
  }
  
  // Helper class to initialize the CoreContainer
  public static class Initializer {
    protected String solrConfigFilename = null;
    protected boolean abortOnConfigurationError = true;
    protected String managementPath = null;
    
    public boolean isAbortOnConfigurationError() {
      return abortOnConfigurationError;
    }

    public void setAbortOnConfigurationError(boolean abortOnConfigurationError) {
      this.abortOnConfigurationError = abortOnConfigurationError;
    }

    public String getSolrConfigFilename() {
      return solrConfigFilename;
    }

    public void setSolrConfigFilename(String solrConfigFilename) {
      this.solrConfigFilename = solrConfigFilename;
    }
    
    public String getManagementPath() {
      return managementPath;
    }

    public void setManagementPath(String managementPath) {
      this.managementPath = managementPath;
    }

    // core container instantiation
    public CoreContainer initialize() throws IOException, ParserConfigurationException, SAXException {
      CoreContainer cores = null;
      String instanceDir = SolrResourceLoader.locateInstanceDir();
      File fconf = new File(instanceDir, solrConfigFilename == null? "solr.xml": solrConfigFilename);
      log.info("looking for solr.xml: " + fconf.getAbsolutePath());

      if (fconf.exists()) {
        cores = new CoreContainer();
        cores.load(instanceDir, fconf);
        abortOnConfigurationError = false;
        // if any core aborts on startup, then abort
        for (SolrCore c : cores.getCores()) {
          if (c.getSolrConfig().getBool("abortOnConfigurationError", false)) {
            abortOnConfigurationError = true;
            break;
          }
        }
        solrConfigFilename = cores.getConfigFile().getName();
      } else {
        // perform compatibility init
        cores = new CoreContainer();
        cores.loader = new SolrResourceLoader(instanceDir);
        SolrConfig cfg = solrConfigFilename == null ? new SolrConfig() : new SolrConfig(solrConfigFilename);
        CoreDescriptor dcore = new CoreDescriptor(cores);
        dcore.init("", cfg.getResourceLoader().getInstanceDir());
        SolrCore singlecore = new SolrCore(null, null, cfg, null, dcore);
        dcore.setCore(singlecore);
        abortOnConfigurationError = cfg.getBool(
                "abortOnConfigurationError", abortOnConfigurationError);
        cores.register(dcore);
        cores.setPersistent(false);
        solrConfigFilename = cfg.getName();
      }
      return cores;
    }
  }

  /**
   * Initalize CoreContainer directly from the constructor
   * 
   * @param dir
   * @param configFile
   * @throws ParserConfigurationException
   * @throws IOException
   * @throws SAXException
   */
  public CoreContainer(String dir, File configFile ) throws ParserConfigurationException, IOException, SAXException 
  {
    this.load(dir, configFile);
  }
  
  //-------------------------------------------------------------------
  // Initialization / Cleanup
  //-------------------------------------------------------------------
  
  /**
   * Load a config file listing the available solr cores.
   * @param dir the home directory of all resources.
   * @param configFile the configuration file
   * @throws javax.xml.parsers.ParserConfigurationException
   * @throws java.io.IOException
   * @throws org.xml.sax.SAXException
   */
  public void load(String dir, File configFile ) throws ParserConfigurationException, IOException, SAXException {
    this.configFile = configFile;
    this.loader = new SolrResourceLoader(dir);
    FileInputStream cfgis = new FileInputStream(configFile);
    try {
      Config cfg = new Config(loader, null, cfgis, null);
    
      persistent = cfg.getBool( "solr/@persistent", false );
      libDir     = cfg.get(     "solr/@sharedLib", null);
      adminPath  = cfg.get(     "solr/cores/@adminPath", null );
      managementPath  = cfg.get("solr/cores/@managementPath", null );
      
      if (libDir != null) {
        // relative dir to conf
        File f = new File(dir, libDir);
        libDir = f.getPath(); 
        log.info( "loading shared library: "+f.getAbsolutePath() );
        libLoader = SolrResourceLoader.createClassLoader(f, null);
      }
      
      if( adminPath != null ) {
        coreAdminHandler = this.createMultiCoreHandler();
      }
      
      NodeList nodes = (NodeList)cfg.evaluate("solr/cores/core", XPathConstants.NODESET);
      synchronized (cores) {
        for (int i=0; i<nodes.getLength(); i++) {
          Node node = nodes.item(i);
          try {
            CoreDescriptor p = new CoreDescriptor(this);
            p.init(DOMUtil.getAttr(node, "name", null), DOMUtil.getAttr(node, "instanceDir", null));
            // deal with optional settings
            String opt = DOMUtil.getAttr(node, "config", null);
            if (opt != null) {
              p.setConfigName(opt);
            }
            opt = DOMUtil.getAttr(node, "schema", null);
            if (opt != null) {
              p.setSchemaName(opt);
            }
            CoreDescriptor old = cores.get(p.getName());
            if (old != null && old.getName() != null && old.getName().equals(p.getName())) {
              throw new RuntimeException( cfg.getName() +
                " registers multiple cores to the same name: " + p.name);
            }
            p.setCore(create(p));
          }
          catch (Throwable ex) {
            SolrConfig.severeErrors.add( ex );
            SolrException.logOnce(log,null,ex);
          }
        }
      }
    }
    finally {
      if (cfgis != null) {
        try { cfgis.close(); } catch (Exception xany) {}
      }
    }
  }
  
  /**
   * Stops all cores.
   */
  public void shutdown() {
    synchronized(cores) {
      for(CoreDescriptor descriptor : cores.values()) {
        SolrCore core = descriptor.getCore();
        if( core != null ) {
          core.close();
        }
      }
      cores.clear();
    }
  }
  
  @Override
  protected void finalize() {
    shutdown();
  }
  
  // ---------------- CoreDescriptor related methods --------------- 
  /**
   * Registers a SolrCore descriptor in the registry.
   * @param descr the Solr core descriptor
   * @return a previous descriptor having the same name if it existed, null otherwise
   */
  public CoreDescriptor register( CoreDescriptor descr ) {
    if( descr == null ) {
      throw new RuntimeException( "Can not register a null core." );
    }
    String name = descr.getName();
    if( name == null || 
        name.indexOf( '/'  ) >= 0 ||
        name.indexOf( '\\' ) >= 0 ){
      throw new RuntimeException( "Invalid core name: "+name );
    }

    CoreDescriptor old = null;    
    synchronized (cores) {
      old = cores.put(name, descr);
    }
    if( old == null ) {
      log.info( "registering core: "+name );
      return null;
    } 
    else {
      log.info( "replacing core: "+name );
      return old;
    }
  }

  /**
   * Creates a new core based on a descriptor.
   *
   * @param dcore a core descriptor
   * @return the newly created core
   * @throws javax.xml.parsers.ParserConfigurationException
   * @throws java.io.IOException
   * @throws org.xml.sax.SAXException
   */
  public SolrCore create(CoreDescriptor dcore)  throws ParserConfigurationException, IOException, SAXException {
    // Make the instanceDir relative to the cores instanceDir if not absolute
    File idir = new File(dcore.getInstanceDir());
    if (!idir.isAbsolute()) {
      idir = new File(loader.getInstanceDir(), dcore.getInstanceDir());
    }
    String instanceDir = idir.getPath();
    
    // Initialize the solr config
    SolrResourceLoader solrLoader = new SolrResourceLoader(instanceDir, libLoader);
    SolrConfig config = new SolrConfig(solrLoader, dcore.getConfigName(), null);
    IndexSchema schema = new IndexSchema(config, dcore.getSchemaName(), null);
    SolrCore core = new SolrCore(dcore.getName(), null, config, schema, dcore);
    dcore.setCore(core);
    
    // Register the new core
    CoreDescriptor old = this.register(dcore);
    return core;
  }
    
  /**
   * @return a Collection of registered SolrCores
   */
  public Collection<SolrCore> getCores() {
    java.util.List<SolrCore> l = new java.util.ArrayList<SolrCore>();
    synchronized (cores) {
      for(CoreDescriptor descr : this.cores.values()) {
        if (descr.getCore() != null)
          l.add(descr.getCore());
      }
    }
    return l;
  }
    
  /**
   * @return a Collection of registered CoreDescriptors
   */
  public Collection<CoreDescriptor> getDescriptors() {
   java.util.List<CoreDescriptor> l = new java.util.ArrayList<CoreDescriptor>();
   synchronized (cores) {
     l.addAll(cores.values());
   }
   return l;
  }

  /**
   * @return the CoreDescriptor registered under that name
   */
  public CoreDescriptor getDescriptor(String name) {
    synchronized(cores) {
      return cores.get( name );
    }
  }
  
  
  // ---------------- Core name related methods --------------- 
  /**
   * Recreates a SolrCore.
   * While the new core is loading, requests will continue to be dispatched to
   * and processed by the old core
   * 
   * @param name the name of the SolrCore to reload
   * @throws ParserConfigurationException
   * @throws IOException
   * @throws SAXException
   */

  public void reload(String name) throws ParserConfigurationException, IOException, SAXException {
    synchronized(cores) {
      CoreDescriptor dcore = cores.get(name);
      if (dcore == null)
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "No such core: " + name );   
      create(new CoreDescriptor(dcore));
    }
  }
    
  
  /**
   * Swaps two SolrCore descriptors.
   * @param n0
   * @param n1
   */
  public void swap(String n0, String n1) {
    if( n0 == null || n1 == null ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Can not swap unnamed cores." );
    }
    synchronized( cores ) {
      CoreDescriptor c0 = cores.get(n0);
      if (c0 == null)
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "No such core: " + n0 );
      CoreDescriptor c1 = cores.get(n1);
      if (c1 == null)
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "No such core: " + n1 );
      cores.put(n0, c1);
      cores.put(n1, c0);
      c0.setName( n1 );
      if (c0.getCore() != null)
        c0.getCore().setName(n1);
      c1.setName( n0 );
      if (c1.getCore() != null)
        c1.getCore().setName(n0);
      log.info( "swaped: "+c0.getName() + " with " + c1.getName() );
    }
  }
  
  /** Removes & closes a registered core. */
  public void remove( String name ) {
    synchronized(cores) {
      CoreDescriptor dcore = cores.remove( name );
      if (dcore == null) {
        return;
      }
      
      SolrCore core = dcore.getCore();
      if (core != null) {
        core.close();
      }
    }
  }

  
  /** Gets a core by name and increase its refcount.
   * @see SolrCore.open() @see SolrCore.close()
   * @param name the core name
   * @return the core if found
   */
  public SolrCore getCore(String name) {
    synchronized(cores) {
      CoreDescriptor dcore = cores.get(name);
       SolrCore core = null;
      if (dcore != null)
        core = dcore.getCore();
       return core;
// solr-647
//      if (core != null)
//        return core.open();
//      return null;
    }
  }

  /**
   * Sets the preferred core used to handle MultiCore admin tasks.
   */
  public void setAdminCore(SolrCore core) {
    synchronized (cores) {
      adminCore = new java.lang.ref.WeakReference<SolrCore>(core);
    }
  }

  /**
   * Ensures there is a valid core to handle MultiCore admin taks and
   * increase its refcount.
   * @return the acquired admin core, null if no core is available
   */
  public SolrCore getAdminCore() {
    synchronized (cores) {
      SolrCore core = adminCore != null ? adminCore.get() : null;
//      solr-647
//      if (core != null)
//        core = core.open();
      if (core == null) {
        for (CoreDescriptor descr : this.cores.values()) {
          core = descr.getCore();
//          solr-647
//          if (core != null)
//            core = core.open();
          if (core != null) {
            break;
          }
        }
        setAdminCore(core);
      }
      return core;
    }
  }

  // ---------------- Multicore self related methods --------------- 
  /** 
   * Creates a CoreAdminHandler for this MultiCore.
   * @return a CoreAdminHandler
   */
  protected CoreAdminHandler createMultiCoreHandler() {
    return new CoreAdminHandler() {
      @Override
      public CoreContainer getCoreContainer() {
        return CoreContainer.this;
      }
    };
  }
 
  public CoreAdminHandler getMultiCoreHandler() {
    return coreAdminHandler;
  }
  
  // all of the following properties aren't synchronized
  // but this should be OK since they normally won't be changed rapidly
  public boolean isPersistent() {
    return persistent;
  }
  
  public void setPersistent(boolean persistent) {
    this.persistent = persistent;
  }
  
  public String getAdminPath() {
    return adminPath;
  }
  
  public void setAdminPath(String adminPath) {
      this.adminPath = adminPath;
  }
  

  public String getManagementPath() {
    return managementPath;
  }
  
  /**
   * Sets the alternate path for multicore handling:
   * This is used in case there is a registered unnamed core (aka name is "") to
   * declare an alternate way of accessing named cores.
   * This can also be used in a pseudo single-core environment so admins can prepare
   * a new version before swapping.
   * @param path
   */
  public void setManagementPath(String path) {
    this.managementPath = path;
  }
  
  public File getConfigFile() {
    return configFile;
  }
  
/** Persists the cores config file in cores.xml. */
  public void persist() {
    persistFile(null);
  }
  
  /** Persists the cores config file in a user provided file. */
  public void persistFile(File file) {
    File tmpFile = null;
    try {
      // write in temp first
      if (file == null) {
        file = tmpFile = File.createTempFile("solr", ".xml", configFile.getParentFile());
      }
      java.io.FileOutputStream out = new java.io.FileOutputStream(file);
      synchronized(cores) {
        Writer writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
        persist(writer);
        writer.flush();
        writer.close();
        out.close();
        // rename over origin or copy it this fails
        if (tmpFile != null) {
          if (tmpFile.renameTo(configFile))
            tmpFile = null;
          else
            fileCopy(tmpFile, configFile);
        }
      }
    } 
    catch(java.io.FileNotFoundException xnf) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, xnf);
    } 
    catch(java.io.IOException xio) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, xio);
    } 
    finally {
      if (tmpFile != null) {
        if (!tmpFile.delete())
          tmpFile.deleteOnExit();
      }
    }
  }
  
  /** Write the cores configuration through a writer.*/
  void persist(Writer writer) throws IOException {
    writer.write("<?xml version='1.0' encoding='UTF-8'?>");
    writer.write("<solr");
    if (this.libDir != null) {
      writer.write(" sharedLib='");
      XML.escapeAttributeValue(libDir, writer);
      writer.write('\'');
    }
    writer.write(" persistent='");
    if (isPersistent()) {
      writer.write("true'");
    }
    else {
      writer.write("false'");
    }
    writer.write(">\n");
    writer.write("<cores adminPath='");
    XML.escapeAttributeValue(adminPath, writer);
    writer.write('\'');
    writer.write(">\n");

    synchronized(cores) {
      for (Map.Entry<String, CoreDescriptor> entry : cores.entrySet()) {
        persist(writer, entry.getValue());
      }
    }
    writer.write("</cores>\n");
    writer.write("</solr>\n");
  }
  
  /** Writes the cores configuration node for a given core. */
  void persist(Writer writer, CoreDescriptor dcore) throws IOException {
    writer.write("  <core");
    writer.write (" name='");
    XML.escapeAttributeValue(dcore.getName(), writer);
    writer.write("' instanceDir='");
    XML.escapeAttributeValue(dcore.getInstanceDir(), writer);
    writer.write('\'');
    //write config (if not default)
    String opt = dcore.getConfigName();
    if (opt != null && !opt.equals(dcore.getDefaultConfigName())) {
      writer.write(" config='");
      XML.escapeAttributeValue(opt, writer);
      writer.write('\'');
    }
    //write schema (if not default)
    opt = dcore.getSchemaName();
    if (opt != null && !opt.equals(dcore.getDefaultSchemaName())) {
      writer.write(" schema='");
      XML.escapeAttributeValue(opt, writer);
      writer.write('\'');
    }
    writer.write("/>\n"); // core
  }
  
  /** Copies a src file to a dest file:
   *  used to circumvent the platform discrepancies regarding renaming files.
   */
  public static void fileCopy(File src, File dest) throws IOException {
    IOException xforward = null;
    FileInputStream fis =  null;
    FileOutputStream fos = null;
    FileChannel fcin = null;
    FileChannel fcout = null;
    try {
      fis = new FileInputStream(src);
      fos = new FileOutputStream(dest);
      fcin = fis.getChannel();
      fcout = fos.getChannel();
      // do the file copy 32Mb at a time
      final int MB32 = 32*1024*1024;
      long size = fcin.size();
      long position = 0;
      while (position < size) {
        position += fcin.transferTo(position, MB32, fcout);
      }
    } 
    catch(IOException xio) {
      xforward = xio;
    } 
    finally {
      if (fis   != null) try { fis.close(); fis = null; } catch(IOException xio) {}
      if (fos   != null) try { fos.close(); fos = null; } catch(IOException xio) {}
      if (fcin  != null && fcin.isOpen() ) try { fcin.close();  fcin = null;  } catch(IOException xio) {}
      if (fcout != null && fcout.isOpen()) try { fcout.close(); fcout = null; } catch(IOException xio) {}
    }
    if (xforward != null) {
      throw xforward;
    }
  }
 
}
