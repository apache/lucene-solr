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
import org.apache.solr.handler.admin.MultiCoreHandler;
import org.apache.solr.schema.IndexSchema;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


/**
 * @version $Id$
 * @since solr 1.3
 */
public class MultiCore 
{
  protected static Logger log = Logger.getLogger(MultiCore.class.getName());
  
  protected final Map<String, CoreDescriptor> cores = new LinkedHashMap<String, CoreDescriptor>();
  protected boolean enabled = false;
  protected boolean persistent = false;
  protected String adminPath = null;
  protected MultiCoreHandler multiCoreHandler = null;
  protected File configFile = null;
  protected String libDir = null;
  protected ClassLoader libLoader = null;
  protected SolrResourceLoader loader = null;
  protected java.lang.ref.WeakReference<SolrCore> adminCore = null;
  
  public MultiCore() {
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
    
      persistent = cfg.getBool( "multicore/@persistent", false );
      adminPath  = cfg.get(     "multicore/@adminPath", null );
      libDir     = cfg.get(     "multicore/@sharedLib", null);
      
      if (libDir != null) {
        // relative dir to conf
        File f = new File(dir, libDir);
        libDir = f.getPath(); 
        log.info( "loading shared library: "+f.getAbsolutePath() );
        libLoader = SolrResourceLoader.createClassLoader(f, null);
      }
      
      if( adminPath != null ) {
        multiCoreHandler = this.createMultiCoreHandler();
      }
      
      NodeList nodes = (NodeList)cfg.evaluate("multicore/core", XPathConstants.NODESET);
      synchronized (cores) {
        for (int i=0; i<nodes.getLength(); i++) {
          Node node = nodes.item(i);
          try {
            CoreDescriptor p = new CoreDescriptor();
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
    enabled = true;
  }
  
  /**
   * Stops all cores.
   */
  public void shutdown() {
    synchronized(cores) {
      for(Map.Entry<String,CoreDescriptor> e : cores.entrySet()) {
        SolrCore core = e.getValue().getCore();
        if (core == null) continue;
        String key = e.getKey();
        if (core.getName().equals(key))
        core.close();
      }
      cores.clear();
    }
  }
  
  @Override
  protected void finalize() {
    shutdown();
  }
  
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
        name.length() < 1 ||
        name.indexOf( '/'  ) >= 0 ||
        name.indexOf( '\\' ) >= 0 ){
      throw new RuntimeException( "Invalid core name: "+name );
    }
    
    CoreDescriptor old = cores.put(name, descr);
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
   * Swaps two SolrCore descriptors.
   * @param c0
   * @param c1
   */
  public void swap(CoreDescriptor c0, CoreDescriptor c1) {
    if( c0 == null || c1 == null ) {
      throw new RuntimeException( "Can not swap a null core." );
    }
    synchronized( cores ) {
      String n0 = c0.getName();
      String n1 = c1.getName();
      cores.put(n0, c1);
      cores.put(n1, c0);
      c0.setName( n1 );
      if (c0.getCore() != null)
        c0.getCore().setName(n1);
      c1.setName( n0 );
      if (c1.getCore() != null)
        c1.getCore().setName(n0);
    }
    log.info( "swaped: "+c0.getName() + " with " + c1.getName() );
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
    // Make the instanceDir relative to the multicore instanceDir if not absolute
    File idir = new File(dcore.getInstanceDir());
    if (!idir.isAbsolute()) {
      idir = new File(loader.getInstanceDir(), dcore.getInstanceDir());
    }
    String instanceDir = idir.getPath();
    
    // Initialize the solr config
    SolrResourceLoader solrLoader = new SolrResourceLoader(instanceDir, libLoader);
    SolrConfig config = new SolrConfig(solrLoader, dcore.getConfigName(), null);
    IndexSchema schema = new IndexSchema(config, dcore.getSchemaName(), null);
    SolrCore core = new SolrCore(dcore.getName(), null, config, schema);
    dcore.setCore(core);
    
    // Register the new core
    CoreDescriptor old = this.register(dcore);
    return core;
  }
  
  /**
   * Recreates a SolrCore.
   * While the new core is loading, requests will continue to be dispatched to
   * and processed by the old core
   * 
   * @param core the SolrCore to reload
   * @throws ParserConfigurationException
   * @throws IOException
   * @throws SAXException
   */
  public void reload(CoreDescriptor dcore) throws ParserConfigurationException, IOException, SAXException {
    create(new CoreDescriptor(dcore));
  }
    
  // TODO? -- add some kind of hook to close the core after all references are 
  // gone...  is finalize() enough?
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
  
  /**
   * @return a Collection of registered SolrCores
   */
  public Collection<SolrCore> getCores() {
    java.util.List<SolrCore> l = new java.util.ArrayList<SolrCore>();
    for(CoreDescriptor descr : this.cores.values()) {
      if (descr.getCore() != null)
        l.add(descr.getCore());
    }
    return l;
  }
  
  public Collection<CoreDescriptor> getDescriptors() {
    return cores.values();
  }
  
  public SolrCore getCore(String name) {
    CoreDescriptor dcore = getDescriptor( name );
    return (dcore == null) ? null : dcore.getCore();
  }
  
  public CoreDescriptor getDescriptor(String name) {
    synchronized(cores) {
      return cores.get( name );
    }
  }
  
  public boolean isEnabled() {
    return enabled;
  }
  
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }
  
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
  
  /**
   * Sets the preferred core used to handle MultiCore admin tasks.
   * Note that getAdminCore is not symmetrical to this method since
   * it will allways return an opened SolrCore.
   * This however can be useful implementing a "metacore" (a core of cores).
   */
  public void setAdminCore(SolrCore core) {
    adminCore = new java.lang.ref.WeakReference<SolrCore>(core);
  }

  /**
   * Gets a core to handle MultiCore admin tasks (@see SolrDispatchFilter).
   * This makes the best attempt to reuse the same opened SolrCore accross calls.
   */
  public SolrCore getAdminCore() {
    SolrCore core = adminCore != null ? adminCore.get() : null;
    if (core == null || core.isClosed()) {
      for (CoreDescriptor descr : this.cores.values()) {
        core = descr.getCore();
        if (core == null || core.isClosed()) {
          core = null;
        } else {
          break;
        }
      }
      setAdminCore(core);
    }
    return core;
  }

  /** 
   * Creates a MultiCoreHandler for this MultiCore.
   * @return a MultiCoreHandler
   */
  public MultiCoreHandler createMultiCoreHandler() {
    return new MultiCoreHandler() {
      @Override
      public MultiCore getMultiCore() {
        return MultiCore.this;
      }
    };
  }
 
  public MultiCoreHandler getMultiCoreHandler() {
    return multiCoreHandler;
  }
  
  public File getConfigFile() {
    return configFile;
  }
  
  /** Persists the multicore config file. */
  public void persist() {
    File tmpFile = null;
    try {
      // write in temp first
      tmpFile = File.createTempFile("multicore", ".xml", configFile.getParentFile());
      java.io.FileOutputStream out = new java.io.FileOutputStream(tmpFile);
      synchronized(cores) {
        Writer writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
        persist(writer);
        writer.flush();
        writer.close();
        out.close();
        // rename over origin or copy it it this fails
        if (tmpFile.renameTo(configFile))
          tmpFile = null;
        else
          fileCopy(tmpFile, configFile);
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
  
  /** Write the multicore configuration through a writer.*/
  void persist(Writer writer) throws IOException {
    writer.write("<?xml version='1.0' encoding='UTF-8'?>");
    writer.write("\n");
    writer.write("<multicore adminPath='");
    XML.escapeAttributeValue(adminPath, writer);
    writer.write('\'');
    if (this.libDir != null) {
      writer.write(" libDir='");
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
    
    // for all cores...(synchronized on cores by caller)
    for (Map.Entry<String, CoreDescriptor> entry : cores.entrySet()) {
      persist(writer, entry.getValue());
    }
    writer.write("</multicore>\n");
  }
  
  /** Writes the multicore configuration node for a given core. */
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
      // do the file copy
      fcin.transferTo(0, fcin.size(), fcout);
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
