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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.DOMUtil;
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
  private static Logger log = Logger.getLogger(MultiCore.class.getName());
  private static final MultiCore instance = new MultiCore();
  
  // Synchronized map of all cores
  private final Map<String, SolrCore> cores =
      Collections.synchronizedMap( new HashMap<String, SolrCore>() );
  
  private SolrCore defaultCore = null;
  private boolean enabled = false;
  private boolean persistent = false;
  private String adminPath = null;
  private MultiCoreHandler multiCoreHandler = null;
  private File configFile = null;
  private String libDir = null;
  private ClassLoader libLoader = null;
  
  // no one else can make the registry
  private MultiCore() { }
  
  //-------------------------------------------------------------------
  // Initialization / Cleanup
  //-------------------------------------------------------------------
  
  /**
   * Load a config file listing the available solr cores
   */
  public void load(String dir, File configFile ) throws ParserConfigurationException, IOException, SAXException {
    this.configFile = configFile;
    Config cfg = new Config( new SolrResourceLoader(dir), 
        null, new FileInputStream( configFile ), null );
    
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
      multiCoreHandler = new MultiCoreHandler();
    }
    
    boolean hasDefault = false;
    NodeList nodes = (NodeList)cfg.evaluate("multicore/core", XPathConstants.NODESET);
    for (int i=0; i<nodes.getLength(); i++) {
      Node node = nodes.item(i);
      
      try {
        String name         = DOMUtil.getAttr(node,"name", "Core needs a name" );
        String instanceDir  = DOMUtil.getAttr(node,"instanceDir", "Missing required 'instanceDir'" );
        String defaultStr   = DOMUtil.getAttr(node,"default", null );
        
        // Make the instanceDir relative to the core config
        File idir = new File( dir, instanceDir );
        instanceDir = idir.getPath();
        
        // Initialize the solr config
        SolrResourceLoader solrLoader = new SolrResourceLoader(instanceDir, libLoader);
        SolrConfig solrConfig = new SolrConfig( solrLoader, SolrConfig.DEFAULT_CONF_FILE, null );
        IndexSchema schema = new IndexSchema(solrConfig, instanceDir+"/conf/schema.xml");
        SolrCore core = new SolrCore( name, null, solrConfig, schema );
        
        // Register the new core
        SolrCore old = this.register( core );
        if( old != null ) {
          throw new RuntimeException( cfg.getName() +
                  " registers multiple cores to the same name: "+name );
        }
        
        if( "true".equalsIgnoreCase( defaultStr ) ) {
          if( hasDefault ) {
            throw new RuntimeException( 
                "multicore.xml defines multiple default cores. "+
                getDefaultCore().getName() + " and " + core.getName() );
          }
          this.setDefaultCore( core );
          hasDefault = true;
        }
      } 
      catch( Throwable ex ) {
        SolrConfig.severeErrors.add( ex );
        SolrException.logOnce(log,null,ex);
      }
    }
    
    if( !hasDefault ) {
      throw new RuntimeException( 
          "multicore.xml must define at least one default core" );
    }
    enabled = true;
  }
  
  /** Stops all cores. */
  public void shutdown() {
    synchronized(cores) {
      for( SolrCore core : cores.values() ) {
        core.close();
      }
      cores.clear();
    }
  }
  
  @Override
  protected void finalize() {
    shutdown();
  }
  
  //-------------------------------------------------------------------
  //
  //-------------------------------------------------------------------
  
  /** Get the singleton */
  public static MultiCore getRegistry() {
    return instance;
  }
  
  public SolrCore register( SolrCore core ) {
    if( core == null ) {
      throw new RuntimeException( "Can not register a null core." );
    }
    String name = core.getName();
    if( name == null || name.length() == 0 ) {
      throw new RuntimeException( "Invalid core name." );
    }
    SolrCore old = cores.put(name, core);
    if( old == null ) {
      log.info( "registering core: "+name );
    } else {
      log.info( "replacing core: "+name );
    }
    return old;
  }

  /**
   * While the new core is loading, requests will continue to be dispatched to
   * and processed by the old core
   * 
   * @param core
   * @throws ParserConfigurationException
   * @throws IOException
   * @throws SAXException
   */
  public void reload(SolrCore core) throws ParserConfigurationException, IOException, SAXException 
  {
    boolean wasDefault = (core==defaultCore);
    
    SolrResourceLoader loader = new SolrResourceLoader( core.getResourceLoader().getInstanceDir() );
    SolrConfig config = new SolrConfig( loader, core.getConfigFile(), null );
    IndexSchema schema = new IndexSchema( config, core.getSchemaFile() );
    SolrCore loaded = new SolrCore( core.getName(), core.getDataDir(), config, schema );
    this.register( loaded );
    if( wasDefault ) {
      this.setDefaultCore( loaded );
    }
    
    // TODO? -- add some kind of hook to close the core after all references are 
    // gone...  is finalize() enough?
  }
  
  public void setDefaultCore( SolrCore core )
  {
    defaultCore = core;
    cores.put( null, core );
    cores.put( "", core );
  }
  
  public SolrCore getDefaultCore() {
    return defaultCore;
  }
  
  /**
   * @return a Collection of registered SolrCores
   */
  public Collection<SolrCore> getCores() {
    ArrayList<SolrCore> c = new ArrayList<SolrCore>(cores.size());
    for( Map.Entry<String, SolrCore> entry : cores.entrySet() ) {
      if( entry.getKey() != null && entry.getKey().length() > 0 ) {
        c.add( entry.getValue() );
      }
    }
    return c;
  }
  
  public SolrCore getCore(String name) {
    return cores.get( name );
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
  
  public MultiCoreHandler getMultiCoreHandler() {
    return multiCoreHandler;
  }
  
  public File getConfigFile() {
    return configFile;
  }

}
