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

package org.apache.solr.util.plugin;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.core.Config;
import org.apache.solr.core.SolrConfig;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * An abstract super class that manages standard solr-style plugin configuration.
 * 
 * @version $Id$
 * @since solr 1.3
 */
public abstract class AbstractPluginLoader<T>
{
  public static Logger log = Logger.getLogger(AbstractPluginLoader.class.getName());
  
  private final String type;
  private final boolean preRegister;
  
  /**
   * @param type is the 'type' name included in error messages.
   * @param preRegister, if true, this will first register all Plugins, then it will initialize them.
   */
  public AbstractPluginLoader( String type, boolean preRegister )
  {
    this.type = type;
    this.preRegister = preRegister;
  }

  public AbstractPluginLoader( String type )
  {
    this( type, false );
  }
  
  /**
   * Where to look for classes
   */
  protected String[] getDefaultPackages()
  {
    return new String[]{};
  }
  
  /**
   * Create a plugin from an XML configuration.  Plugins are defined using:
   *   <plugin name="name1" class="solr.ClassName">
   *      ...
   *   </plugin>
   * 
   * @param name - The registered name.  In the above example: "name1"
   * @param className - class name for requested plugin.  In the above example: "solr.ClassName"
   * @param node - the XML node defining this plugin
   */
  @SuppressWarnings("unchecked")
  protected T create( String name, String className, Node node ) throws Exception
  {
    return (T) Config.newInstance( className, getDefaultPackages() );
  }
  
  /**
   * Register a plugin with a given name.
   * @return The plugin previously registered to this name, or null
   */
  abstract protected T register( String name, T plugin ) throws Exception;

  /**
   * Initialize the plugin.  
   * 
   * @param plugin - the plugin to initialize
   * @param node - the XML node defining this plugin
   */
  abstract protected void init( T plugin, Node node ) throws Exception;

  /**
   * Given a NodeList from XML in the form:
   * 
   *  <plugins>
   *    <plugin name="name1" class="solr.ClassName" >
   *      ...
   *    </plugin>
   *    <plugin name="name2" class="solr.ClassName" >
   *      ...
   *    </plugin>
   *  </plugins>
   * 
   * This will initialize and register each plugin from the list.  A class will 
   * be generated for each class name and registered to the given name.
   * 
   * If 'preRegister' is true, each plugin will be registered *before* it is initialized
   * This may be useful for implementations that need to inspect other registered 
   * plugins at startup.
   * 
   * One (and only one) plugin may declare itself to be the 'default' plugin using:
   *    <plugin name="name2" class="solr.ClassName" default="true">
   * If a default element is defined, it will be returned from this function.
   * 
   */
  public T load( NodeList nodes )
  {
    List<PluginInitInfo> info = new ArrayList<PluginInitInfo>();
    T defaultPlugin = null;
    
    if (nodes !=null ) {
      for (int i=0; i<nodes.getLength(); i++) {
        Node node = nodes.item(i);
  
        // In a production environment, we can tolerate an error in some request handlers, 
        // still load the others, and have a working system.
        try {
          String name       = DOMUtil.getAttr(node,"name", type);
          String className  = DOMUtil.getAttr(node,"class", type);
          String defaultStr = DOMUtil.getAttr(node,"default", null );
            
          T plugin = create(name, className, node );
          log.info("created "+name+": " + plugin.getClass().getName() );
          
          // Either initialize now or wait till everything has been registered
          if( preRegister ) {
            info.add( new PluginInitInfo( plugin, node ) );
          }
          else {
            init( plugin, node );
          }
          
          T old = register( name, plugin );
          if( old != null ) {
            throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
                "Multiple "+type+" registered to the same name: "+name+" ignoring: "+old );
          }
          
          if( defaultStr != null && Boolean.parseBoolean( defaultStr ) ) {
            if( defaultPlugin != null ) {
              throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
                "Multiple default "+type+" plugins: "+defaultPlugin + " AND " + name );
            }
            defaultPlugin = plugin;
          }
        }
        catch (Exception e) {
          SolrConfig.severeErrors.add( e );
          SolrException.logOnce(log,null,e);
        }
      }
    }
    
    // If everything needs to be registered *first*, this will initialize later
    for( PluginInitInfo pinfo : info ) {
      try {
        init( pinfo.plugin, pinfo.node );
      }
      catch( Exception ex ) {
        SolrConfig.severeErrors.add( ex );
        SolrException.logOnce(log,null,ex);
      }
    }
    return defaultPlugin;
  }
  

  /**
   * Internal class to hold onto initialization info so that it can be initialized 
   * after it is registered.
   */
  private class PluginInitInfo
  {
    final T plugin;
    final Node node;
    
    PluginInitInfo( T plugin, Node node )
    {
      this.plugin = plugin;
      this.node = node;
    }
  }
}
