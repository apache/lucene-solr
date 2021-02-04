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
package org.apache.solr.util.plugin;

import javax.xml.xpath.XPath;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import net.sf.saxon.om.NodeInfo;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.DOMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * An abstract super class that manages standard solr-style plugin configuration.
 * 
 *
 * @since solr 1.3
 */
public abstract class AbstractPluginLoader<T>
{
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String[] EMPTY_STRINGS = {};

  private final String type;
  private final boolean preRegister;
  private final boolean requireName;
  private final Class<T> pluginClassType;
  
  /**
   * @param type is the 'type' name included in error messages.
   * @param preRegister if true, this will first register all Plugins, then it will initialize them.
   */
  public AbstractPluginLoader(String type, Class<T> pluginClassType, boolean preRegister, boolean requireName )
  {
    this.type = type;
    this.pluginClassType = pluginClassType;
    this.preRegister = preRegister;
    this.requireName = requireName;
  }

  public AbstractPluginLoader(String type, Class<T> pluginClassType)
  {
    this(type, pluginClassType, false, true);
  }
  
  /**
   * Where to look for classes
   */
  protected String[] getDefaultPackages()
  {
    return EMPTY_STRINGS;
  }
  
  /**
   * Create a plugin from an XML configuration.  Plugins are defined using:
   * <pre class="prettyprint">
   * {@code
   * <plugin name="name1" class="solr.ClassName">
   *      ...
   * </plugin>}
   * </pre>
   * 
   * @param name - The registered name.  In the above example: "name1"
   * @param className - class name for requested plugin.  In the above example: "solr.ClassName"
   * @param node - the XML node defining this plugin
   */
  @SuppressWarnings("unchecked")
//  protected T create( SolrResourceLoader loader, String name, String className, Node node, XPath xpath) throws Exception
//  {
//    return loader.newInstance(className, pluginClassType, getDefaultPackages());
//  }

  protected T create( SolrResourceLoader loader, String name, String className, NodeInfo node, XPath xpath) throws Exception
  {
    return loader.newInstance(className, pluginClassType, getDefaultPackages());
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
  abstract protected void init( T plugin, NodeInfo node ) throws Exception;

  /**
   * Initializes and registers each plugin in the list.
   * Given a NodeList from XML in the form:
   * <pre class="prettyprint">
   * {@code
   * <plugins>
   *    <plugin name="name1" class="solr.ClassName" >
   *      ...
   *    </plugin>
   *    <plugin name="name2" class="solr.ClassName" >
   *      ...
   *    </plugin>
   * </plugins>}
   * </pre>
   * 
   * This will initialize and register each plugin from the list.  A class will 
   * be generated for each class name and registered to the given name.
   * 
   * If 'preRegister' is true, each plugin will be registered *before* it is initialized
   * This may be useful for implementations that need to inspect other registered 
   * plugins at startup.
   * 
   * One (and only one) plugin may declare itself to be the 'default' plugin using:
   * <pre class="prettyprint">
   * {@code
   *    <plugin name="name2" class="solr.ClassName" default="true">}
   * </pre>
   * If a default element is defined, it will be returned from this function.
   * 
   */
  public T load( SolrResourceLoader loader, ArrayList<NodeInfo> nodes )
  {
    List<PluginInitInfo> info = new ArrayList<>();
    AtomicReference<T> defaultPlugin = new AtomicReference<>();
    XPath xpath = loader.getXPath();
    if (nodes !=null ) {
      for (int i=0; i<nodes.size(); i++) {
        try (ParWork parWork = new ParWork(this, false, false)) {
          NodeInfo node = nodes.get(i);

          String name = null;
          try {
            name = DOMUtil.getAttr(node, NAME, requireName ? type : null);
            String className = DOMUtil.getAttr(node, "class", null);
            String defaultStr = DOMUtil.getAttr(node, "default", null);

            if (Objects.isNull(className) && Objects.isNull(name)) {
              throw new RuntimeException(type + ": missing mandatory attribute 'class' or 'name'");
            }

            String finalName = name;
            parWork.collect(name, ()->{
              try {
                T plugin = create(loader, finalName, className, node, xpath);

                if (log.isTraceEnabled()) log.trace("created {}: {}", ((finalName != null) ? finalName : ""), plugin.getClass().getName());

                // Either initialize now or wait till everything has been registered
                if (preRegister) {
                  info.add(new PluginInitInfo(plugin, node));
                } else {
                  init(plugin, node);
                }

                T old = register(finalName, plugin);
                if (old != null && !(finalName == null && !requireName)) {
                  throw new SolrException(ErrorCode.SERVER_ERROR, "Multiple " + type + " registered to the same name: " + finalName + " ignoring: " + old);
                }

                if (defaultStr != null && Boolean.parseBoolean(defaultStr)) {
                  if (defaultPlugin.get() != null) {
                    throw new SolrException(ErrorCode.SERVER_ERROR, "Multiple default " + type + " plugins: " + defaultPlugin + " AND " + finalName);
                  }
                  defaultPlugin.set(plugin);
                }
              } catch (Exception e) {
                if (e instanceof RuntimeException) {
                  throw (RuntimeException) e;
                } else {
                  throw new SolrException(ErrorCode.SERVER_ERROR, e);
                }
              }
            });

          } catch (Exception ex) {
            ParWork.propagateInterrupt(ex);
            SolrException e = new SolrException(ErrorCode.SERVER_ERROR, "Plugin init failure for " + type + (null != name ? (" \"" + name + "\"") : "") + ": " + ex.getMessage(), ex);
            throw e;
          }
        }
      }
    }
      
    // If everything needs to be registered *first*, this will initialize later
    for( PluginInitInfo pinfo : info ) {
      try {
        init( pinfo.plugin, pinfo.node );
      }
      catch( Exception ex ) {
        SolrException e = new SolrException
          (ErrorCode.SERVER_ERROR, "Plugin Initializing failure for " + type, ex);
        throw e;
      }
    }
    return defaultPlugin.get();
  }
  
  /**
   * Initializes and registers a single plugin.
   * 
   * Given a NodeList from XML in the form:
   * <pre class="prettyprint">
   * {@code
   * <plugin name="name1" class="solr.ClassName" > ... </plugin>}
   * </pre>
   * 
   * This will initialize and register a single plugin. A class will be
   * generated for the plugin and registered to the given name.
   * 
   * If 'preRegister' is true, the plugin will be registered *before* it is
   * initialized This may be useful for implementations that need to inspect
   * other registered plugins at startup.
   * 
   * The created class for the plugin will be returned from this function.
   * 
   */
  public T loadSingle(SolrResourceLoader loader, NodeInfo node) {
    List<PluginInitInfo> info = new ArrayList<>();
    T plugin = null;

    try {
      String name = DOMUtil.getAttr(node, NAME, requireName ? type : null);
      String className = DOMUtil.getAttr(node, "class", type);
      plugin = create(loader, name, className, node, loader.getXPath());
      if (log.isDebugEnabled()) {
        log.debug("created {}: {}", name, plugin.getClass().getName());
      }

      // Either initialize now or wait till everything has been registered
      if (preRegister) {
        info.add(new PluginInitInfo(plugin, node));
      } else {
        init(plugin, node);
      }

      T old = register(name, plugin);
      if (old != null && !(name == null && !requireName)) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Multiple " + type + " registered to the same name: " + name
                + " ignoring: " + old);
      }

    } catch (Exception ex) {
      ParWork.propagateInterrupt(ex);
      SolrException e = new SolrException
        (ErrorCode.SERVER_ERROR, "Plugin init failure for " + type, ex);
      throw e;
    }

    // If everything needs to be registered *first*, this will initialize later
    for (PluginInitInfo pinfo : info) {
      try {
        init(pinfo.plugin, pinfo.node);
      } catch (Exception ex) {
        ParWork.propagateInterrupt(ex);
        SolrException e = new SolrException
          (ErrorCode.SERVER_ERROR, "Plugin init failure for " + type, ex);
        throw e;
      }
    }
    return plugin;
  }
  

  /**
   * Internal class to hold onto initialization info so that it can be initialized 
   * after it is registered.
   */
  private class PluginInitInfo {
    final T plugin;
    final NodeInfo node;

   // final Node domNode;


    PluginInitInfo(T plugin, NodeInfo node) {
      this.plugin = plugin;
      this.node = node; // nocommit
   //   this.domNode = null;
    }

//    PluginInitInfo(T plugin, Node node) {
//      this.plugin = plugin;
//      this.domNode = node; // nocommit
//      this. node = null;
//    }
  }
}
