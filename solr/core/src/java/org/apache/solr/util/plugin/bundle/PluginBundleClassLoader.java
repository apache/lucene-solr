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

package org.apache.solr.util.plugin.bundle;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class loader that will act as SolrResourceLoader's class loader when the plugin bundles are in use.
 * It first delegates to its parent loader (typically SolrResourceLoader's initial class loader), and
 * then iterates through all plugin bundles until the class or resource is found.
 */
public class PluginBundleClassLoader extends URLClassLoader {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final SolrPf4jPluginManager manager;
  private final ClassLoader myParent;

  /**
   * Creates the class loader, taking the parent loader, plugin manager and urls as input
   * @param parent the parent class loader that will be consulted first
   * @param manager the plugin manager that knows about each plugin
   * @param urls an optional list of URLs to look up
   */
  public PluginBundleClassLoader(ClassLoader parent, SolrPf4jPluginManager manager, URL[] urls) {
    super(urls == null ? new URL[] {} : urls, parent);
    this.manager = manager;
    this.myParent = parent;
  }
  
  /**
   * Only need to override find* methods, as getClass etc will use findClass internally.
   * First ask 
   */
  @Override
  public Class<?> findClass(String name) throws ClassNotFoundException {
    try {
      if (log.isDebugEnabled()) {
        log.debug("Attempting to load class {} from URLs {}", name, getURLs());
      }
      return super.findClass(name);
    } catch (ClassNotFoundException ignored) {}
    try {
      if (log.isDebugEnabled()) {
        log.debug("Attempting to load class {} from parent loader", name);
      }
      return myParent.loadClass(name);
    } catch (ClassNotFoundException ignored) {}
    for (Map.Entry<String, ClassLoader> loader : manager.getPluginClassLoaders().entrySet()) {
      try {
        if (log.isDebugEnabled()) {
          log.debug("Attempting to load class {} from bundle {}", name, loader.getKey());
        }
        return loader.getValue().loadClass(name);
      } catch (ClassNotFoundException ignore) {}
    }
    throw new ClassNotFoundException("Class " + name + " not found in any plugin. Tried: " + manager.getPluginClassLoaders().keySet());
  }
  
  @Override
  public URL findResource(String name) {
    if (log.isDebugEnabled()) {
      log.debug("Attempting to find resource {} in URLs {}", name, getURLs());
    }
    URL url = super.findResource(name);
    if (url != null) {
      return url;
    }
    if (log.isDebugEnabled()) {
      log.debug("Attempting to find resource {} from parent", name);
    }
    url = myParent.getResource(name);
    if (url != null) {
      return url;
    }
    for (Map.Entry<String, ClassLoader> loader : manager.getPluginClassLoaders().entrySet()) {
      if (log.isDebugEnabled()) {
        log.debug("Attempting to find resource {} from bundle {}", name, loader.getKey());
      }
      url = loader.getValue().getResource(name);
      if (url != null) {
        return url;
      }
    }

    return null;
  }

  @Override
  public Enumeration<URL> findResources(String name) throws IOException {
    List<URL> resources = new ArrayList<>();
    if (log.isDebugEnabled()) {
      log.debug("Attempting to add resources {} from URLs {}", name, getURLs());
    }
    resources.addAll(Collections.list(super.findResources(name)));
    if (log.isDebugEnabled()) {
      log.debug("Attempting to add resources {} from parent", name);
    }
    resources.addAll(Collections.list(myParent.getResources(name)));
    for (Map.Entry<String, ClassLoader> loader : manager.getPluginClassLoaders().entrySet()) {
      if (log.isDebugEnabled()) {
        log.debug("Attempting to add resources {} from bundle {}", name, loader.getKey());
      }
      resources.addAll(Collections.list(loader.getValue().getResources(name)));
    }

    return Collections.enumeration(resources);
  }

}
