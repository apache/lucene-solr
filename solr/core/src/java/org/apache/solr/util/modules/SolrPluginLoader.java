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

package org.apache.solr.util.modules;

import java.nio.file.Path;

import ro.fortsoft.pf4j.DefaultPluginLoader;
import ro.fortsoft.pf4j.PluginClassLoader;
import ro.fortsoft.pf4j.PluginClasspath;
import ro.fortsoft.pf4j.PluginDescriptor;
import ro.fortsoft.pf4j.PluginManager;

/**
 * Loads plugins with our own class loader as parent
 */
public class SolrPluginLoader extends DefaultPluginLoader {
  private ClassLoader parentClassLoader;

  public SolrPluginLoader(PluginManager pluginManager, PluginClasspath pluginClasspath, ClassLoader parentClassLoader) {
    super(pluginManager, pluginClasspath);
    this.parentClassLoader = parentClassLoader;
  }

  @Override
  protected PluginClassLoader createPluginClassLoader(Path pluginPath, PluginDescriptor pluginDescriptor) {
    return new PluginClassLoader(pluginManager, pluginDescriptor, parentClassLoader);
  }
}
