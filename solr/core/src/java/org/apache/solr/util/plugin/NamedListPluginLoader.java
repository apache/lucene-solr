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

import java.util.Map;

import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.common.ConfigNode;

/**
 *
 * @since solr 1.3
 */
public class NamedListPluginLoader<T extends NamedListInitializedPlugin> extends AbstractPluginLoader<T>
{
  private final Map<String,T> registry;
  
  public NamedListPluginLoader(String name, Class<T> pluginClassType, Map<String, T> map) {
    super(name, pluginClassType);
    registry = map;
  }

  @Override
  protected void init(T plugin, ConfigNode node) throws Exception {
    plugin.init( DOMUtil.childNodesToNamedList(node) );
  }

  @Override
  protected T register(String name, T plugin) throws Exception {
    if( registry != null ) {
      return registry.put( name, plugin );
    }
    return null;
  }
}
