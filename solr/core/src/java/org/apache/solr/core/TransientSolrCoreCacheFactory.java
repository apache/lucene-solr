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

import java.lang.invoke.MethodHandles;
import java.util.Collections;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An interface that allows custom transient caches to be maintained with different implementations
 */
public abstract class TransientSolrCoreCacheFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private volatile CoreContainer coreContainer = null;

  /**
   * @return the cache holding the transient cores; never null.
   */
  public abstract TransientSolrCoreCache getTransientSolrCoreCache();
  /**
   * Create a new TransientSolrCoreCacheFactory instance
   *
   * @param loader a SolrResourceLoader used to find the TransientSolrCacheFactory classes
   * @param coreContainer CoreContainer that encloses all the Solr cores.              
   * @return a new, initialized TransientSolrCoreCache instance
   */

  public static TransientSolrCoreCacheFactory newInstance(SolrResourceLoader loader, CoreContainer coreContainer) {
    PluginInfo info = coreContainer.getConfig().getTransientCachePluginInfo();
    if (info == null) { // definition not in our solr.xml file, use default
      info = DEFAULT_TRANSIENT_SOLR_CACHE_INFO;
    }

    try {
      // According to the docs, this returns a TransientSolrCoreCacheFactory with the default c'tor
      TransientSolrCoreCacheFactory tccf = loader.findClass(info.className, TransientSolrCoreCacheFactory.class).newInstance(); 
      
      // OK, now we call its init method.
      if (PluginInfoInitialized.class.isAssignableFrom(tccf.getClass()))
        PluginInfoInitialized.class.cast(tccf).init(info);
      tccf.setCoreContainer(coreContainer);
      return tccf;
    } catch (Exception e) {
      // Many things could cause this, bad solrconfig, mis-typed class name, whatever.
      // Throw an exception to stop loading here; never return null.
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error instantiating "
              + TransientSolrCoreCacheFactory.class.getName() + " class [" + info.className + "]", e);
    }
  }

  public static final PluginInfo DEFAULT_TRANSIENT_SOLR_CACHE_INFO =
      new PluginInfo("transientSolrCoreCacheFactory",
          ImmutableMap.of("class", TransientSolrCoreCacheFactoryDefault.class.getName(), 
              "name", TransientSolrCoreCacheFactory.class.getName()),
          null, Collections.<PluginInfo>emptyList());


  // Need this because the plugin framework doesn't require a PluginINfo in the init method, don't see a way to
  // pass additional parameters and we need this when we create the transient core cache, it's _really_ important.
  public void setCoreContainer(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  public CoreContainer getCoreContainer() {
    return coreContainer;
  }
}
