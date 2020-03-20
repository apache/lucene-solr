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
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransientSolrCoreCacheDefault extends TransientSolrCoreCache {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private int cacheSize = NodeConfig.NodeConfigBuilder.DEFAULT_TRANSIENT_CACHE_SIZE;

  protected CoreContainer coreContainer;

  protected final Map<String, CoreDescriptor> transientDescriptors = new LinkedHashMap<>();

  //WARNING! The _only_ place you put anything into the list of transient cores is with the putTransientCore method!
  protected Map<String, SolrCore> transientCores = new LinkedHashMap<>(); // For "lazily loaded" cores

  /**
   * @param container The enclosing CoreContainer. It allows us to access everything we need.
   */
  public TransientSolrCoreCacheDefault(final CoreContainer container) {
    this.coreContainer = container;

    NodeConfig cfg = container.getNodeConfig();
    if (cfg.getTransientCachePluginInfo() == null) {
      // Still handle just having transientCacheSize defined in the body of solr.xml  not in a transient handler clause.
      // deprecate this for 7.0?
      this.cacheSize = cfg.getTransientCacheSize();
    } else {
      NamedList args = cfg.getTransientCachePluginInfo().initArgs;
      Object obj = args.get("transientCacheSize");
      if (obj != null) {
        this.cacheSize = (int) obj;
      }
    }
    doInit();
  }
  // This just moves the 
  private void doInit() {
    NodeConfig cfg = coreContainer.getNodeConfig();
    if (cfg.getTransientCachePluginInfo() == null) {
      // Still handle just having transientCacheSize defined in the body of solr.xml not in a transient handler clause.
      this.cacheSize = cfg.getTransientCacheSize();
    } else {
      NamedList args = cfg.getTransientCachePluginInfo().initArgs;
      Object obj = args.get("transientCacheSize");
      if (obj != null) {
        this.cacheSize = (int) obj;
      }
    }

    log.info("Allocating transient cache for {} transient cores", cacheSize);
    // it's possible for cache
    if (cacheSize < 0) { // Trap old flag
      cacheSize = Integer.MAX_VALUE;
    }
    // Now don't allow ridiculous allocations here, if the size is > 1,000, we'll just deal with
    // adding cores as they're opened. This blows up with the marker value of -1.
    transientCores = new LinkedHashMap<String, SolrCore>(Math.min(cacheSize, 1000), 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<String, SolrCore> eldest) {
        if (size() > cacheSize) {
          SolrCore coreToClose = eldest.getValue();
          log.info("Closing transient core [{}]", coreToClose.getName());
          coreContainer.queueCoreToClose(coreToClose);
          return true;
        }
        return false;
      }
    };
  }

  
  @Override
  public Collection<SolrCore> prepareForShutdown() {
    // Returna copy of the values
    List<SolrCore> ret = new ArrayList(transientCores.values());
    transientCores.clear();
    return ret;
  }

  @Override
  public CoreContainer getContainer() { return this.coreContainer; }

  @Override
  public SolrCore addCore(String name, SolrCore core) {
    return transientCores.put(name, core);
  }

  @Override
  public Set<String> getAllCoreNames() {
    return transientDescriptors.keySet();
  }
  
  @Override
  public Set<String> getLoadedCoreNames() {
    return transientCores.keySet();
  }

  // Remove a core from the internal structures, presumably it 
  // being closed. If the core is re-opened, it will be readded by CoreContainer.
  @Override
  public SolrCore removeCore(String name) {
    return transientCores.remove(name);
  }

  // Get the core associated with the name. Return null if you don't want this core to be used.
  @Override
  public SolrCore getCore(String name) {
    return transientCores.get(name);
  }

  @Override
  public boolean containsCore(String name) {
    return transientCores.containsKey(name);
  }

  // These methods allow the implementation to maintain control over the core descriptors.


  // This method will only be called during core discovery at startup.
  @Override
  public void addTransientDescriptor(String rawName, CoreDescriptor cd) {
    transientDescriptors.put(rawName, cd);
  }

  // This method is used when opening cores and the like. If you want to change a core's descriptor, override this
  // method and return the current core descriptor.
  @Override
  public CoreDescriptor getTransientDescriptor(String name) {
    return transientDescriptors.get(name);
  }

  @Override
  public CoreDescriptor removeTransientDescriptor(String name) {
    return transientDescriptors.remove(name);
  }

  @Override
  public List<String> getNamesForCore(SolrCore core) {
    List<String> ret = new ArrayList<>();
    for (Map.Entry<String, SolrCore> entry : transientCores.entrySet()) {
      if (core == entry.getValue()) {
        ret.add(entry.getKey());
      }
    }
    return ret;
  }

  // For custom implementations to communicate arbitrary information as necessary.
  @Override
  public int getStatus(String coreName) { return 0; } //no_op for default handler.

  @Override
  public void setStatus(String coreName, int status) {} //no_op for default handler.

}
