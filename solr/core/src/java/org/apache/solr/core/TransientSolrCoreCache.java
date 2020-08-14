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


import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.http.annotation.Experimental;

/**
 * The base class for custom transient core maintenance. Any custom plugin that want's to take control of transient
 * caches (i.e. any core defined with transient=true) should override this class.
 *
 * Register your plugin in solr.xml similarly to:
 *
 *   &lt;transientCoreCacheFactory name="transientCoreCacheFactory" class="TransientSolrCoreCacheFactoryDefault"&gt;
 *        &lt;int name="transientCacheSize"&gt;4&lt;/int&gt;
 *   &lt;/transientCoreCacheFactory&gt;
 *
 *
 * WARNING: There is quite a bit of higher-level locking done by the CoreContainer to avoid various race conditions
 *          etc. You should _only_ manipulate them within the method calls designed to change them. E.g.
 *          only add to the transient core descriptors in addTransientDescriptor etc.
 *          
 *          Trust the higher-level code (mainly SolrCores and CoreContainer) to call the appropriate operations when
 *          necessary and to coordinate shutting down cores, manipulating the internal structures and the like..
 *          
 *          The only real action you should _initiate_ is to close a core for whatever reason, and do that by 
 *          calling notifyCoreCloseListener(coreToClose); The observer will call back to removeCore(name) at the appropriate 
 *          time. There is no need to directly remove the core _at that time_ from the transientCores list, a call
 *          will come back to this class when CoreContainer is closing this core.
 *          
 *          CoreDescriptors are read-once. During "core discovery" all valid descriptors are enumerated and added to
 *          the appropriate list. Thereafter, they are NOT re-read from disk. In those situations where you want
 *          to re-define the coreDescriptor, maintain a "side list" of changed core descriptors. Then override
 *          getTransientDescriptor to return your new core descriptor. NOTE: assuming you've already closed the
 *          core, the _next_ time that core is required getTransientDescriptor will be called and if you return the
 *          new core descriptor your re-definition should be honored. You'll have to maintain this list for the
 *          duration of this Solr instance running. If you persist the coreDescriptor, then next time Solr starts
 *          up the new definition will be read.
 *          
 *
 *  If you need to manipulate the return, for instance block a core from being loaded for some period of time, override
 *  say getTransientDescriptor and return null.
 *  
 *  In particular, DO NOT reach into the transientCores structure from a method called to manipulate core descriptors
 *  or vice-versa.
 */
public abstract class TransientSolrCoreCache {

  // Gets the core container that encloses this cache.
  public abstract CoreContainer getContainer();

  // Add the newly-opened core to the list of open cores.
  public abstract SolrCore addCore(String name, SolrCore core);

  // Return the names of all possible cores, whether they are currently loaded or not.
  public abstract Set<String> getAllCoreNames();
  
  // Return the names of all currently loaded cores
  public abstract Set<String> getLoadedCoreNames();

  // Remove a core from the internal structures, presumably it 
  // being closed. If the core is re-opened, it will be readded by CoreContainer.
  public abstract SolrCore removeCore(String name);

  // Get the core associated with the name. Return null if you don't want this core to be used.
  public abstract SolrCore getCore(String name);

  // reutrn true if the cache contains the named core.
  public abstract boolean containsCore(String name);
  
  // This method will be called when the container is to be shut down. It should return all
  // transient solr cores and clear any internal structures that hold them.
  public abstract Collection<SolrCore> prepareForShutdown();

  // These methods allow the implementation to maintain control over the core descriptors.
  
  // This method will only be called during core discovery at startup.
  public abstract void addTransientDescriptor(String rawName, CoreDescriptor cd);
  
  // This method is used when opening cores and the like. If you want to change a core's descriptor, override this
  // method and return the current core descriptor.
  public abstract CoreDescriptor getTransientDescriptor(String name);


  // Remove the core descriptor from your list of transient descriptors.
  public abstract CoreDescriptor removeTransientDescriptor(String name);

  // Find all the names a specific core is mapped to. Should not return null, return empty set instead.
  @Experimental
  public List<String> getNamesForCore(SolrCore core) {
    return Collections.emptyList();
  }
  
  /**
   * Must be called in order to free resources!
   */
  public  void close(){
    // Nothing to do currently
  };


  // These two methods allow custom implementations to communicate arbitrary information as necessary.
  public abstract int getStatus(String coreName);
  public abstract void setStatus(String coreName, int status);
}


  
