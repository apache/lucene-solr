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
import java.util.Set;
import java.util.stream.Collectors;

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
 *          calling notifyObservers(coreToClose); The observer will call back to removeCore(name) at the appropriate 
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

  /** Gets the core container that encloses this cache. */
  public abstract CoreContainer getContainer();

  /** Adds the newly-opened core to the list of open cores. */
  public abstract SolrCore addCore(String name, SolrCore core);

  /** Returns the names of all possible cores, whether they are currently loaded or not. */
  public abstract Set<String> getAllCoreNames();

  /** Returns the names of all currently loaded cores. */
  public abstract Set<String> getLoadedCoreNames();

  /**
   * Removes a core from the internal structures, presumably it being closed. If the core
   * is re-opened, it will be re-added by CoreContainer.
   */
  public abstract SolrCore removeCore(String name);

  /** Gets the core associated with the name. Returns null if there is none. */
  public abstract SolrCore getCore(String name);

  /** Returns whether the cache contains the named core. */
  public abstract boolean containsCore(String name);

  /**
   * This method will be called when the container is to be shut down. It returns
   * all transient solr cores and clear any internal structures that hold them.
   */
  public abstract Collection<SolrCore> prepareForShutdown();

  // These methods allow the implementation to maintain control over the core descriptors.

  /**
   * Adds a new {@link CoreDescriptor}.
   * This method will only be called during core discovery at startup.
   */
  public abstract void addTransientDescriptor(String rawName, CoreDescriptor cd);

  /**
   * Gets the {@link CoreDescriptor} for a transient core (loaded or unloaded).
   * This method is used when opening cores and the like. If you want to change a core's descriptor,
   * override this method and return the current core descriptor.
   */
  public abstract CoreDescriptor getTransientDescriptor(String name);

  /**
   * Gets the {@link CoreDescriptor} for all transient cores (loaded and unloaded).
   */
  // This method will become abstract in the next major release.
  public Collection<CoreDescriptor> getTransientDescriptors() {
    return getAllCoreNames().stream().map(this::getTransientDescriptor).collect(Collectors.toList());
  }

  /**
   * Removes a {@link CoreDescriptor} from the list of transient cores descriptors.
   */
  public abstract CoreDescriptor removeTransientDescriptor(String name);

  /**
   * Called in order to free resources.
   */
  public void close() {
    // Nothing to do for now
  }

  /**
   * Gets a custom status for the given core name.
   * Allows custom implementations to communicate arbitrary information as necessary.
   */
  public abstract int getStatus(String coreName);

  /**
   * Sets a custom status for the given core name.
   * Allows custom implementations to communicate arbitrary information as necessary.
   */
  public abstract void setStatus(String coreName, int status);
}


  
