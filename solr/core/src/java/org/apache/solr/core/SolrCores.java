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

import com.google.common.collect.Lists;
import org.apache.http.annotation.Experimental;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


class SolrCores {

  private static Object modifyLock = new Object(); // for locking around manipulating any of the core maps.
  private final Map<String, SolrCore> cores = new LinkedHashMap<>(); // For "permanent" cores

  // These descriptors, once loaded, will _not_ be unloaded, i.e. they are not "transient".
  private final Map<String, CoreDescriptor> residentDesciptors = new LinkedHashMap<>();

  private final CoreContainer container;
  
  private Set<String> currentlyLoadingCores = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // This map will hold objects that are being currently operated on. The core (value) may be null in the case of
  // initial load. The rule is, never to any operation on a core that is currently being operated upon.
  private static final Set<String> pendingCoreOps = new HashSet<>();

  // Due to the fact that closes happen potentially whenever anything is _added_ to the transient core list, we need
  // to essentially queue them up to be handled via pendingCoreOps.
  private static final List<SolrCore> pendingCloses = new ArrayList<>();

  private TransientSolrCoreCacheFactory transientCoreCache;

  private TransientSolrCoreCache transientSolrCoreCache = null;
  
  SolrCores(CoreContainer container) {
    this.container = container;
  }
  
  protected void addCoreDescriptor(CoreDescriptor p) {
    synchronized (modifyLock) {
      if (p.isTransient()) {
        if (getTransientCacheHandler() != null) {
          getTransientCacheHandler().addTransientDescriptor(p.getName(), p);
        } else {
          log.warn("We encountered a core marked as transient, but there is no transient handler defined. This core will be inaccessible");
        }
      } else {
        residentDesciptors.put(p.getName(), p);
      }
    }
  }

  protected void removeCoreDescriptor(CoreDescriptor p) {
    synchronized (modifyLock) {
      if (p.isTransient()) {
        if (getTransientCacheHandler() != null) {
          getTransientCacheHandler().removeTransientDescriptor(p.getName());
        }
      } else {
        residentDesciptors.remove(p.getName());
      }
    }
  }

  public void load(SolrResourceLoader loader) {
    transientCoreCache = TransientSolrCoreCacheFactory.newInstance(loader, container);
  }
  // We are shutting down. You can't hold the lock on the various lists of cores while they shut down, so we need to
  // make a temporary copy of the names and shut them down outside the lock.
  protected void close() {
    waitForLoadingCoresToFinish(30*1000);
    Collection<SolrCore> coreList = new ArrayList<>();

    
    TransientSolrCoreCache transientSolrCoreCache = getTransientCacheHandler();
    // Release observer
    if (transientSolrCoreCache != null) {
      transientSolrCoreCache.close();
    }

    // It might be possible for one of the cores to move from one list to another while we're closing them. So
    // loop through the lists until they're all empty. In particular, the core could have moved from the transient
    // list to the pendingCloses list.
    do {
      coreList.clear();
      synchronized (modifyLock) {
        // make a copy of the cores then clear the map so the core isn't handed out to a request again
        coreList.addAll(cores.values());
        cores.clear();
        if (transientSolrCoreCache != null) {
          coreList.addAll(transientSolrCoreCache.prepareForShutdown());
        }

        coreList.addAll(pendingCloses);
        pendingCloses.clear();
      }
      
      ExecutorService coreCloseExecutor = ExecutorUtil.newMDCAwareFixedThreadPool(Integer.MAX_VALUE,
          new DefaultSolrThreadFactory("coreCloseExecutor"));
      try {
        for (SolrCore core : coreList) {
          coreCloseExecutor.submit(() -> {
            MDCLoggingContext.setCore(core);
            try {
              core.close();
            } catch (Throwable e) {
              SolrException.log(log, "Error shutting down core", e);
              if (e instanceof Error) {
                throw (Error) e;
              }
            } finally {
              MDCLoggingContext.clear();
            }
            return core;
          });
        }
      } finally {
        ExecutorUtil.shutdownAndAwaitTermination(coreCloseExecutor);
      }

    } while (coreList.size() > 0);
  }
  
  // Returns the old core if there was a core of the same name.
  //WARNING! This should be the _only_ place you put anything into the list of transient cores!
  protected SolrCore putCore(CoreDescriptor cd, SolrCore core) {
    synchronized (modifyLock) {
      if (cd.isTransient()) {
        if (getTransientCacheHandler() != null) {
          return getTransientCacheHandler().addCore(cd.getName(), core);
        }
      } else {
        return cores.put(cd.getName(), core);
      }
    }
    return null;
  }

  /**
   *
   * @return A list of "permanent" cores, i.e. cores that  may not be swapped out and are currently loaded.
   * 
   * A core may be non-transient but still lazily loaded. If it is "permanent" and lazy-load _and_
   * not yet loaded it will _not_ be returned by this call.
   * 
   * Note: This is one of the places where SolrCloud is incompatible with Transient Cores. This call is used in 
   * cancelRecoveries, transient cores don't participate.
   */

  List<SolrCore> getCores() {

    synchronized (modifyLock) {
      List<SolrCore> lst = new ArrayList<>(cores.values());
      return lst;
    }
  }

  /**
   * Gets the cores that are currently loaded, i.e. cores that have
   * 1> loadOnStartup=true and are either not-transient or, if transient, have been loaded and have not been aged out
   * 2> loadOnStartup=false and have been loaded but either non-transient or have not been aged out.
   * 
   * Put another way, this will not return any names of cores that are lazily loaded but have not been called for yet
   * or are transient and either not loaded or have been swapped out.
   * 
   * @return List of currently loaded cores.
   */
  Set<String> getLoadedCoreNames() {
    Set<String> set;

    synchronized (modifyLock) {
      set = new TreeSet<>(cores.keySet());
      if (getTransientCacheHandler() != null) {
        set.addAll(getTransientCacheHandler().getLoadedCoreNames());
      }
    }
    return set;
  }

  /** This method is currently experimental.
   *
   * @return a Collection of the names that a specific core object is mapped to, there are more than one.
   */
  @Experimental
  List<String> getNamesForCore(SolrCore core) {
    List<String> lst = new ArrayList<>();

    synchronized (modifyLock) {
      for (Map.Entry<String, SolrCore> entry : cores.entrySet()) {
        if (core == entry.getValue()) {
          lst.add(entry.getKey());
        }
      }
      if (getTransientCacheHandler() != null) {
        lst.addAll(getTransientCacheHandler().getNamesForCore(core));
      }
    }
    return lst;
  }

  /**
   * Gets a list of all cores, loaded and unloaded 
   *
   * @return all cores names, whether loaded or unloaded, transient or permanent.
   */
  public Collection<String> getAllCoreNames() {
    Set<String> set;
    synchronized (modifyLock) {
      set = new TreeSet<>(cores.keySet());
      if (getTransientCacheHandler() != null) {
        set.addAll(getTransientCacheHandler().getAllCoreNames());
      }
      set.addAll(residentDesciptors.keySet());
    }
    return set;
  }

  SolrCore getCore(String name) {

    synchronized (modifyLock) {
      return cores.get(name);
    }
  }

  protected void swap(String n0, String n1) {

    synchronized (modifyLock) {
      SolrCore c0 = cores.get(n0);
      SolrCore c1 = cores.get(n1);
      if (c0 == null) { // Might be an unloaded transient core
        c0 = container.getCore(n0);
        if (c0 == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No such core: " + n0);
        }
      }
      if (c1 == null) { // Might be an unloaded transient core
        c1 = container.getCore(n1);
        if (c1 == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No such core: " + n1);
        }
      }
      // When we swap the cores, we also need to swap the associated core descriptors. Note, this changes the 
      // name of the coreDescriptor by virtue of the c-tor
      CoreDescriptor cd1 = c1.getCoreDescriptor(); 
      addCoreDescriptor(new CoreDescriptor(n1, c0.getCoreDescriptor()));
      addCoreDescriptor(new CoreDescriptor(n0, cd1));
      cores.put(n0, c1);
      cores.put(n1, c0);
      c0.setName(n1);
      c1.setName(n0);
      
      container.getMetricManager().swapRegistries(
          c0.getCoreMetricManager().getRegistryName(),
          c1.getCoreMetricManager().getRegistryName());
    }

  }

  protected SolrCore remove(String name) {

    synchronized (modifyLock) {
      SolrCore ret = cores.remove(name);
      // It could have been a newly-created core. It could have been a transient core. The newly-created cores
      // in particular should be checked. It could have been a dynamic core.
      TransientSolrCoreCache transientHandler = getTransientCacheHandler();
      if (ret == null && transientHandler != null) {
        ret = transientHandler.removeCore(name);
      }
      return ret;
    }
  }

  /* If you don't increment the reference count, someone could close the core before you use it. */
  SolrCore  getCoreFromAnyList(String name, boolean incRefCount) {
    synchronized (modifyLock) {
      SolrCore core = cores.get(name);

      if (core == null && getTransientCacheHandler() != null) {
        core = getTransientCacheHandler().getCore(name);
      }

      if (core != null && incRefCount) {
        core.open();
      }

      return core;
    }
  }

  // See SOLR-5366 for why the UNLOAD command needs to know whether a core is actually loaded or not, it might have
  // to close the core. However, there's a race condition. If the core happens to be in the pending "to close" queue,
  // we should NOT close it in unload core.
  protected boolean isLoadedNotPendingClose(String name) {
    // Just all be synchronized
    synchronized (modifyLock) {
      if (cores.containsKey(name)) {
        return true;
      }
      if (getTransientCacheHandler() != null && getTransientCacheHandler().containsCore(name)) {
        // Check pending
        for (SolrCore core : pendingCloses) {
          if (core.getName().equals(name)) {
            return false;
          }
        }

        return true;
      }
    }
    return false;
  }

  protected boolean isLoaded(String name) {
    synchronized (modifyLock) {
      if (cores.containsKey(name)) {
        return true;
      }
      if (getTransientCacheHandler() != null && getTransientCacheHandler().containsCore(name)) {
        return true;
      }
    }
    return false;

  }

  protected CoreDescriptor getUnloadedCoreDescriptor(String cname) {
    synchronized (modifyLock) {
      CoreDescriptor desc = residentDesciptors.get(cname);
      if (desc == null) {
        if (getTransientCacheHandler() == null) return null;
        desc = getTransientCacheHandler().getTransientDescriptor(cname);
        if (desc == null) {
          return null;
        }
      }
      return new CoreDescriptor(cname, desc);
    }
  }

  // Wait here until any pending operations (load, unload or reload) are completed on this core.
  protected SolrCore waitAddPendingCoreOps(String name) {

    // Keep multiple threads from operating on a core at one time.
    synchronized (modifyLock) {
      boolean pending;
      do { // Are we currently doing anything to this core? Loading, unloading, reloading?
        pending = pendingCoreOps.contains(name); // wait for the core to be done being operated upon
        if (! pending) { // Linear list, but shouldn't be too long
          for (SolrCore core : pendingCloses) {
            if (core.getName().equals(name)) {
              pending = true;
              break;
            }
          }
        }
        if (container.isShutDown()) return null; // Just stop already.

        if (pending) {
          try {
            modifyLock.wait();
          } catch (InterruptedException e) {
            return null; // Seems best not to do anything at all if the thread is interrupted
          }
        }
      } while (pending);
      // We _really_ need to do this within the synchronized block!
      if (! container.isShutDown()) {
        if (! pendingCoreOps.add(name)) {
          log.warn("Replaced an entry in pendingCoreOps {}, we should not be doing this", name);
        }
        return getCoreFromAnyList(name, false); // we might have been _unloading_ the core, so return the core if it was loaded.
      }
    }
    return null;
  }

  // We should always be removing the first thing in the list with our name! The idea here is to NOT do anything n
  // any core while some other operation is working on that core.
  protected void removeFromPendingOps(String name) {
    synchronized (modifyLock) {
      if (! pendingCoreOps.remove(name)) {
        log.warn("Tried to remove core {} from pendingCoreOps and it wasn't there. ", name);
      }
      modifyLock.notifyAll();
    }
  }

  protected Object getModifyLock() {
    return modifyLock;
  }

  // Be a little careful. We don't want to either open or close a core unless it's _not_ being opened or closed by
  // another thread. So within this lock we'll walk along the list of pending closes until we find something NOT in
  // the list of threads currently being loaded or reloaded. The "usual" case will probably return the very first
  // one anyway..
  protected SolrCore getCoreToClose() {
    synchronized (modifyLock) {
      for (SolrCore core : pendingCloses) {
        if (! pendingCoreOps.contains(core.getName())) {
          pendingCoreOps.add(core.getName());
          pendingCloses.remove(core);
          return core;
        }
      }
    }
    return null;
  }

  /**
   * Return the CoreDescriptor corresponding to a given core name.
   * Blocks if the SolrCore is still loading until it is ready.
   * @param coreName the name of the core
   * @return the CoreDescriptor
   */
  public CoreDescriptor getCoreDescriptor(String coreName) {
    synchronized (modifyLock) {
      if (residentDesciptors.containsKey(coreName))
        return residentDesciptors.get(coreName);
      return getTransientCacheHandler().getTransientDescriptor(coreName);
    }
  }

  /**
   * Get the CoreDescriptors for every SolrCore managed here
   * @return a List of CoreDescriptors
   */
  public List<CoreDescriptor> getCoreDescriptors() {
    List<CoreDescriptor> cds = Lists.newArrayList();
    synchronized (modifyLock) {
      for (String coreName : getAllCoreNames()) {
        // TODO: This null check is a bit suspicious - it seems that
        // getAllCoreNames might return deleted cores as well?
        CoreDescriptor cd = getCoreDescriptor(coreName);
        if (cd != null)
          cds.add(cd);
      }
    }
    return cds;
  }

  // cores marked as loading will block on getCore
  public void markCoreAsLoading(CoreDescriptor cd) {
    synchronized (modifyLock) {
      currentlyLoadingCores.add(cd.getName());
    }
  }

  //cores marked as loading will block on getCore
  public void markCoreAsNotLoading(CoreDescriptor cd) {
    synchronized (modifyLock) {
      currentlyLoadingCores.remove(cd.getName());
    }
  }

  // returns when no cores are marked as loading
  public void waitForLoadingCoresToFinish(long timeoutMs) {
    long time = System.nanoTime();
    long timeout = time + TimeUnit.NANOSECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS);
    synchronized (modifyLock) {
      while (!currentlyLoadingCores.isEmpty()) {
        try {
          modifyLock.wait(500);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        if (System.nanoTime() >= timeout) {
          log.warn("Timed out waiting for SolrCores to finish loading.");
          break;
        }
      }
    }
  }
  
  // returns when core is finished loading, throws exception if no such core loading or loaded
  public void waitForLoadingCoreToFinish(String core, long timeoutMs) {
    long time = System.nanoTime();
    long timeout = time + TimeUnit.NANOSECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS);
    synchronized (modifyLock) {
      while (isCoreLoading(core)) {
        try {
          modifyLock.wait(500);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        if (System.nanoTime() >= timeout) {
          log.warn("Timed out waiting for SolrCore, {},  to finish loading.", core);
          break;
        }
      }
    }
  }

  public boolean isCoreLoading(String name) {
    if (currentlyLoadingCores.contains(name)) {
      return true;
    }
    return false;
  }

  public void queueCoreToClose(SolrCore coreToClose) {
    synchronized (modifyLock) {
      pendingCloses.add(coreToClose); // Essentially just queue this core up for closing.
      modifyLock.notifyAll(); // Wakes up closer thread too
    }
  }

  public TransientSolrCoreCache getTransientCacheHandler() {

    if (transientCoreCache == null) {
      log.error("No transient handler has been defined. Check solr.xml to see if an attempt to provide a custom " +
          "TransientSolrCoreCacheFactory was done incorrectly since the default should have been used otherwise.");
      return null;
    }
    return transientCoreCache.getTransientSolrCoreCache();
  }

}
