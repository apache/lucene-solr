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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.logging.MDCLoggingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

class SolrCores {

  private static Object modifyLock = new Object(); // for locking around manipulating any of the core maps.
  private final Map<String, SolrCore> cores = new LinkedHashMap<>(); // For "permanent" cores

  // These descriptors, once loaded, will _not_ be unloaded, i.e. they are not "transient".
  private final Map<String, CoreDescriptor> residentDescriptors = new LinkedHashMap<>();

  private final CoreContainer container;
  
  private Set<String> currentlyLoadingCores = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // This map will hold objects that are being currently operated on. The core (value) may be null in the case of
  // initial load. The rule is, never to any operation on a core that is currently being operated upon.
  private static final Set<String> pendingCoreOps = new HashSet<>();

  // Due to the fact that closes happen potentially whenever anything is _added_ to the transient core list, we need
  // to essentially queue them up to be handled via pendingCoreOps.
  private static final List<SolrCore> pendingCloses = new ArrayList<>();

  private TransientSolrCoreCacheFactory transientSolrCoreCacheFactory;

  SolrCores(CoreContainer container) {
    this.container = container;
  }
  
  protected void addCoreDescriptor(CoreDescriptor p) {
    synchronized (modifyLock) {
      if (p.isTransient()) {
        getTransientCacheHandler().addTransientDescriptor(p.getName(), p);
      } else {
        residentDescriptors.put(p.getName(), p);
      }
    }
  }

  protected void removeCoreDescriptor(CoreDescriptor p) {
    synchronized (modifyLock) {
      if (p.isTransient()) {
        getTransientCacheHandler().removeTransientDescriptor(p.getName());
      } else {
        residentDescriptors.remove(p.getName());
      }
    }
  }

  public void load(SolrResourceLoader loader) {
    synchronized (modifyLock) {
      transientSolrCoreCacheFactory = TransientSolrCoreCacheFactory.newInstance(loader, container);
    }
  }

  // We are shutting down. You can't hold the lock on the various lists of cores while they shut down, so we need to
  // make a temporary copy of the names and shut them down outside the lock.
  protected void close() {
    waitForLoadingCoresToFinish(30*1000);
    Collection<SolrCore> coreList = new ArrayList<>();

    // Release transient core cache.
    synchronized (modifyLock) {
      if (transientSolrCoreCacheFactory != null) {
        getTransientCacheHandler().close();
      }
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
        if (transientSolrCoreCacheFactory != null) {
          coreList.addAll(getTransientCacheHandler().prepareForShutdown());
        }

        coreList.addAll(pendingCloses);
        pendingCloses.clear();
      }
      
      ExecutorService coreCloseExecutor = ExecutorUtil.newMDCAwareFixedThreadPool(Integer.MAX_VALUE,
          new SolrNamedThreadFactory("coreCloseExecutor"));
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
      addCoreDescriptor(cd); // cd must always be registered if we register a core

      if (cd.isTransient()) {
        return getTransientCacheHandler().addCore(cd.getName(), core);
      } else {
        return cores.put(cd.getName(), core);
      }
    }
  }

  /**
   * @return A list of "permanent" cores, i.e. cores that may not be swapped out and are currently loaded.
   *
   * A core may be non-transient but still lazily loaded. If it is "permanent" and lazy-load _and_
   * not yet loaded it will _not_ be returned by this call.
   *
   * This list is a new copy, it can be modified by the caller (e.g. it can be sorted).
   *
   * Note: This is one of the places where SolrCloud is incompatible with Transient Cores. This call is used in
   * cancelRecoveries, transient cores don't participate.
   */
  List<SolrCore> getCores() {

    synchronized (modifyLock) {
      return new ArrayList<>(cores.values());
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
   * @return An unsorted list. This list is a new copy, it can be modified by the caller (e.g. it can be sorted).
   */
  List<String> getLoadedCoreNames() {
    synchronized (modifyLock) {
      return distinctSetsUnion(cores.keySet(), getTransientCacheHandler().getLoadedCoreNames());
    }
  }

  /**
   * Gets a collection of all cores names, loaded and unloaded.
   * For efficiency, prefer to check {@link #getCoreDescriptor(String)} != null instead of {@link #getAllCoreNames()}.contains(String)
   *
   * @return An unsorted list. This list is a new copy, it can be modified by the caller (e.g. it can be sorted).
   */
  public List<String> getAllCoreNames() {
    synchronized (modifyLock) {
      return distinctSetsUnion(residentDescriptors.keySet(), getTransientCacheHandler().getAllCoreNames());
    }
  }

  /**
   * Makes the union of two distinct sets.
   *
   * @return An unsorted list. This list is a new copy, it can be modified by the caller (e.g. it can be sorted).
   */
  private static <T> List<T> distinctSetsUnion(Set<T> set1, Set<T> set2) {
    assert areSetsDistinct(set1, set2);
    List<T> union = new ArrayList<>(set1.size() + set2.size());
    union.addAll(set1);
    union.addAll(set2);
    return union;
  }

  /**
   * Indicates whether two sets are distinct (intersection is empty).
   */
  private static <T> boolean areSetsDistinct(Set<T> set1, Set<T> set2) {
    return set1.stream().noneMatch(set2::contains);
  }

  /**
   * Gets the number of currently loaded permanent (non transient) cores.
   * Faster equivalent for {@link #getCores()}.size().
   */
  int getNumLoadedPermanentCores() {
    synchronized (modifyLock) {
      return cores.size();
    }
  }

  /**
   * Gets the number of currently loaded transient cores.
   */
  int getNumLoadedTransientCores() {
    synchronized (modifyLock) {
      return getTransientCacheHandler().getLoadedCoreNames().size();
    }
  }

  /**
   * Gets the number of unloaded cores, including permanent and transient cores.
   */
  int getNumUnloadedCores() {
    synchronized (modifyLock) {
      assert areSetsDistinct(residentDescriptors.keySet(), getTransientCacheHandler().getAllCoreNames());
      return getTransientCacheHandler().getAllCoreNames().size() - getTransientCacheHandler().getLoadedCoreNames().size()
              + residentDescriptors.size() - cores.size();
    }
  }

  /**
   * Gets the total number of cores, including permanent and transient cores, loaded and unloaded cores.
   * Faster equivalent for {@link #getAllCoreNames()}.size().
   */
  public int getNumAllCores() {
    synchronized (modifyLock) {
      assert areSetsDistinct(residentDescriptors.keySet(), getTransientCacheHandler().getAllCoreNames());
      return residentDescriptors.size() + getTransientCacheHandler().getAllCoreNames().size();
    }
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
      if (ret == null) {
        ret = getTransientCacheHandler().removeCore(name);
      }
      return ret;
    }
  }
  SolrCore  getCoreFromAnyList(String name, boolean incRefCount) {
    return getCoreFromAnyList(name, incRefCount, null);
  }

  /* If you don't increment the reference count, someone could close the core before you use it. */
  SolrCore  getCoreFromAnyList(String name, boolean incRefCount, UUID coreId) {
    synchronized (modifyLock) {
      SolrCore core = cores.get(name);

      if (core == null) {
        core = getTransientCacheHandler().getCore(name);
      }
      if(core != null && coreId != null && coreId != core.uniqueId) return null;

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
      if (getTransientCacheHandler().containsCore(name)) {
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
      return cores.containsKey(name) || getTransientCacheHandler().containsCore(name);
    }
  }

  protected CoreDescriptor getUnloadedCoreDescriptor(String cname) {
    synchronized (modifyLock) {
      CoreDescriptor desc = residentDescriptors.get(cname);
      if (desc == null) {
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
      CoreDescriptor coreDescriptor = residentDescriptors.get(coreName);
      if (coreDescriptor != null) {
        return coreDescriptor;
      }
      return getTransientCacheHandler().getTransientDescriptor(coreName);
    }
  }

  /**
   * Get the CoreDescriptors for every {@link SolrCore} managed here (permanent and transient, loaded and unloaded).
   *
   * @return An unordered list copy. This list can be modified by the caller (e.g. sorted).
   */
  public List<CoreDescriptor> getCoreDescriptors() {
    synchronized (modifyLock) {
      Collection<CoreDescriptor> transientCoreDescriptors = getTransientCacheHandler().getTransientDescriptors();
      List<CoreDescriptor> coreDescriptors = new ArrayList<>(residentDescriptors.size() + transientCoreDescriptors.size());
      coreDescriptors.addAll(residentDescriptors.values());
      coreDescriptors.addAll(transientCoreDescriptors);
      return coreDescriptors;
    }
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
    return currentlyLoadingCores.contains(name);
  }

  // Let transient cache implementation tell us when it ages out a core
  public void queueCoreToClose(SolrCore coreToClose) {
    synchronized (modifyLock) {
      pendingCloses.add(coreToClose); // Essentially just queue this core up for closing.
      modifyLock.notifyAll(); // Wakes up closer thread too
    }
  }

  /**
   * @return the cache holding the transient cores; never null.
   */
  public TransientSolrCoreCache getTransientCacheHandler() {
    synchronized (modifyLock) {
      if (transientSolrCoreCacheFactory == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, getClass().getName() + " not loaded; call load() before using it");
      }
      return transientSolrCoreCacheFactory.getTransientSolrCoreCache();
    }
  }


}
