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

  //WARNING! The _only_ place you put anything into the list of transient cores is with the putTransientCore method!
  private Map<String, SolrCore> transientCores = new LinkedHashMap<>(); // For "lazily loaded" cores

  private final Map<String, CoreDescriptor> dynamicDescriptors = new LinkedHashMap<>();

  private final CoreContainer container;
  
  private Set<String> currentlyLoadingCores = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // This map will hold objects that are being currently operated on. The core (value) may be null in the case of
  // initial load. The rule is, never to any operation on a core that is currently being operated upon.
  private static final Set<String> pendingCoreOps = new HashSet<>();

  // Due to the fact that closes happen potentially whenever anything is _added_ to the transient core list, we need
  // to essentially queue them up to be handled via pendingCoreOps.
  private static final List<SolrCore> pendingCloses = new ArrayList<>();

  SolrCores(CoreContainer container) {
    this.container = container;
  }

  // Trivial helper method for load, note it implements LRU on transient cores. Also note, if
  // there is no setting for max size, nothing is done and all cores go in the regular "cores" list
  protected void allocateLazyCores(final int cacheSize, final SolrResourceLoader loader) {
    if (cacheSize != Integer.MAX_VALUE) {
      log.info("Allocating transient cache for {} transient cores", cacheSize);
      transientCores = new LinkedHashMap<String, SolrCore>(cacheSize, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, SolrCore> eldest) {
          if (size() > cacheSize) {
            synchronized (modifyLock) {
              SolrCore coreToClose = eldest.getValue();
              log.info("Closing transient core [{}]", coreToClose.getName());
              pendingCloses.add(coreToClose); // Essentially just queue this core up for closing.
              modifyLock.notifyAll(); // Wakes up closer thread too
            }
            return true;
          }
          return false;
        }
      };
    }
  }

  protected void putDynamicDescriptor(String rawName, CoreDescriptor p) {
    synchronized (modifyLock) {
      dynamicDescriptors.put(rawName, p);
    }
  }

  // We are shutting down. You can't hold the lock on the various lists of cores while they shut down, so we need to
  // make a temporary copy of the names and shut them down outside the lock.
  protected void close() {
    Collection<SolrCore> coreList = new ArrayList<>();

    // It might be possible for one of the cores to move from one list to another while we're closing them. So
    // loop through the lists until they're all empty. In particular, the core could have moved from the transient
    // list to the pendingCloses list.

    do {
      coreList.clear();
      synchronized (modifyLock) {
        // make a copy of the cores then clear the map so the core isn't handed out to a request again
        coreList.addAll(cores.values());
        cores.clear();

        coreList.addAll(transientCores.values());
        transientCores.clear();

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

  //WARNING! This should be the _only_ place you put anything into the list of transient cores!
  protected SolrCore putTransientCore(NodeConfig cfg, String name, SolrCore core, SolrResourceLoader loader) {
    SolrCore retCore;
    log.info("Opening transient core {}", name);
    synchronized (modifyLock) {
      retCore = transientCores.put(name, core);
    }
    return retCore;
  }

  protected SolrCore putCore(String name, SolrCore core) {
    synchronized (modifyLock) {
      return cores.put(name, core);
    }
  }

  List<SolrCore> getCores() {
    List<SolrCore> lst = new ArrayList<>();

    synchronized (modifyLock) {
      lst.addAll(cores.values());
      return lst;
    }
  }

  Set<String> getCoreNames() {
    Set<String> set = new TreeSet<>();

    synchronized (modifyLock) {
      set.addAll(cores.keySet());
      set.addAll(transientCores.keySet());
    }
    return set;
  }

  List<String> getCoreNames(SolrCore core) {
    List<String> lst = new ArrayList<>();

    synchronized (modifyLock) {
      for (Map.Entry<String, SolrCore> entry : cores.entrySet()) {
        if (core == entry.getValue()) {
          lst.add(entry.getKey());
        }
      }
      for (Map.Entry<String, SolrCore> entry : transientCores.entrySet()) {
        if (core == entry.getValue()) {
          lst.add(entry.getKey());
        }
      }
    }
    return lst;
  }

  /**
   * Gets a list of all cores, loaded and unloaded (dynamic)
   *
   * @return all cores names, whether loaded or unloaded.
   */
  public Collection<String> getAllCoreNames() {
    Set<String> set = new TreeSet<>();
    synchronized (modifyLock) {
      set.addAll(cores.keySet());
      set.addAll(transientCores.keySet());
      set.addAll(dynamicDescriptors.keySet());
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
      cores.put(n0, c1);
      cores.put(n1, c0);
      container.getMetricManager().swapRegistries(
          c0.getCoreMetricManager().getRegistryName(),
          c1.getCoreMetricManager().getRegistryName());
      c0.setName(n1);
      c1.setName(n0);
    }

  }

  protected SolrCore remove(String name) {

    synchronized (modifyLock) {
      SolrCore tmp = cores.remove(name);
      SolrCore ret = null;
      ret = (ret == null) ? tmp : ret;
      // It could have been a newly-created core. It could have been a transient core. The newly-created cores
      // in particular should be checked. It could have been a dynamic core.
      tmp = transientCores.remove(name);
      ret = (ret == null) ? tmp : ret;
      dynamicDescriptors.remove(name);
      return ret;
    }
  }

  /* If you don't increment the reference count, someone could close the core before you use it. */
  SolrCore  getCoreFromAnyList(String name, boolean incRefCount) {
    synchronized (modifyLock) {
      SolrCore core = cores.get(name);

      if (core == null) {
        core = transientCores.get(name);
      }

      if (core != null && incRefCount) {
        core.open();
      }

      return core;
    }
  }

  protected CoreDescriptor getDynamicDescriptor(String name) {
    synchronized (modifyLock) {
      return dynamicDescriptors.get(name);
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
      if (transientCores.containsKey(name)) {
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
      if (transientCores.containsKey(name)) {
        return true;
      }
    }
    return false;

  }

  protected CoreDescriptor getUnloadedCoreDescriptor(String cname) {
    synchronized (modifyLock) {
      CoreDescriptor desc = dynamicDescriptors.get(cname);
      if (desc == null) {
        return null;
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
      if (cores.containsKey(coreName))
        return cores.get(coreName).getCoreDescriptor();
      if (dynamicDescriptors.containsKey(coreName))
        return dynamicDescriptors.get(coreName);
      return null;
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
}
