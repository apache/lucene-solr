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
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
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


class SolrCores implements Closeable {
  private final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private volatile boolean closed;

  private final Map<String, SolrCore> cores = new ConcurrentHashMap<>(64, 0.75f, 200);

  // These descriptors, once loaded, will _not_ be unloaded, i.e. they are not "transient".
  private final Map<String, CoreDescriptor> residentDesciptors = new ConcurrentHashMap<>(64, 0.75f, 200);

  private final CoreContainer container;

  private final Object loadingSignal = new Object();
  
  private final Set<String> currentlyLoadingCores = ConcurrentHashMap.newKeySet(64);

  // This map will hold objects that are being currently operated on. The core (value) may be null in the case of
  // initial load. The rule is, never to any operation on a core that is currently being operated upon.
  private final Set<String> pendingCoreOps = ConcurrentHashMap.newKeySet(64);

  // Due to the fact that closes happen potentially whenever anything is _added_ to the transient core list, we need
  // to essentially queue them up to be handled via pendingCoreOps.
  private final Set<SolrCore> pendingCloses = ConcurrentHashMap.newKeySet(64);;

  private volatile TransientSolrCoreCacheFactory transientCoreCache;

  private volatile TransientSolrCoreCache transientSolrCoreCache = null;
  
  SolrCores(CoreContainer container) {
    this.container = container;
  }
  
  protected void addCoreDescriptor(CoreDescriptor p) {
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

  protected void removeCoreDescriptor(CoreDescriptor p) {
    if (p.isTransient()) {
      if (getTransientCacheHandler() != null) {
        getTransientCacheHandler().removeTransientDescriptor(p.getName());
      }
    } else {
      residentDesciptors.remove(p.getName());
    }
  }

  public void load(SolrResourceLoader loader) {
    transientCoreCache = TransientSolrCoreCacheFactory.newInstance(loader, container);
  }

  // We are shutting down. You can't hold the lock on the various lists of cores while they shut down, so we need to
  // make a temporary copy of the names and shut them down outside the lock.
  public void close() {
    log.info("Closing SolrCores");
    this.closed = true;

    waitForLoadingAndOps();

    Collection<SolrCore> coreList = new ArrayList<>();

    TransientSolrCoreCache transientSolrCoreCache = getTransientCacheHandler();
    // Release observer
    if (transientSolrCoreCache != null) {
      transientSolrCoreCache.close();
    }

    // It might be possible for one of the cores to move from one list to another while we're closing them. So
    // loop through the lists until they're all empty. In particular, the core could have moved from the transient
    // list to the pendingCloses list.

    // make a copy of the cores then clear the map so the core isn't handed out to a request again
    coreList.addAll(cores.values());
    if (transientSolrCoreCache != null) {
      coreList.addAll(transientSolrCoreCache.prepareForShutdown());
    }
    cores.clear();
    coreList.addAll(pendingCloses);
    pendingCloses.forEach((c) -> coreList.add(c));

    try (ParWork closer = new ParWork(this, true)) {
      for (SolrCore core : coreList) {
        closer.collect(() -> {
          MDCLoggingContext.setCore(core);
          try {
            core.close();
          } catch (Throwable e) {
            log.error("Error closing SolrCore", e);
            ParWork.propegateInterrupt("Error shutting down core", e);
          } finally {
            MDCLoggingContext.clear();
          }
          return core;
        });
      }
      closer.addCollect("CloseSolrCores");
    }

  }

  public void waitForLoadingAndOps() {
    waitForLoadingCoresToFinish(30 * 1000); // nocommit timeout config
    waitAddPendingCoreOps();
  }
  
  // Returns the old core if there was a core of the same name.
  //WARNING! This should be the _only_ place you put anything into the list of transient cores!
  protected SolrCore putCore(CoreDescriptor cd, SolrCore core) {
    if (isClosed()) {
      throw new AlreadyClosedException();
    }
    if (cd.isTransient()) {
      if (getTransientCacheHandler() != null) {
        return getTransientCacheHandler().addCore(cd.getName(), core);
      }
    } else {
      return cores.put(cd.getName(), core);
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
    List<SolrCore> lst = new ArrayList<>(cores.values());
    return lst;
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
    set = new TreeSet<>(cores.keySet());
    if (getTransientCacheHandler() != null) {
      set.addAll(getTransientCacheHandler().getLoadedCoreNames());
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

    for (Map.Entry<String, SolrCore> entry : cores.entrySet()) {
      if (core == entry.getValue()) {
        lst.add(entry.getKey());
      }
    }
    if (getTransientCacheHandler() != null) {
      lst.addAll(getTransientCacheHandler().getNamesForCore(core));
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
    set = new TreeSet<>(cores.keySet());
    if (getTransientCacheHandler() != null) {
      set.addAll(getTransientCacheHandler().getAllCoreNames());
    }
    set.addAll(residentDesciptors.keySet());

    return set;
  }

  SolrCore getCore(String name) {
      return cores.get(name);
  }

  protected void swap(String n0, String n1) {
    if (isClosed()) {
      throw new AlreadyClosedException();
    }
    synchronized (cores) {
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

  private boolean isClosed() {
    return closed || container.isShutDown();
  }

  protected SolrCore remove(String name) {
    SolrCore ret = cores.remove(name);
    // It could have been a newly-created core. It could have been a transient core. The newly-created cores
    // in particular should be checked. It could have been a dynamic core.
    TransientSolrCoreCache transientHandler = getTransientCacheHandler();
    if (ret == null && transientHandler != null) {
      ret = transientHandler.removeCore(name);
    }
    return ret;
  }

  SolrCore  getCoreFromAnyList(String name, boolean incRefCount) {
    return getCoreFromAnyList(name, incRefCount, false);
  }

  /* If you don't increment the reference count, someone could close the core before you use it. */
  SolrCore  getCoreFromAnyList(String name, boolean incRefCount, boolean onClose) {
    if (!onClose && closed) {
      throw new AlreadyClosedException("SolrCores has been closed");
    }
    SolrCore core = cores.get(name);
    if (core == null && getTransientCacheHandler() != null) {
      core = getTransientCacheHandler().getCore(name);
    }

    if (core != null && incRefCount) {
      core.open();
    }

    return core;
  }

  // See SOLR-5366 for why the UNLOAD command needs to know whether a core is actually loaded or not, it might have
  // to close the core. However, there's a race condition. If the core happens to be in the pending "to close" queue,
  // we should NOT close it in unload core.
  protected boolean isLoadedNotPendingClose(String name) {
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
    return false;
  }

  protected boolean isLoaded(String name) {
    if (cores.containsKey(name)) {
      return true;
    }
    if (getTransientCacheHandler() != null && getTransientCacheHandler().containsCore(name)) {
      return true;
    }

    return false;

  }

  protected CoreDescriptor getUnloadedCoreDescriptor(String cname) {
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

  // Wait here until any pending operations (load, unload or reload) are completed on this core.
  protected SolrCore waitAddPendingCoreOps(String name) {

    // Keep multiple threads from operating on a core at one time.
      boolean pending;
      do { // Are we currently doing anything to this core? Loading, unloading, reloading?
        pending = pendingCoreOps.contains(name); // wait for the core to be done being operated upon
//        if (!pending) { // Linear list, but shouldn't be too long
//          for (SolrCore core : pendingCloses) {
//            if (core.getName().equals(name)) {
//              pending = true;
//              break;
//            }
//          }
//        }

        if (pending) {
          try {
            Thread.sleep(250);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
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

    return null;
  }

  protected SolrCore waitAddPendingCoreOps() {
      boolean pending;
      do {
        pending = pendingCoreOps.size() > 0;

        if (pending) {
          synchronized (pendingCoreOps) {
            try {
              pendingCoreOps.wait(500);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
          }

        }
      } while (pending);
    return null;
  }

  // We should always be removing the first thing in the list with our name! The idea here is to NOT do anything n
  // any core while some other operation is working on that core.
  protected void removeFromPendingOps(String name) {
    synchronized (pendingCoreOps) {
      if (!pendingCoreOps.remove(name)) {
        log.warn("Tried to remove core {} from pendingCoreOps and it wasn't there. ", name);
      }
      pendingCoreOps.notifyAll();
    }
  }

  /**
   * Return the CoreDescriptor corresponding to a given core name.
   * Blocks if the SolrCore is still loading until it is ready.
   * @param coreName the name of the core
   * @return the CoreDescriptor
   */
  public CoreDescriptor getCoreDescriptor(String coreName) {
    if (coreName == null) return null;

    if (residentDesciptors.containsKey(coreName))
      return residentDesciptors.get(coreName);
    return getTransientCacheHandler().getTransientDescriptor(coreName);
  }

  /**
   * Get the CoreDescriptors for every SolrCore managed here
   * @return a List of CoreDescriptors
   */
  public List<CoreDescriptor> getCoreDescriptors() {
    List<CoreDescriptor> cds = Lists.newArrayList();
    for (String coreName : getAllCoreNames()) {
      // TODO: This null check is a bit suspicious - it seems that
      // getAllCoreNames might return deleted cores as well?
      CoreDescriptor cd = getCoreDescriptor(coreName);
      if (cd != null)
        cds.add(cd);
    }

    return cds;
  }

  // cores marked as loading will block on getCore
  public void markCoreAsLoading(CoreDescriptor cd) {
    currentlyLoadingCores.add(cd.getName());
  }

  //cores marked as loading will block on getCore
  public void markCoreAsNotLoading(CoreDescriptor cd) {
    currentlyLoadingCores.remove(cd.getName());
    synchronized (loadingSignal) {
      loadingSignal.notifyAll();
    }

  }

  // returns when no cores are marked as loading
  public void waitForLoadingCoresToFinish(long timeoutMs) {
    long time = System.nanoTime();
    long timeout = time + TimeUnit.NANOSECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS);
      while (!currentlyLoadingCores.isEmpty()) {
        synchronized (loadingSignal) {
          try {
            loadingSignal.wait(1000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
        if (System.nanoTime() >= timeout) {
          log.warn("Timed out waiting for SolrCores to finish loading.");
          throw new RuntimeException("Timed out waiting for SolrCores to finish loading.");
        }
      }
  }
  
  // returns when core is finished loading, throws exception if no such core loading or loaded
  public void waitForLoadingCoreToFinish(String core, long timeoutMs) {
    long time = System.nanoTime();
    long timeout = time + TimeUnit.NANOSECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS);

      while (isCoreLoading(core)) {
        synchronized (loadingSignal) {
          try {
            loadingSignal.wait(1000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
        if (System.nanoTime() >= timeout) {
          log.warn("Timed out waiting for SolrCore, {},  to finish loading.", core);
          throw new RuntimeException("Timed out waiting for SolrCore, "+ core + ",  to finish loading.");
        }
      }
  }

  public boolean isCoreLoading(String name) {
    if (currentlyLoadingCores.contains(name)) {
      return true;
    }
    return false;
  }

//  public void queueCoreToClose(SolrCore coreToClose) {
//    synchronized (pendingCloses) {
//      pendingCloses.add(coreToClose); // Essentially just queue this core up for closing.
//      pendingCloses.notifyAll(); // Wakes up closer thread too
//    }
//  }

  public TransientSolrCoreCache getTransientCacheHandler() {

    if (transientCoreCache == null) {
      log.error("No transient handler has been defined. Check solr.xml to see if an attempt to provide a custom {}"
          , "TransientSolrCoreCacheFactory was done incorrectly since the default should have been used otherwise.");
      return null;
    }
    return transientCoreCache.getTransientSolrCoreCache();
  }

  public void closing() {
    this.closed = true;
  }
}
