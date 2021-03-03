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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link DirectoryFactory} impl base class for caching Directory instances
 * per path. Most DirectoryFactory implementations will want to extend this
 * class and simply implement {@link DirectoryFactory#create(String, LockFactory, DirContext)}.
 * <p>
 * This is an expert class and these API's are subject to change.
 */
public abstract class CachingDirectoryFactory extends DirectoryFactory {
  protected static class CacheValue {
    final public String path;
    final public Directory directory;
    // for debug
    //final Exception originTrace;
    // use the setter!
    private boolean deleteOnClose = false;

    public int refCnt = 1;
    // has doneWithDirectory(Directory) been called on this?
    public boolean closeCacheValueCalled = false;
    public boolean doneWithDir = false;
    private boolean deleteAfterCoreClose = false;
    public final Set<CacheValue> removeEntries = new HashSet<>();
    public final Set<CacheValue> closeEntries = new HashSet<>();

    public CacheValue(String path, Directory directory) {
      this.path = path;
      this.directory = directory;
      this.closeEntries.add(this);
      // for debug
      // this.originTrace = new RuntimeException("Originated from:");
    }



    public void setDeleteOnClose(boolean deleteOnClose, boolean deleteAfterCoreClose) {
      if (log.isDebugEnabled()) {
        log.debug("setDeleteOnClose(boolean deleteOnClose={}, boolean deleteAfterCoreClose={}) - start", deleteOnClose, deleteAfterCoreClose);
      }

      if (deleteOnClose) {
        removeEntries.add(this);
      }
      this.deleteOnClose = deleteOnClose;
      this.deleteAfterCoreClose = deleteAfterCoreClose;

      if (log.isDebugEnabled()) {
        log.debug("setDeleteOnClose(boolean, boolean) - end");
      }
    }

    @Override
    public String toString() {
      return "CachedDir<<" + "refCount=" + refCnt + ";path=" + path + ";done=" + doneWithDir + ">>";
    }
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final boolean DEBUG_GET_RELEASE = false;

  protected final Map<String, CacheValue> byPathCache = new HashMap<>();

  protected final Map<Directory, CacheValue> byDirectoryCache = new IdentityHashMap<>();

  protected final Map<Directory, List<CloseListener>> closeListeners = new HashMap<>();

  protected final Set<CacheValue> removeEntries = new HashSet<>();

  private volatile Double maxWriteMBPerSecFlush;

  private volatile Double maxWriteMBPerSecMerge;

  private volatile Double maxWriteMBPerSecRead;

  private volatile Double maxWriteMBPerSecDefault;

  private volatile boolean closed;

  public interface CloseListener {
    public void postClose();

    public void preClose();
  }

  @Override
  public void addCloseListener(Directory dir, CloseListener closeListener) {
    if (log.isTraceEnabled()) log.trace("addCloseListener(Directory dir={}, CloseListener closeListener={}) - start", dir, closeListener);


    synchronized (this) {
      if (!byDirectoryCache.containsKey(dir)) {
        throw new IllegalArgumentException("Unknown directory: " + dir
                + " " + byDirectoryCache);
      }
      List<CloseListener> listeners = closeListeners.get(dir);
      if (listeners == null) {
        listeners = new ArrayList<>();
        closeListeners.put(dir, listeners);
      }
      listeners.add(closeListener);

      closeListeners.put(dir, listeners);
    }

    if (log.isTraceEnabled()) log.trace("addCloseListener(Directory, CloseListener) - end");
  }

  @Override
  public void doneWithDirectory(Directory directory) throws IOException {
    if (log.isTraceEnabled()) log.trace("doneWithDirectory(Directory directory={}) - start", directory);

    synchronized (this) {
      CacheValue cacheValue = byDirectoryCache.get(directory);
      if (cacheValue == null) {
        log.warn("done with an unknown directory, {}", directory);
        org.apache.solr.common.util.IOUtils.closeQuietly(directory);
        return;
      }
      cacheValue.doneWithDir = true;
      if (log.isDebugEnabled()) log.debug("Done with dir: {}", cacheValue);
      if (cacheValue.refCnt == 0) {
        boolean cl = closeCacheValue(cacheValue);
        if (cl) {
          removeFromCache(cacheValue);
        }
      }
    }


    if (log.isTraceEnabled()) log.trace("doneWithDirectory(Directory) - end");
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.solr.core.DirectoryFactory#close()
   */
  @Override
  public void close() throws IOException {
    if (log.isTraceEnabled()) log.trace("close() - start");


    synchronized (this) {
      closed = true;
      if (log.isDebugEnabled()) log.debug("Closing {} - {} directories currently being tracked", this.getClass().getSimpleName(), byDirectoryCache.size());
      Collection<CacheValue> values = new HashSet<>(byDirectoryCache.values());
      for (CacheValue val : values) {
        if (log.isDebugEnabled()) log.debug("Closing {} - currently tracking: {}",
                this.getClass().getSimpleName(), val);
      }

      values = byDirectoryCache.values();
      Set<CacheValue> closedDirs = new HashSet<>();
      for (CacheValue val : values) {
        try {
          if (val.refCnt > 0) continue;
          for (CacheValue v : val.closeEntries) {
            if (log.isDebugEnabled()) log.debug("Closing directory when closing factory: " + v.path);
            boolean cl = closeCacheValue(v);
            if (cl) {
              closedDirs.add(v);
            }
          }
        } catch (Exception e) {
          ParWork.propagateInterrupt("Error closing directory", e);
        }
      }

      for (CacheValue val : removeEntries) {
        if (log.isDebugEnabled()) log.debug("Removing directory after core close: " + val.path);
        try {
          removeDirectory(val);
        } catch (Exception e) {
          ParWork.propagateInterrupt("Error removing directory", e);
        }
      }

      for (CacheValue v : closedDirs) {
        removeFromCache(v);
      }
    }

    if (log.isTraceEnabled()) log.trace("close() - end");
  }

  private synchronized void removeFromCache(CacheValue v) {
    if (log.isTraceEnabled()) log.trace("removeFromCache(CacheValue v={}) - start", v);

    if (log.isDebugEnabled()) log.debug("Removing from cache: {}", v);
    byDirectoryCache.remove(v.directory);
    byPathCache.remove(v.path);

    if (log.isTraceEnabled()) log.trace("removeFromCache(CacheValue) - end");
  }

  // be sure this is called with the this sync lock
  // returns true if we closed the cacheValue, false if it will be closed later
  private boolean closeCacheValue(CacheValue cacheValue) {
    if (log.isTraceEnabled()) log.trace("closeCacheValue(CacheValue cacheValue={}) - start", cacheValue);

    if (log.isDebugEnabled()) log.debug("looking to close {} {}", cacheValue.path, cacheValue.closeEntries.toString());
    List<CloseListener> listeners = closeListeners.remove(cacheValue.directory);
    if (listeners != null) {
      for (CloseListener listener : listeners) {
        try {
          listener.preClose();
        } catch (Exception e) {
          log.error("closeCacheValue(CacheValue=" + cacheValue + ")", e);

          ParWork.propagateInterrupt("Error executing preClose for directory", e);
        }
      }
    }
    cacheValue.closeCacheValueCalled = true;
    if (cacheValue.deleteOnClose) {
      // see if we are a subpath
      Collection<CacheValue> values = byPathCache.values();

      Collection<CacheValue> cacheValues = new ArrayList<>(values);
      cacheValues.remove(cacheValue);
      for (CacheValue otherCacheValue : cacheValues) {
        // if we are a parent path and a sub path is not already closed, get a sub path to close us later
        if (isSubPath(cacheValue, otherCacheValue) && !otherCacheValue.closeCacheValueCalled) {
          // we let the sub dir remove and close us
          if (!otherCacheValue.deleteAfterCoreClose && cacheValue.deleteAfterCoreClose) {
            otherCacheValue.deleteAfterCoreClose = true;
          }
          otherCacheValue.removeEntries.addAll(cacheValue.removeEntries);
          otherCacheValue.closeEntries.addAll(cacheValue.closeEntries);
          cacheValue.closeEntries.clear();
          cacheValue.removeEntries.clear();

          if (log.isDebugEnabled()) {
            log.debug("closeCacheValue(CacheValue) - end");
          }
          return false;
        }
      }
    }

    boolean cl = false;
    for (CacheValue val : cacheValue.closeEntries) {
      close(val);
      if (val == cacheValue) {
        cl = true;
      }
    }

    for (CacheValue val : cacheValue.removeEntries) {
      if (!val.deleteAfterCoreClose) {
        if (log.isDebugEnabled()) log.debug("Removing directory before core close: " + val.path);
        try {
          removeDirectory(val);
        } catch (Exception e) {
          ParWork.propagateInterrupt(e);
          log.error("closeCacheValue(CacheValue=" + cacheValue + ")", e);

          SolrException.log(log, "Error removing directory " + val.path + " before core close", e);
        }
      } else {
        removeEntries.add(val);
      }
    }

    if (listeners != null) {
      for (CloseListener listener : listeners) {
        try {
          listener.postClose();
        } catch (Exception e) {
          ParWork.propagateInterrupt(e);
          log.error("closeCacheValue(CacheValue=" + cacheValue + ")", e);

          ParWork.propagateInterrupt("Error executing postClose for directory", e);
        }
      }
    }

    if (log.isTraceEnabled()) log.trace("closeCacheValue(CacheValue) - end");
    return cl;
  }

  private void close(CacheValue val) {
    if (log.isTraceEnabled()) log.trace("close(CacheValue val={}) - start", val);

    if (log.isDebugEnabled()) log.debug("Closing directory, CoreContainer#isShutdown={}", coreContainer != null ? coreContainer.isShutDown() : "null");
    try {
      if (coreContainer != null && coreContainer.isShutDown() && val.directory instanceof ShutdownAwareDirectory) {
        if (log.isDebugEnabled()) log.debug("Closing directory on shutdown: " + val.path);
        ((ShutdownAwareDirectory) val.directory).closeOnShutdown();
      } else {
        if (log.isDebugEnabled()) log.debug("Closing directory: " + val.path);
        val.directory.close();
      }
      assert ObjectReleaseTracker.release(val.directory);
    } catch (Exception e) {
      log.error("close(CacheValue=" + val + ")", e);

      ParWork.propagateInterrupt("Error closing directory", e);
    }

    if (log.isTraceEnabled()) log.trace("close(CacheValue) - end");
  }

  private boolean isSubPath(CacheValue cacheValue, CacheValue otherCacheValue) {
    if (log.isTraceEnabled()) log.trace("isSubPath(CacheValue cacheValue={}, CacheValue otherCacheValue={}) - start", cacheValue, otherCacheValue);

    int one = cacheValue.path.lastIndexOf('/');
    int two = otherCacheValue.path.lastIndexOf('/');

    boolean returnboolean = otherCacheValue.path.startsWith(cacheValue.path + "/") && two > one;

    if (log.isTraceEnabled()) log.trace("isSubPath(CacheValue, CacheValue) - end");

    return returnboolean;
  }

  @Override
  public boolean exists(String path) throws IOException {
    if (log.isTraceEnabled()) log.trace("exists(String path={}) - start", path);

    // back compat behavior
    File dirFile = new File(path);
    boolean returnboolean = dirFile.canRead() && dirFile.list().length > 0;

    if (log.isTraceEnabled()) log.trace("exists(String) - end");

    return returnboolean;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.solr.core.DirectoryFactory#get(java.lang.String,
   * java.lang.String, boolean)
   */
  @Override
  public final Directory get(String path, DirContext dirContext, String rawLockType)
          throws IOException {
    if (log.isTraceEnabled()) log.trace("get(String path={}, DirContext dirContext={}, String rawLockType={}) - start", path, dirContext, rawLockType);

    String fullPath = normalize(path);
    synchronized (this) {

      final CacheValue cacheValue = byPathCache.get(fullPath);
      Directory directory = null;
      if (cacheValue != null) {
        directory = cacheValue.directory;
      }

      if (directory == null) {
        directory = create(fullPath, createLockFactory(rawLockType), dirContext);
        assert !directory.getClass().getSimpleName().equals("MockDirectoryWrapper") ? ObjectReleaseTracker.track(directory) : true;
        boolean success = false;
        try {
          CacheValue newCacheValue = new CacheValue(fullPath, directory);
          byDirectoryCache.put(directory, newCacheValue);
          byPathCache.put(fullPath, newCacheValue);
          if (log.isDebugEnabled()) log.debug("return new directory for {}", newCacheValue, DEBUG_GET_RELEASE && newCacheValue.path.equals("data/index") ? new RuntimeException() : null );
          success = true;
        } finally {
          if (!success) {
            IOUtils.closeWhileHandlingException(directory);
            remove(fullPath);
            remove(directory);
          }
        }
      } else {
        cacheValue.refCnt++;
        if (log.isDebugEnabled()) log.debug("Reusing cached directory: {}", cacheValue, DEBUG_GET_RELEASE && cacheValue.path.equals("data/index") ? new RuntimeException() : null );
      }
      //  if (cacheValue.path.equals("data/index")) {
      //    log.info("getDir " + path, new RuntimeException("track get " + fullPath)); // MRM TODO:
      // }

      if (log.isTraceEnabled()) log.trace("get(String, DirContext, String) - end");

      return directory;
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.solr.core.DirectoryFactory#incRef(org.apache.lucene.store.Directory
   * )
   */
  @Override
  public void incRef(Directory directory) {
    if (log.isTraceEnabled()) log.trace("incRef(Directory directory={}) - start", directory);

    synchronized (this) {
      CacheValue cacheValue = byDirectoryCache.get(directory);
      if (cacheValue == null) {
        org.apache.solr.common.util.IOUtils.closeQuietly(directory);
        log.warn("Unknown directory: " + directory
            + " " + byDirectoryCache);
        return;
      }

      cacheValue.refCnt++;
      log.debug("incRef'ed: {}", cacheValue,  DEBUG_GET_RELEASE && cacheValue.path.equals("data/index") ? new RuntimeException() : null);
    }

    if (log.isTraceEnabled()) log.trace("incRef(Directory) - end");
  }

  @Override
  public void init(NamedList args) {
    if (log.isTraceEnabled()) log.trace("init(NamedList args={}) - start", args);

    maxWriteMBPerSecFlush = (Double) args.get("maxWriteMBPerSecFlush");
    maxWriteMBPerSecMerge = (Double) args.get("maxWriteMBPerSecMerge");
    maxWriteMBPerSecRead = (Double) args.get("maxWriteMBPerSecRead");
    maxWriteMBPerSecDefault = (Double) args.get("maxWriteMBPerSecDefault");

    // override global config
    if (args.get(SolrXmlConfig.SOLR_DATA_HOME) != null) {
      dataHomePath = Paths.get((String) args.get(SolrXmlConfig.SOLR_DATA_HOME));
    }
    if (dataHomePath != null) {
      log.info(SolrXmlConfig.SOLR_DATA_HOME + "=" + dataHomePath);
    }

    if (log.isTraceEnabled()) log.trace("init(NamedList) - end");
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.solr.core.DirectoryFactory#release(org.apache.lucene.store.Directory
   * )
   */
  @Override
  public void release(Directory directory) throws IOException {
    if (log.isTraceEnabled()) log.trace("release(Directory directory={}) - start", directory);

    if (directory == null) {
      throw new NullPointerException();
    }
    synchronized (this) {
      // don't check if already closed here - we need to able to release
      // while #close() waits.

      CacheValue cacheValue = byDirectoryCache.get(directory);
      if (cacheValue == null) {
        org.apache.solr.common.util.IOUtils.closeQuietly(directory);
        assert ObjectReleaseTracker.release(directory);
        IOUtils.close(directory);
        log.warn("Unknown directory: " + directory
                + " " + byDirectoryCache);
        return;
      }
//      if (cacheValue.path.equals("data/index")) {
//        log.info(
//            "Releasing directory: " + cacheValue.path + " " + (cacheValue.refCnt - 1) + " " + cacheValue.doneWithDir,
//            new RuntimeException("Fake to find stack trace")); // MRM TODO:
//      } else {
      if (log.isDebugEnabled()) log.debug(
              "Releasing directory: " + cacheValue.path + " " + (cacheValue.refCnt - 1) + " " + cacheValue.doneWithDir,  DEBUG_GET_RELEASE && cacheValue.path.equals("data/index") ? new RuntimeException() : null ); // MRM TODO:

      //    }
      cacheValue.refCnt--;

      if (cacheValue.refCnt == 0 && cacheValue.doneWithDir || closed) {
        boolean cl = closeCacheValue(cacheValue);
        if (cl) {
          removeFromCache(cacheValue);
        }
      }
    }

    if (log.isTraceEnabled()) log.trace("release(Directory) - end");
  }

  @Override
  public void remove(String path) throws IOException {
    if (log.isTraceEnabled()) log.trace("remove(String path={}) - start", path);

    remove(path, false);

    if (log.isTraceEnabled()) log.trace("remove(String) - end");
  }

  @Override
  public void remove(Directory dir) throws IOException {
    if (log.isTraceEnabled()) log.trace("remove(Directory dir={}) - start", dir);

    remove(dir, false);
  }

  @Override
  public void remove(String path, boolean deleteAfterCoreClose) throws IOException {
    if (log.isTraceEnabled()) log.trace("remove(String path={}, boolean deleteAfterCoreClose={}) - start", path, deleteAfterCoreClose);

    synchronized (this) {
      CacheValue val = byPathCache.get(normalize(path));
      if (val == null) {
        throw new IllegalArgumentException("Unknown directory " + path);
      }
      val.setDeleteOnClose(true, deleteAfterCoreClose);
    }
  }

  @Override
  public void remove(Directory dir, boolean deleteAfterCoreClose) throws IOException {
    if (log.isTraceEnabled()) log.trace("remove(Directory dir={}, boolean deleteAfterCoreClose={}) - start", dir, deleteAfterCoreClose);


    synchronized (this) {
      CacheValue val = byDirectoryCache.get(dir);
      if (val == null) {
        log.warn("Unknown directory path={}", dir);
        return;
      }
      val.setDeleteOnClose(true, deleteAfterCoreClose);
    }
  }

  protected void removeDirectory(CacheValue cacheValue) throws IOException {
    // this page intentionally left blank
  }

  @Override
  public String normalize(String path) throws IOException {
    if (log.isTraceEnabled()) log.trace("normalize(String path={}) - start", path);


    path = stripTrailingSlash(path);

    return path;
  }

  protected String stripTrailingSlash(String path) {
    if (log.isTraceEnabled()) log.trace("stripTrailingSlash(String path={}) - start", path);

    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }

    return path;
  }

  /**
   * Method for inspecting the cache
   *
   * @return paths in the cache which have not been marked "done"
   * @see #doneWithDirectory
   */
  public synchronized Set<String> getLivePaths() {
    if (log.isTraceEnabled()) log.trace("getLivePaths() - start");

    HashSet<String> livePaths = new HashSet<>(byPathCache.size());
    for (CacheValue val : byPathCache.values()) {
      if (!val.doneWithDir) {
        livePaths.add(val.path);
      }
    }

    if (log.isTraceEnabled()) log.trace("getLivePaths() - end");

    return livePaths;
  }

  @Override
  protected boolean deleteOldIndexDirectory(String oldDirPath) throws IOException {
    if (log.isTraceEnabled()) log.trace("deleteOldIndexDirectory(String oldDirPath={}) - start", oldDirPath);

    Set<String> livePaths = getLivePaths();
    if (livePaths.contains(oldDirPath)) {
      log.warn("Cannot delete directory {} as it is still being referenced in the cache!", oldDirPath);
      return false;
    }

    return super.deleteOldIndexDirectory(oldDirPath);
  }

  protected synchronized String getPath(Directory directory) {
    if (log.isTraceEnabled()) log.trace("getPath(Directory directory={}) - start", directory);

    return byDirectoryCache.get(directory).path;
  }
}
