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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public CacheValue(String path, Directory directory) {
      this.path = path;
      this.directory = directory;
      this.closeEntries.add(this);
      // for debug
      // this.originTrace = new RuntimeException("Originated from:");
    }

    public int refCnt = 1;
    // has doneWithDirectory(Directory) been called on this?
    public boolean closeCacheValueCalled = false;
    public boolean doneWithDir = false;
    private boolean deleteAfterCoreClose = false;
    public Set<CacheValue> removeEntries = new HashSet<>();
    public Set<CacheValue> closeEntries = new HashSet<>();

    public void setDeleteOnClose(boolean deleteOnClose, boolean deleteAfterCoreClose) {
      if (deleteOnClose) {
        removeEntries.add(this);
      }
      this.deleteOnClose = deleteOnClose;
      this.deleteAfterCoreClose = deleteAfterCoreClose;
    }

    @Override
    public String toString() {
      return "CachedDir<<" + "refCount=" + refCnt + ";path=" + path + ";done=" + doneWithDir + ">>";
    }
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected Map<String, CacheValue> byPathCache = new HashMap<>();

  protected Map<Directory, CacheValue> byDirectoryCache = new IdentityHashMap<>();

  protected Map<Directory, List<CloseListener>> closeListeners = new HashMap<>();

  protected Set<CacheValue> removeEntries = new HashSet<>();

  private Double maxWriteMBPerSecFlush;

  private Double maxWriteMBPerSecMerge;

  private Double maxWriteMBPerSecRead;

  private Double maxWriteMBPerSecDefault;

  private boolean closed;

  public interface CloseListener {
    public void postClose();

    public void preClose();
  }

  @Override
  public void addCloseListener(Directory dir, CloseListener closeListener) {
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
  }

  @Override
  public void doneWithDirectory(Directory directory) throws IOException {
    synchronized (this) {
      CacheValue cacheValue = byDirectoryCache.get(directory);
      if (cacheValue == null) {
        throw new IllegalArgumentException("Unknown directory: " + directory
            + " " + byDirectoryCache);
      }
      cacheValue.doneWithDir = true;
      log.debug("Done with dir: {}", cacheValue);
      if (cacheValue.refCnt == 0 && !closed) {
        boolean cl = closeCacheValue(cacheValue);
        if (cl) {
          removeFromCache(cacheValue);
        }
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.solr.core.DirectoryFactory#close()
   */
  @Override
  public void close() throws IOException {
    synchronized (this) {
      if (log.isDebugEnabled()) {
        log.debug("Closing {} - {} directories currently being tracked", this.getClass().getSimpleName(), byDirectoryCache.size());
      }
      this.closed = true;
      Collection<CacheValue> values = byDirectoryCache.values();
      for (CacheValue val : values) {

        if (log.isDebugEnabled()) {
          log.debug("Closing {} - currently tracking: {}", this.getClass().getSimpleName(), val);
        }
        try {
          // if there are still refs out, we have to wait for them
          assert val.refCnt > -1 : val.refCnt;
          int cnt = 0;
          while (val.refCnt != 0) {
            wait(100);

            if (cnt++ >= 120) {
              String msg = "Timeout waiting for all directory ref counts to be released - gave up waiting on " + val;
              log.error(msg);
              // debug
              // val.originTrace.printStackTrace();
              throw new SolrException(ErrorCode.SERVER_ERROR, msg);
            }
          }
          assert val.refCnt == 0 : val.refCnt;
        } catch (Exception e) {
          SolrException.log(log, "Error closing directory", e);
        }
      }

      values = byDirectoryCache.values();
      Set<CacheValue> closedDirs = new HashSet<>();
      for (CacheValue val : values) {
        try {
          for (CacheValue v : val.closeEntries) {
            assert v.refCnt == 0 : val.refCnt;
            log.debug("Closing directory when closing factory: {}", v.path);
            boolean cl = closeCacheValue(v);
            if (cl) {
              closedDirs.add(v);
            }
          }
        } catch (Exception e) {
          SolrException.log(log, "Error closing directory", e);
        }
      }

      for (CacheValue val : removeEntries) {
        log.debug("Removing directory after core close: {}", val.path);
        try {
          removeDirectory(val);
        } catch (Exception e) {
          SolrException.log(log, "Error removing directory", e);
        }
      }

      for (CacheValue v : closedDirs) {
        removeFromCache(v);
      }
    }
  }

  private void removeFromCache(CacheValue v) {
    log.debug("Removing from cache: {}", v);
    byDirectoryCache.remove(v.directory);
    byPathCache.remove(v.path);
  }

  // be sure this is called with the this sync lock
  // returns true if we closed the cacheValue, false if it will be closed later
  private boolean closeCacheValue(CacheValue cacheValue) {
    log.debug("looking to close {} {}", cacheValue.path, cacheValue.closeEntries);
    List<CloseListener> listeners = closeListeners.remove(cacheValue.directory);
    if (listeners != null) {
      for (CloseListener listener : listeners) {
        try {
          listener.preClose();
        } catch (Exception e) {
          SolrException.log(log, "Error executing preClose for directory", e);
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
        log.debug("Removing directory before core close: {}", val.path);
        try {
          removeDirectory(val);
        } catch (Exception e) {
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
          SolrException.log(log, "Error executing postClose for directory", e);
        }
      }
    }
    return cl;
  }

  private void close(CacheValue val) {
    if (log.isDebugEnabled()) {
      log.debug("Closing directory, CoreContainer#isShutdown={}", coreContainer != null ? coreContainer.isShutDown() : "null");
    }
    try {
      if (coreContainer != null && coreContainer.isShutDown() && val.directory instanceof ShutdownAwareDirectory) {
        log.debug("Closing directory on shutdown: {}", val.path);
        ((ShutdownAwareDirectory) val.directory).closeOnShutdown();
      } else {
        log.debug("Closing directory: {}", val.path);
        val.directory.close();
      }
      assert ObjectReleaseTracker.release(val.directory);
    } catch (Exception e) {
      SolrException.log(log, "Error closing directory", e);
    }
  }

  private boolean isSubPath(CacheValue cacheValue, CacheValue otherCacheValue) {
    int one = cacheValue.path.lastIndexOf('/');
    int two = otherCacheValue.path.lastIndexOf('/');

    return otherCacheValue.path.startsWith(cacheValue.path + "/") && two > one;
  }

  @Override
  public boolean exists(String path) throws IOException {
    // we go by the persistent storage ...
    Path dirPath = FileSystems.getDefault().getPath(path);
    if (Files.isReadable(dirPath)) {
      try (DirectoryStream<Path> directory = Files.newDirectoryStream(dirPath)) {
        return directory.iterator().hasNext();
      }
    }
    return false;
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
    String fullPath = normalize(path);
    synchronized (this) {
      if (closed) {
        throw new AlreadyClosedException("Already closed");
      }

      final CacheValue cacheValue = byPathCache.get(fullPath);
      Directory directory = null;
      if (cacheValue != null) {
        directory = cacheValue.directory;
      }

      if (directory == null) {
        directory = create(fullPath, createLockFactory(rawLockType), dirContext);
        assert ObjectReleaseTracker.track(directory);
        boolean success = false;
        try {
          CacheValue newCacheValue = new CacheValue(fullPath, directory);
          byDirectoryCache.put(directory, newCacheValue);
          byPathCache.put(fullPath, newCacheValue);
          log.debug("return new directory for {}", fullPath);
          success = true;
        } finally {
          if (!success) {
            IOUtils.closeWhileHandlingException(directory);
          }
        }
      } else {
        cacheValue.refCnt++;
        log.debug("Reusing cached directory: {}", cacheValue);
      }

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
    synchronized (this) {
      if (closed) {
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Already closed");
      }
      CacheValue cacheValue = byDirectoryCache.get(directory);
      if (cacheValue == null) {
        throw new IllegalArgumentException("Unknown directory: " + directory);
      }

      cacheValue.refCnt++;
      log.debug("incRef'ed: {}", cacheValue);
    }
  }

  @Override
  public void init(@SuppressWarnings("rawtypes") NamedList args) {
    maxWriteMBPerSecFlush = (Double) args.get("maxWriteMBPerSecFlush");
    maxWriteMBPerSecMerge = (Double) args.get("maxWriteMBPerSecMerge");
    maxWriteMBPerSecRead = (Double) args.get("maxWriteMBPerSecRead");
    maxWriteMBPerSecDefault = (Double) args.get("maxWriteMBPerSecDefault");

    // override global config
    if (args.get(SolrXmlConfig.SOLR_DATA_HOME) != null) {
      dataHomePath = Paths.get((String) args.get(SolrXmlConfig.SOLR_DATA_HOME)).toAbsolutePath().normalize();
    }
    if (dataHomePath != null) {
      log.info("{} = {}", SolrXmlConfig.SOLR_DATA_HOME, dataHomePath);
    }
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
    if (directory == null) {
      throw new NullPointerException();
    }
    synchronized (this) {
      // don't check if already closed here - we need to able to release
      // while #close() waits.

      CacheValue cacheValue = byDirectoryCache.get(directory);
      if (cacheValue == null) {
        throw new IllegalArgumentException("Unknown directory: " + directory
            + " " + byDirectoryCache);
      }
      if (log.isDebugEnabled()) {
        log.debug("Releasing directory: {} {} {}", cacheValue.path, (cacheValue.refCnt - 1), cacheValue.doneWithDir);
      }

      cacheValue.refCnt--;

      assert cacheValue.refCnt >= 0 : cacheValue.refCnt;

      if (cacheValue.refCnt == 0 && cacheValue.doneWithDir && !closed) {
        boolean cl = closeCacheValue(cacheValue);
        if (cl) {
          removeFromCache(cacheValue);
        }
      }
    }
  }

  @Override
  public void remove(String path) throws IOException {
    remove(path, false);
  }

  @Override
  public void remove(Directory dir) throws IOException {
    remove(dir, false);
  }

  @Override
  public void remove(String path, boolean deleteAfterCoreClose) throws IOException {
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
    synchronized (this) {
      CacheValue val = byDirectoryCache.get(dir);
      if (val == null) {
        throw new IllegalArgumentException("Unknown directory " + dir);
      }
      val.setDeleteOnClose(true, deleteAfterCoreClose);
    }
  }

  protected synchronized void removeDirectory(CacheValue cacheValue) throws IOException {
    // this page intentionally left blank
  }

  @Override
  public String normalize(String path) throws IOException {
    path = stripTrailingSlash(path);
    return path;
  }

  protected String stripTrailingSlash(String path) {
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
    HashSet<String> livePaths = new HashSet<>();
    for (CacheValue val : byPathCache.values()) {
      if (!val.doneWithDir) {
        livePaths.add(val.path);
      }
    }
    return livePaths;
  }

  @Override
  protected boolean deleteOldIndexDirectory(String oldDirPath) throws IOException {
    Set<String> livePaths = getLivePaths();
    if (livePaths.contains(oldDirPath)) {
      log.warn("Cannot delete directory {} as it is still being referenced in the cache!", oldDirPath);
      return false;
    }

    return super.deleteOldIndexDirectory(oldDirPath);
  }

  protected synchronized String getPath(Directory directory) {
    return byDirectoryCache.get(directory).path;
  }
}
