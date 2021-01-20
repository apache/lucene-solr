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

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.LockFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CachingDirectoryFactory.CloseListener;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides access to a Directory implementation. You must release every
 * Directory that you get.
 */
public abstract class DirectoryFactory implements NamedListInitializedPlugin,
    Closeable {

  // Estimate 10M docs, 100GB size, to avoid caching by NRTCachingDirectory
  // Stayed away from upper bounds of the int/long in case any other code tried to aggregate these numbers.
  // A large estimate should currently have no other side effects.
  public static final IOContext IOCONTEXT_NO_CACHE = new IOContext(new FlushInfo(10*1000*1000, 100L*1000*1000*1000));

  protected static final String INDEX_W_TIMESTAMP_REGEX = "index\\.[0-9]{17}"; // see SnapShooter.DATE_FMT

  // May be set by sub classes as data root, in which case getDataHome will use it as base.  Absolute.
  protected Path dataHomePath;

  // hint about what the directory contains - default is index directory
  public enum DirContext {DEFAULT, META_DATA}

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // Available lock types
  public final static String LOCK_TYPE_SIMPLE = "simple";
  public final static String LOCK_TYPE_NATIVE = "native";
  public final static String LOCK_TYPE_SINGLE = "single";
  public final static String LOCK_TYPE_NONE   = "none";
  public final static String LOCK_TYPE_HDFS   = "hdfs";

  protected volatile CoreContainer coreContainer;
  
  /**
   * Indicates a Directory will no longer be used, and when its ref count
   * hits 0, it can be closed. On close all directories will be closed
   * whether this has been called or not. This is simply to allow early cleanup.
   * 
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void doneWithDirectory(Directory directory) throws IOException;
  
  /**
   * Adds a close listener for a Directory.
   */
  public abstract void addCloseListener(Directory dir, CloseListener closeListener);
  
  /**
   * Close the this and all of the Directories it contains.
   * 
   * @throws IOException If there is a low-level I/O error.
   */
  @Override
  public abstract void close() throws IOException;
  
  /**
   * Creates a new Directory for a given path.
   * 
   * @throws IOException If there is a low-level I/O error.
   */
  protected abstract Directory create(String path, LockFactory lockFactory, DirContext dirContext) throws IOException;
  
  /**
   * Creates a new LockFactory for a given path.
   * @param rawLockType A string value as passed in config. Every factory should at least support 'none' to disable locking.
   * @throws IOException If there is a low-level I/O error.
   */
  protected abstract LockFactory createLockFactory(String rawLockType) throws IOException;
  
  /**
   * Returns true if a Directory exists for a given path in the underlying (stable) storage <em>and</em> 
   * contains at least one file.  
   * Note that the existence of a {@link Directory} <em>Object</em> as returned by a previous call to the 
   * {@link #get} method (on the specified <code>path</code>) is not enough to cause this method to return 
   * true.  Some prior user of that Directory must have written &amp; synced at least one file to that 
   * Directory (and at least one file must still exist)
   *
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract boolean exists(String path) throws IOException;
  
  /**
   * Removes the Directory's persistent storage.
   * For example: A file system impl may remove the
   * on disk directory.
   * @throws IOException If there is a low-level I/O error.
   * 
   */
  public abstract void remove(Directory dir) throws IOException;
  
  /**
   * Removes the Directory's persistent storage.
   * For example: A file system impl may remove the
   * on disk directory.
   * @throws IOException If there is a low-level I/O error.
   * 
   */
  public abstract void remove(Directory dir, boolean afterCoreClose) throws IOException;
  
  /**
   * This remove is special in that it may be called even after
   * the factory has been closed. Remove only makes sense for
   * persistent directory factories.
   * 
   * @param path to remove
   * @param afterCoreClose whether to wait until after the core is closed.
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void remove(String path, boolean afterCoreClose) throws IOException;
  
  /**
   * This remove is special in that it may be called even after
   * the factory has been closed. Remove only makes sense for
   * persistent directory factories.
   * 
   * @param path to remove
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void remove(String path) throws IOException;
  
  /**
   * @param directory to calculate size of
   * @return size in bytes
   * @throws IOException on low level IO error
   */
  public long size(Directory directory) throws IOException {
    return sizeOfDirectory(directory);
  }
  
  /**
   * @param path to calculate size of
   * @return size in bytes
   * @throws IOException on low level IO error
   */
  public long size(String path) throws IOException {
    Directory dir = get(path, DirContext.DEFAULT, null);
    long size;
    try {
      size = sizeOfDirectory(dir);
    } finally {
      release(dir); 
    }
    return size;
  }
  
  /**
   * Override for more efficient moves.
   * 
   * Intended for use with replication - use
   * carefully - some Directory wrappers will
   * cache files for example.
   * 
   * @throws IOException If there is a low-level I/O error.
   */
  public void move(Directory fromDir, Directory toDir, String fileName, IOContext ioContext) throws IOException {
    toDir.copyFrom(fromDir, fileName, fileName, ioContext);
    fromDir.deleteFile(fileName);
  }
  
  // sub classes perform an atomic rename if possible, otherwise fall back to delete + rename
  // this is important to support for index roll over durability after crashes
  public void renameWithOverwrite(Directory dir, String fileName, String toName) throws IOException {
    try {
      dir.deleteFile(toName);
    } catch (FileNotFoundException | NoSuchFileException e) {

    } catch (Exception e) {
      log.error("Exception deleting file", e);
    }

    dir.rename(fileName, toName);
  }
  
  /**
   * Returns the Directory for a given path, using the specified rawLockType.
   * Will return the same Directory instance for the same path.
   * 
   * 
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract Directory get(String path, DirContext dirContext, String rawLockType)
      throws IOException;
  
  /**
   * Increment the number of references to the given Directory. You must call
   * release for every call to this method.
   * 
   */
  public abstract void incRef(Directory directory);
  
  
  /**
   * @return true if data is kept after close.
   */
  public abstract boolean isPersistent();
  
  /**
   * @return true if storage is shared.
   */
  public boolean isSharedStorage() {
    return false;
  }
  
  /**
   * Releases the Directory so that it may be closed when it is no longer
   * referenced.
   * 
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void release(Directory directory) throws IOException;
  
  /**
   * Normalize a given path.
   * 
   * @param path to normalize
   * @return normalized path
   * @throws IOException on io error
   */
  public String normalize(String path) throws IOException {
    return path;
  }
  
  /**
   * @param path the path to check
   * @return true if absolute, as in not relative
   */
  public boolean isAbsolute(String path) {
    // back compat
    return new File(path).isAbsolute();
  }
  
  public static long sizeOfDirectory(Directory directory) throws IOException {
    final String[] files = directory.listAll();
    long size = 0;
    
    for (final String file : files) {
      size += sizeOf(directory, file);
      if (size < 0) {
        break;
      }
    }
    
    return size;
  }
  
  public static long sizeOf(Directory directory, String file) throws IOException {
    try {
      return directory.fileLength(file);
    } catch (IOException e) {
      // could be a race, file no longer exists, access denied, is a directory, etc.
      return 0;
    }
  }
  
  /**
   * Delete the files in the Directory
   */
  public static boolean empty(Directory dir) {
    boolean isSuccess = true;
    String contents[];
    try {
      contents = dir.listAll();
      if (contents != null) {
        for (String file : contents) {
          dir.deleteFile(file);
        }
      }
    } catch (IOException e) {
      SolrException.log(log, "Error deleting files from Directory", e);
      isSuccess = false;
    }
    return isSuccess;
  }

  /**
   * If your implementation can count on delete-on-last-close semantics
   * or throws an exception when trying to remove a file in use, return
   * false (eg NFS). Otherwise, return true. Defaults to returning false.
   * 
   * @return true if factory impl requires that Searcher's explicitly
   * reserve commit points.
   */
  public boolean searchersReserveCommitPoints() {
    return false;
  }

  /**
   * Get the data home folder. If solr.data.home is set, that is used, else base on instanceDir
   * @param cd core descriptor instance
   * @return a String with absolute path to data direcotry
   */
  public String getDataHome(CoreDescriptor cd) throws IOException {
    Path dataDir;
    if (dataHomePath != null) {
      Path instanceDirLastPath = cd.getInstanceDir().getName(cd.getInstanceDir().getNameCount()-1);
      dataDir = dataHomePath.resolve(instanceDirLastPath).resolve(cd.getDataDir());
    } else {
      // by default, we go off the instance directory
      dataDir = cd.getInstanceDir().resolve(cd.getDataDir());
    }
    assert dataDir.isAbsolute();
    return dataDir.toString();
  }

  public void cleanupOldIndexDirectories(final String dataDirPath, final String currentIndexDirPath, boolean afterCoreReload) {
    File dataDir = new File(dataDirPath);
    if (!dataDir.isDirectory()) {
      log.debug("{} does not point to a valid data directory; skipping clean-up of old index directories.", dataDirPath);
      return;
    }

    final File currentIndexDir = new File(currentIndexDirPath);
    File[] oldIndexDirs = dataDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {
        String fileName = file.getName();
        return file.isDirectory() &&
               !file.equals(currentIndexDir) &&
               (fileName.equals("index") || fileName.matches(INDEX_W_TIMESTAMP_REGEX));
      }
    });

    if (oldIndexDirs == null || oldIndexDirs.length == 0)
      return; // nothing to do (no log message needed)

    List<File> dirsList = Arrays.asList(oldIndexDirs);
    Collections.sort(dirsList, Collections.reverseOrder());
    
    int i = 0;
    if (afterCoreReload) {
      log.info("Will not remove most recent old directory after reload {}", oldIndexDirs[0]);
      i = 1;
    }
    log.info("Found {} old index directories to clean-up under {} afterReload={}", oldIndexDirs.length - i, dataDirPath, afterCoreReload);
    for (; i < dirsList.size(); i++) {
      File dir = dirsList.get(i);
      String dirToRmPath = dir.getAbsolutePath();
      try {
        if (deleteOldIndexDirectory(dirToRmPath)) {
          log.info("Deleted old index directory: {}", dirToRmPath);
        } else {
          log.warn("Delete old index directory {} failed.", dirToRmPath);
        }
      } catch (IOException ioExc) {
        log.error("Failed to delete old directory {} due to: ", dir.getAbsolutePath(), ioExc);
      }
    }
  }

  // Extension point to allow sub-classes to infuse additional code when deleting old index directories
  protected boolean deleteOldIndexDirectory(String oldDirPath) throws IOException {
    File dirToRm = new File(oldDirPath);
    FileUtils.deleteDirectory(dirToRm);
    return !dirToRm.isDirectory();
  }
  
  public void initCoreContainer(CoreContainer cc) {
    this.coreContainer = cc;
    if (cc != null && cc.getConfig() != null) {
      this.dataHomePath = cc.getConfig().getSolrDataHome(); // absolute
    }
  }
  
  // special hack to work with FilterDirectory
  protected Directory getBaseDir(Directory dir) {
    Directory baseDir = dir;
    while (baseDir instanceof FilterDirectory) {
      baseDir = ((FilterDirectory)baseDir).getDelegate();
    } 
    
    return baseDir;
  }

  /**
   * Create a new DirectoryFactory instance from the given SolrConfig and tied to the specified core container.
   */
  static DirectoryFactory loadDirectoryFactory(SolrConfig config, CoreContainer cc, String registryName) {
    final PluginInfo info = config.getPluginInfo(DirectoryFactory.class.getName());
    final DirectoryFactory dirFactory;
    if (info != null) {
      log.debug(info.className);
      dirFactory = config.getResourceLoader().newInstance (info, DirectoryFactory.class, true);
      // allow DirectoryFactory instances to access the CoreContainer
      dirFactory.initCoreContainer(cc);
      dirFactory.init(info.initArgs);
    } else {
      log.debug("solr.NRTCachingDirectoryFactory");
      dirFactory = new NRTCachingDirectoryFactory();
      dirFactory.initCoreContainer(cc);
    }
    return dirFactory;
  }
}
