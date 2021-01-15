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

package org.apache.solr.blob;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CachingDirectoryFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobDirectoryFactory extends CachingDirectoryFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Pattern INDEX_NAME_PATTERN = Pattern.compile("index(?:\\.[0-9]{17})?");

  private String localRootPath;
  private BlobStore blobStore;
  private BlobPusher blobPusher;
  private DirectoryFactory delegateFactory;
  private String delegateLockType;

  // Parameters for MMapDirectory
  // TODO: Change DirectoryFactory.get() upstream to allow us to provide a Function<Directory,
  //  Directory> to wrap the directory when it is created. This would unblock the delegation
  //  of DirectoryFactory here. And we could get rid of these params, we could simply delegate
  //  to a delegateFactory instead.
  private boolean unmapHack;
  private boolean preload;
  private int maxChunk;

  @Override
  public void initCoreContainer(CoreContainer cc) {
    super.initCoreContainer(cc);
    if (delegateFactory != null) {
      delegateFactory.initCoreContainer(cc);
    }
    localRootPath = (dataHomePath == null ? cc.getCoreRootDirectory() : dataHomePath).getParent().toString();
    //        blobListingManager = BlobListingManager.getInstance(cc, "/blobDirListings");
  }

  @Override
  public void init(NamedList args) {
    super.init(args);
    SolrParams params = args.toSolrParams();

    String delegateFactoryClass = params.get("delegateFactory");
    if (delegateFactoryClass == null) {
      throw new IllegalArgumentException("delegateFactory class is required");
    }
    delegateFactory =
        coreContainer.getResourceLoader().newInstance(delegateFactoryClass, DirectoryFactory.class);
    delegateFactory.initCoreContainer(coreContainer);
    delegateFactory.init(args);

    delegateLockType = params.get("delegateLockType");
    if (delegateLockType == null) {
      throw new IllegalArgumentException("delegateLockType is required");
    }

    String blobRootDir = params.get("blobRootDir");
    if (blobRootDir == null) {
      throw new IllegalArgumentException("blobRootDir is required");
    }
    blobStore = null;//TODO new BlobStore(blobRootDir);
    blobPusher = new BlobPusher(blobStore);

    maxChunk = params.getInt("maxChunkSize", MMapDirectory.DEFAULT_MAX_CHUNK_SIZE);
    if (maxChunk <= 0) {
      throw new IllegalArgumentException("maxChunk must be greater than 0");
    }
    unmapHack = params.getBool("unmap", true);
    preload = params.getBool("preload", false); // default turn-off
  }

  @Override
  public void doneWithDirectory(Directory directory) throws IOException {
    log.debug("doneWithDirectory {}", directory);
    ((BlobDirectory) directory).release();
    // TODO delegateFactory.doneWithDirectory(directory);
    super.doneWithDirectory(directory);
  }

  @Override
  public void close() throws IOException {
    log.debug("close");
    IOUtils.closeQuietly(blobStore);
    IOUtils.closeQuietly(blobPusher);
    IOUtils.closeQuietly(delegateFactory);
    super.close();
  }

  @Override
  protected LockFactory createLockFactory(String rawLockType) throws IOException {
    return rawLockType.equals(DirectoryFactory.LOCK_TYPE_NONE)
        ? NoLockFactory.INSTANCE
        : NativeFSLockFactory.INSTANCE;
    // TODO return rawLockType.equals(DirectoryFactory.LOCK_TYPE_NONE) ? NoLockFactory.INSTANCE :
    //  DELEGATE_LOCK_FACTORY;
  }

  @Override
  protected Directory create(String path, LockFactory lockFactory, DirContext dirContext)
      throws IOException {
    log.debug("Create Directory {}", path);
    MMapDirectory mapDirectory = new MMapDirectory(new File(path).toPath(), lockFactory, maxChunk);
    try {
      mapDirectory.setUseUnmap(unmapHack);
    } catch (IllegalArgumentException e) {
      log.warn("Unmap not supported on this JVM, continuing on without setting unmap", e);
    }
    mapDirectory.setPreload(preload);
    Directory delegateDirectory = mapDirectory;
    // TODO
    //  String delegateLockType = lockFactory == NoLockFactory.INSTANCE ?
    //  DirectoryFactory.LOCK_TYPE_NONE : this.delegateLockType;
    //  Directory delegateDirectory = delegateFactory.get(path, dirContext, delegateLockType);
    String blobDirPath = getRelativePath(path, localRootPath);
    return new BlobDirectory(delegateDirectory, blobDirPath, blobPusher);
  }

  private String getRelativePath(String path, String referencePath) {
    if (!path.startsWith(referencePath)) {
      throw new IllegalArgumentException("Path=" + path + " is expected to start with referencePath="
              + referencePath + " otherwise we have to adapt the code");
    }
    String relativePath = path.substring(referencePath.length());
    if (relativePath.startsWith("/")) {
      relativePath = relativePath.substring(1);
    }
    return relativePath;
  }

  @Override
  public boolean exists(String path) throws IOException {
    boolean exists = super.exists(path);
    log.debug("exists {} = {}", path, exists);
    return exists;
    // TODO return delegateFactory.exists(path);
  }

  @Override
  protected void removeDirectory(CacheValue cacheValue) throws IOException {
    log.debug("removeDirectory {}", cacheValue);
    File dirFile = new File(cacheValue.path);
    FileUtils.deleteDirectory(dirFile);
    // TODO delegateFactory.remove(cacheValue.path);
    String blobDirPath = getRelativePath(cacheValue.path, localRootPath);
    blobStore.deleteDirectory(blobDirPath);
  }

  @Override
  public void move(Directory fromDir, Directory toDir, String fileName, IOContext ioContext)
      throws IOException {
    // TODO: override for efficiency?
    log.debug("move {} {} to {}", fromDir, fileName, toDir);
    super.move(fromDir, toDir, fileName, ioContext);
  }

  @Override
  public void renameWithOverwrite(Directory dir, String fileName, String toName)
      throws IOException {
    // TODO: override to perform an atomic rename if possible?
    log.debug("renameWithOverwrite {} {} to {}", dir, fileName, toName);
    super.renameWithOverwrite(dir, fileName, toName);
  }

  @Override
  public boolean isPersistent() {
    return true;
  }

  @Override
  public boolean isSharedStorage() {
    return true;
  }

  @Override
  public void release(Directory directory) throws IOException {
    log.debug("release {}", directory);
    ((BlobDirectory) directory).release();
    // TODO delegateFactory.release(directory);
    super.release(directory);
  }

  @Override
  public boolean isAbsolute(String path) {
    boolean isAbsolute = new File(path).isAbsolute();
    log.debug("isAbsolute {} = {}", path, isAbsolute);
    return isAbsolute;
    // TODO return delegateFactory.isAbsolute(path);
  }

  @Override
  public boolean searchersReserveCommitPoints() {
    return false; // TODO: double check
  }

  @Override
  public String getDataHome(CoreDescriptor cd) throws IOException {
    String dataHome = super.getDataHome(cd);
    log.debug("getDataHome {}", dataHome);
    return dataHome;
  }

  @Override
  public void cleanupOldIndexDirectories(String dataDirPath, String currentIndexDirPath, boolean afterCoreReload) {
    log.debug("cleanupOldIndexDirectories {} {}", dataDirPath, currentIndexDirPath);

    super.cleanupOldIndexDirectories(dataDirPath, currentIndexDirPath, afterCoreReload);
    // TODO delegateFactory.cleanupOldIndexDirectories(dataDirPath, currentIndexDirPath, afterCoreReload);

    try {
      dataDirPath = normalize(dataDirPath);
      currentIndexDirPath = normalize(currentIndexDirPath);
    } catch (IOException e) {
      log.error("Failed to delete old index directories in {} due to: ", dataDirPath, e);
    }
    String blobDirPath = getRelativePath(dataDirPath, localRootPath);
    String currentIndexDirName = getRelativePath(currentIndexDirPath, dataDirPath);
    List<String> oldIndexDirs;
    try {
      oldIndexDirs = blobStore.listInDirectory(blobDirPath,
              (name) -> !name.equals(currentIndexDirName)
                      && INDEX_NAME_PATTERN.matcher(name).matches());
    } catch (IOException e) {
      log.error("Failed to delete old index directories in {} due to: ", blobDirPath, e);
      return;
    }
    if (oldIndexDirs.isEmpty()) {
      return;
    }
    if (afterCoreReload) {
      // Do not remove the most recent old directory after a core reload.
      if (oldIndexDirs.size() == 1) {
        return;
      }
      oldIndexDirs.sort(null);
      oldIndexDirs = oldIndexDirs.subList(0, oldIndexDirs.size() - 1);
    }
    try {
      blobStore.deleteDirectories(blobDirPath, oldIndexDirs);
    } catch (IOException e) {
      log.error("Failed to delete old index directories {} in {} due to: ", oldIndexDirs, blobDirPath, e);
    }
  }
}
