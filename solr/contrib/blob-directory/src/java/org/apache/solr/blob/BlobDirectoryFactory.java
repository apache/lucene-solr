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
import java.lang.invoke.MethodHandles;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CachingDirectoryFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO now: still failing tests (run CoreSynonymLoadTest no SolrCloud) because we have to support
// removing dir and old
// indexes in BlobStore. See all "TODO now"

public class BlobDirectoryFactory extends CachingDirectoryFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private DirectoryFactory delegateFactory;
  private String delegateLockType;
  private String blobPath;

  // Parameters for MMapDirectory
  // TODO: Change DirectoryFactory.get() upstream to allow us to provide a Function<Directory,
  // Directory> to wrap the
  //  directory when it is created. This would unblock the delegation of DirectoryFactory here. And
  // we could get rid
  //  of these params, we could simply delegate to a delegateFactory instead.
  private boolean unmapHack;
  private boolean preload;
  private int maxChunk;

  @Override
  public void initCoreContainer(CoreContainer cc) {
    super.initCoreContainer(cc);
    if (delegateFactory != null) {
      delegateFactory.initCoreContainer(cc);
    }
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

    blobPath = params.get("blobPath");
    if (blobPath == null) {
      throw new IllegalArgumentException("blobPath is required");
    }

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
    // TODO delegateFactory.close();
    super.close();
  }

  @Override
  protected LockFactory createLockFactory(String rawLockType) throws IOException {
    return rawLockType.equals(DirectoryFactory.LOCK_TYPE_NONE)
        ? NoLockFactory.INSTANCE
        : NativeFSLockFactory.INSTANCE;
    // TODO return rawLockType.equals(DirectoryFactory.LOCK_TYPE_NONE) ? NoLockFactory.INSTANCE :
    // DELEGATE_LOCK_FACTORY;
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
    // String delegateLockType = lockFactory == NoLockFactory.INSTANCE ?
    // DirectoryFactory.LOCK_TYPE_NONE : this.delegateLockType;
    // Directory delegateDirectory = delegateFactory.get(path, dirContext, delegateLockType);
    BlobPusher blobPusher =
        null; // nocommit new BlobPusher(new BlobStore(blobPath, path));//TODO now: Reuse BlobPusher
    return new BlobDirectory(delegateDirectory, blobPusher);
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
    BlobPusher blobPusher =
        null; // nocommit new BlobPusher(new BlobStore(blobPath, cacheValue.path));//TODO now: Reuse
    // BlobPusher
    // TODO now: blobPusher.deleteDirectory();
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
  public void cleanupOldIndexDirectories(
      final String dataDirPath, final String currentIndexDirPath, boolean afterCoreReload) {
    log.debug("cleanupOldIndexDirectories {} {}", dataDirPath, currentIndexDirPath);
    super.cleanupOldIndexDirectories(dataDirPath, currentIndexDirPath, afterCoreReload);
    // TODO now: cleanup with BlobPusher
  }
}
