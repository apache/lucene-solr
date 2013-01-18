package org.apache.lucene.store;

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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.RAMDirectory;      // javadocs
import org.apache.lucene.util.IOUtils;

// TODO
//   - let subclass dictate policy...?
//   - rename to MergeCacheingDir?  NRTCachingDir

/**
 * Wraps a {@link RAMDirectory}
 * around any provided delegate directory, to
 * be used during NRT search.
 *
 * <p>This class is likely only useful in a near-real-time
 * context, where indexing rate is lowish but reopen
 * rate is highish, resulting in many tiny files being
 * written.  This directory keeps such segments (as well as
 * the segments produced by merging them, as long as they
 * are small enough), in RAM.</p>
 *
 * <p>This is safe to use: when your app calls {IndexWriter#commit},
 * all cached files will be flushed from the cached and sync'd.</p>
 *
 * <p>Here's a simple example usage:
 *
 * <pre class="prettyprint">
 *   Directory fsDir = FSDirectory.open(new File("/path/to/index"));
 *   NRTCachingDirectory cachedFSDir = new NRTCachingDirectory(fsDir, 5.0, 60.0);
 *   IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_32, analyzer);
 *   IndexWriter writer = new IndexWriter(cachedFSDir, conf);
 * </pre>
 *
 * <p>This will cache all newly flushed segments, all merges
 * whose expected segment size is <= 5 MB, unless the net
 * cached bytes exceeds 60 MB at which point all writes will
 * not be cached (until the net bytes falls below 60 MB).</p>
 *
 * @lucene.experimental
 */

public class NRTCachingDirectory extends Directory {

  private final RAMDirectory cache = new RAMDirectory();

  private final Directory delegate;

  private final long maxMergeSizeBytes;
  private final long maxCachedBytes;

  private static final boolean VERBOSE = false;

  /**
   *  We will cache a newly created output if 1) it's a
   *  flush or a merge and the estimated size of the merged segment is <=
   *  maxMergeSizeMB, and 2) the total cached bytes is <=
   *  maxCachedMB */
  public NRTCachingDirectory(Directory delegate, double maxMergeSizeMB, double maxCachedMB) {
    this.delegate = delegate;
    maxMergeSizeBytes = (long) (maxMergeSizeMB*1024*1024);
    maxCachedBytes = (long) (maxCachedMB*1024*1024);
  }

  public Directory getDelegate() {
    return delegate;
  }

  @Override
  public LockFactory getLockFactory() {
    return delegate.getLockFactory();
  }

  @Override
  public void setLockFactory(LockFactory lf) throws IOException {
    delegate.setLockFactory(lf);
  }

  @Override
  public String getLockID() {
    return delegate.getLockID();
  }

  @Override
  public Lock makeLock(String name) {
    return delegate.makeLock(name);
  }

  @Override
  public void clearLock(String name) throws IOException {
    delegate.clearLock(name);
  }

  @Override
  public String toString() {
    return "NRTCachingDirectory(" + delegate + "; maxCacheMB=" + (maxCachedBytes/1024/1024.) + " maxMergeSizeMB=" + (maxMergeSizeBytes/1024/1024.) + ")";
  }

  @Override
  public synchronized String[] listAll() throws IOException {
    final Set<String> files = new HashSet<String>();
    for(String f : cache.listAll()) {
      files.add(f);
    }
    // LUCENE-1468: our NRTCachingDirectory will actually exist (RAMDir!),
    // but if the underlying delegate is an FSDir and mkdirs() has not
    // yet been called, because so far everything is a cached write,
    // in this case, we don't want to throw a NoSuchDirectoryException
    try {
      for(String f : delegate.listAll()) {
        // Cannot do this -- if lucene calls createOutput but
        // file already exists then this falsely trips:
        //assert !files.contains(f): "file \"" + f + "\" is in both dirs";
        files.add(f);
      }
    } catch (NoSuchDirectoryException ex) {
      // however, if there are no cached files, then the directory truly
      // does not "exist"
      if (files.isEmpty()) {
        throw ex;
      }
    }
    return files.toArray(new String[files.size()]);
  }

  /** Returns how many bytes are being used by the
   *  RAMDirectory cache */
  public long sizeInBytes()  {
    return cache.sizeInBytes();
  }

  @Override
  public synchronized boolean fileExists(String name) throws IOException {
    return cache.fileExists(name) || delegate.fileExists(name);
  }

  @Override
  public synchronized void deleteFile(String name) throws IOException {
    if (VERBOSE) {
      System.out.println("nrtdir.deleteFile name=" + name);
    }
    if (cache.fileExists(name)) {
      assert !delegate.fileExists(name): "name=" + name;
      cache.deleteFile(name);
    } else {
      delegate.deleteFile(name);
    }
  }

  @Override
  public synchronized long fileLength(String name) throws IOException {
    if (cache.fileExists(name)) {
      return cache.fileLength(name);
    } else {
      return delegate.fileLength(name);
    }
  }

  public String[] listCachedFiles() {
    return cache.listAll();
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    if (VERBOSE) {
      System.out.println("nrtdir.createOutput name=" + name);
    }
    if (doCacheWrite(name, context)) {
      if (VERBOSE) {
        System.out.println("  to cache");
      }
      try {
        delegate.deleteFile(name);
      } catch (IOException ioe) {
        // This is fine: file may not exist
      }
      return cache.createOutput(name, context);
    } else {
      try {
        cache.deleteFile(name);
      } catch (IOException ioe) {
        // This is fine: file may not exist
      }
      return delegate.createOutput(name, context);
    }
  }

  @Override
  public void sync(Collection<String> fileNames) throws IOException {
    if (VERBOSE) {
      System.out.println("nrtdir.sync files=" + fileNames);
    }
    for(String fileName : fileNames) {
      unCache(fileName);
    }
    delegate.sync(fileNames);
  }

  @Override
  public synchronized IndexInput openInput(String name, IOContext context) throws IOException {
    if (VERBOSE) {
      System.out.println("nrtdir.openInput name=" + name);
    }
    if (cache.fileExists(name)) {
      if (VERBOSE) {
        System.out.println("  from cache");
      }
      return cache.openInput(name, context);
    } else {
      return delegate.openInput(name, context);
    }
  }

  @Override
  public synchronized IndexInputSlicer createSlicer(final String name, final IOContext context) throws IOException {
    ensureOpen();
    if (VERBOSE) {
      System.out.println("nrtdir.openInput name=" + name);
    }
    if (cache.fileExists(name)) {
      if (VERBOSE) {
        System.out.println("  from cache");
      }
      return cache.createSlicer(name, context);
    } else {
      return delegate.createSlicer(name, context);
    }
  }
  
  /** Close this directory, which flushes any cached files
   *  to the delegate and then closes the delegate. */
  @Override
  public void close() throws IOException {
    // NOTE: technically we shouldn't have to do this, ie,
    // IndexWriter should have sync'd all files, but we do
    // it for defensive reasons... or in case the app is
    // doing something custom (creating outputs directly w/o
    // using IndexWriter):
    for(String fileName : cache.listAll()) {
      unCache(fileName);
    }
    cache.close();
    delegate.close();
  }

  /** Subclass can override this to customize logic; return
   *  true if this file should be written to the RAMDirectory. */
  protected boolean doCacheWrite(String name, IOContext context) {
    //System.out.println(Thread.currentThread().getName() + ": CACHE check merge=" + merge + " size=" + (merge==null ? 0 : merge.estimatedMergeBytes));

    long bytes = 0;
    if (context.mergeInfo != null) {
      bytes = context.mergeInfo.estimatedMergeBytes;
    } else if (context.flushInfo != null) {
      bytes = context.flushInfo.estimatedSegmentSize;
    }

    return !name.equals(IndexFileNames.SEGMENTS_GEN) && (bytes <= maxMergeSizeBytes) && (bytes + cache.sizeInBytes()) <= maxCachedBytes;
  }

  private final Object uncacheLock = new Object();

  private void unCache(String fileName) throws IOException {
    // Only let one thread uncache at a time; this only
    // happens during commit() or close():
    synchronized(uncacheLock) {
      if (VERBOSE) {
        System.out.println("nrtdir.unCache name=" + fileName);
      }
      if (!cache.fileExists(fileName)) {
        // Another thread beat us...
        return;
      }
      if (delegate.fileExists(fileName)) {
        throw new IOException("cannot uncache file=\"" + fileName + "\": it was separately also created in the delegate directory");
      }
      final IOContext context = IOContext.DEFAULT;
      final IndexOutput out = delegate.createOutput(fileName, context);
      IndexInput in = null;
      try {
        in = cache.openInput(fileName, context);
        out.copyBytes(in, in.length());
      } finally {
        IOUtils.close(in, out);
      }

      // Lock order: uncacheLock -> this
      synchronized(this) {
        // Must sync here because other sync methods have
        // if (cache.fileExists(name)) { ... } else { ... }:
        cache.deleteFile(fileName);
      }
    }
  }
}
