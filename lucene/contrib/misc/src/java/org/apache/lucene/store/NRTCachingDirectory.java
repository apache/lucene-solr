package org.apache.lucene.store;

/**
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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;       // javadocs
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.util.IOUtils;

// TODO
//   - let subclass dictate policy...?
//   - rename to MergeCacheingDir?  NRTCachingDir

/**
 * Wraps a {@link RAMDirectory}
 * around any provided delegate directory, to
 * be used during NRT search.  Make sure you pull the merge
 * scheduler using {@link #getMergeScheduler} and pass that to your
 * {@link IndexWriter}; this class uses that to keep track of which
 * merges are being done by which threads, to decide when to
 * cache each written file.
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
 * <p><b>NOTE</b>: this class is somewhat sneaky in its
 * approach for spying on merges to determine the size of a
 * merge: it records which threads are running which merges
 * by watching ConcurrentMergeScheduler's doMerge method.
 * While this works correctly, likely future versions of
 * this class will take a more general approach.
 *
 * <p>Here's a simple example usage:
 *
 * <pre>
 *   Directory fsDir = FSDirectory.open(new File("/path/to/index"));
 *   NRTCachingDirectory cachedFSDir = new NRTCachingDirectory(fsDir, 5.0, 60.0);
 *   IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_32, analyzer);
 *   conf.setMergeScheduler(cachedFSDir.getMergeScheduler());
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
   *  flush or a merge and the estimated size of the merged segmnt is <=
   *  maxMergeSizeMB, and 2) the total cached bytes is <=
   *  maxCachedMB */
  public NRTCachingDirectory(Directory delegate, double maxMergeSizeMB, double maxCachedMB) {
    this.delegate = delegate;
    maxMergeSizeBytes = (long) (maxMergeSizeMB*1024*1024);
    maxCachedBytes = (long) (maxCachedMB*1024*1024);
  }

  @Override
  public synchronized String[] listAll() throws IOException {
    final Set<String> files = new HashSet<String>();
    for(String f : cache.listAll()) {
      files.add(f);
    }
    for(String f : delegate.listAll()) {
      assert !files.contains(f);
      files.add(f);
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
  public synchronized long fileModified(String name) throws IOException {
    if (cache.fileExists(name)) {
      return cache.fileModified(name);
    } else {
      return delegate.fileModified(name);
    }
  }

  @Override
  public synchronized void touchFile(String name) throws IOException {
    if (cache.fileExists(name)) {
      cache.touchFile(name);
    } else {
      delegate.touchFile(name);
    }
  }

  @Override
  public synchronized void deleteFile(String name) throws IOException {
    // Delete from both, in case we are currently uncaching:
    if (VERBOSE) {
      System.out.println("nrtdir.deleteFile name=" + name);
    }
    cache.deleteFile(name);
    delegate.deleteFile(name);
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
  public IndexOutput createOutput(String name) throws IOException {
    if (VERBOSE) {
      System.out.println("nrtdir.createOutput name=" + name);
    }
    if (doCacheWrite(name)) {
      if (VERBOSE) {
        System.out.println("  to cache");
      }
      return cache.createOutput(name);
    } else {
      return delegate.createOutput(name);
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
  public synchronized IndexInput openInput(String name) throws IOException {
    if (VERBOSE) {
      System.out.println("nrtdir.openInput name=" + name);
    }
    if (cache.fileExists(name)) {
      if (VERBOSE) {
        System.out.println("  from cache");
      }
      return cache.openInput(name);
    } else {
      return delegate.openInput(name);
    }
  }

  @Override
  public synchronized IndexInput openInput(String name, int bufferSize) throws IOException {
    if (cache.fileExists(name)) {
      return cache.openInput(name, bufferSize);
    } else {
      return delegate.openInput(name, bufferSize);
    }
  }

  @Override
  public Lock makeLock(String name) {
    return delegate.makeLock(name);
  }

  @Override
  public void clearLock(String name) throws IOException {
    delegate.clearLock(name);
  }

  /** Close thius directory, which flushes any cached files
   *  to the delegate and then closes the delegate. */
  @Override
  public void close() throws IOException {
    for(String fileName : cache.listAll()) {
      unCache(fileName);
    }
    cache.close();
    delegate.close();
  }

  private final ConcurrentHashMap<Thread,MergePolicy.OneMerge> merges = new ConcurrentHashMap<Thread,MergePolicy.OneMerge>();

  public MergeScheduler getMergeScheduler() {
    return new ConcurrentMergeScheduler() {
      @Override
      protected void doMerge(MergePolicy.OneMerge merge) throws IOException {
        try {
          merges.put(Thread.currentThread(), merge);
          super.doMerge(merge);
        } finally {
          merges.remove(Thread.currentThread());
        }
      }
    };
  }

  /** Subclass can override this to customize logic; return
   *  true if this file should be written to the RAMDirectory. */
  protected boolean doCacheWrite(String name) {
    final MergePolicy.OneMerge merge = merges.get(Thread.currentThread());
    //System.out.println(Thread.currentThread().getName() + ": CACHE check merge=" + merge + " size=" + (merge==null ? 0 : merge.estimatedMergeBytes));
    return !name.equals(IndexFileNames.SEGMENTS_GEN) && (merge == null || merge.estimatedMergeBytes <= maxMergeSizeBytes) && cache.sizeInBytes() <= maxCachedBytes;
  }

  private void unCache(String fileName) throws IOException {
    final IndexOutput out;
    synchronized(this) {
      if (!delegate.fileExists(fileName)) {
        assert cache.fileExists(fileName);
        out = delegate.createOutput(fileName);
      } else {
        out = null;
      }
    }

    if (out != null) {
      IndexInput in = null;
      try {
        in = cache.openInput(fileName);
        in.copyBytes(out, in.length());
      } finally {
        IOUtils.closeSafely(in, out);
      }
      synchronized(this) {
        cache.deleteFile(fileName);
      }
    }
  }
}

