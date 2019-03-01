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
package org.apache.lucene.store;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;

// TODO
//   - let subclass dictate policy...?
//   - rename to MergeCacheingDir?  NRTCachingDir

/**
 * Wraps a RAM-resident directory around any provided delegate directory, to
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
 *   Directory fsDir = FSDirectory.open(new File("/path/to/index").toPath());
 *   NRTCachingDirectory cachedFSDir = new NRTCachingDirectory(fsDir, 5.0, 60.0);
 *   IndexWriterConfig conf = new IndexWriterConfig(analyzer);
 *   IndexWriter writer = new IndexWriter(cachedFSDir, conf);
 * </pre>
 *
 * <p>This will cache all newly flushed segments, all merges
 * whose expected segment size is {@code <= 5 MB}, unless the net
 * cached bytes exceeds 60 MB at which point all writes will
 * not be cached (until the net bytes falls below 60 MB).</p>
 *
 * @lucene.experimental
 */

public class NRTCachingDirectory extends FilterDirectory implements Accountable {
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Current total size of files in the cache is maintained separately for faster access.
   */
  private final AtomicLong cacheSize = new AtomicLong();

  /**
   * RAM-resident directory that updates {@link #cacheSize} when files are successfully closed.
   */
  private final ByteBuffersDirectory cacheDirectory = new ByteBuffersDirectory(
      new SingleInstanceLockFactory(),
      ByteBuffersDataOutput::new,
      (fileName, content) -> {
        cacheSize.addAndGet(content.size());
        return ByteBuffersDirectory.OUTPUT_AS_MANY_BUFFERS_LUCENE.apply(fileName, content);
      }
  );

  private final long maxMergeSizeBytes;
  private final long maxCachedBytes;

  private static final boolean VERBOSE = false;

  /**
   *  We will cache a newly created output if 1) it's a
   *  flush or a merge and the estimated size of the merged segment is 
   *  {@code <= maxMergeSizeMB}, and 2) the total cached bytes is 
   *  {@code <= maxCachedMB} */
  public NRTCachingDirectory(Directory delegate, double maxMergeSizeMB, double maxCachedMB) {
    super(delegate);
    maxMergeSizeBytes = (long) (maxMergeSizeMB * 1024 * 1024);
    maxCachedBytes = (long) (maxCachedMB * 1024 * 1024);
  }


  @Override
  public String toString() {
    return "NRTCachingDirectory(" + in + "; maxCacheMB=" + (maxCachedBytes/1024/1024.) + " maxMergeSizeMB=" + (maxMergeSizeBytes/1024/1024.) + ")";
  }

  @Override
  public synchronized String[] listAll() throws IOException {
    final Set<String> files = new HashSet<>();
    for (String f : cacheDirectory.listAll()) {
      files.add(f);
    }
    for (String f : in.listAll()) {
      files.add(f);
    }
    String[] result = files.toArray(new String[files.size()]);
    Arrays.sort(result);
    return result;
  }

  @Override
  public synchronized void deleteFile(String name) throws IOException {
    if (VERBOSE) {
      System.out.println("nrtdir.deleteFile name=" + name);
    }
    if (cacheDirectory.fileExists(name)) {
      cacheDirectory.deleteFile(name);
    } else {
      in.deleteFile(name);
    }
  }

  @Override
  public synchronized long fileLength(String name) throws IOException {
    if (cacheDirectory.fileExists(name)) {
      return cacheDirectory.fileLength(name);
    } else {
      return in.fileLength(name);
    }
  }

  public String[] listCachedFiles() {
    try {
      return cacheDirectory.listAll();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
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
      return cacheDirectory.createOutput(name, context);
    } else {
      return in.createOutput(name, context);
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
    in.sync(fileNames);
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    unCache(source);
    if (cacheDirectory.fileExists(dest)) {
      throw new IllegalArgumentException("target file " + dest + " already exists");
    }
    in.rename(source, dest);
  }

  @Override
  public synchronized IndexInput openInput(String name, IOContext context) throws IOException {
    if (VERBOSE) {
      System.out.println("nrtdir.openInput name=" + name);
    }
    if (cacheDirectory.fileExists(name)) {
      if (VERBOSE) {
        System.out.println("  from cache");
      }
      return cacheDirectory.openInput(name, context);
    } else {
      return in.openInput(name, context);
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
    IOUtils.close(
        () -> {
          if (!closed.getAndSet(true)) {
            for(String fileName : cacheDirectory.listAll()) {
              unCache(fileName);
            }
          }
        },
        cacheDirectory,
        in);
  }

  /** Subclass can override this to customize logic; return
   *  true if this file should be written to the RAM-based cache first. */
  protected boolean doCacheWrite(String name, IOContext context) {
    //System.out.println(Thread.currentThread().getName() + ": CACHE check merge=" + merge + " size=" + (merge==null ? 0 : merge.estimatedMergeBytes));

    long bytes = 0;
    if (context.mergeInfo != null) {
      bytes = context.mergeInfo.estimatedMergeBytes;
    } else if (context.flushInfo != null) {
      bytes = context.flushInfo.estimatedSegmentSize;
    }

    return (bytes <= maxMergeSizeBytes) && (bytes + cacheSize.get()) <= maxCachedBytes;
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
    if (VERBOSE) {
      System.out.println("nrtdir.createTempOutput prefix=" + prefix + " suffix=" + suffix);
    }
    Set<String> toDelete = new HashSet<>();

    // This is very ugly/messy/dangerous (can in some disastrous case maybe create too many temp files), but I don't know of a cleaner way:
    boolean success = false;

    Directory first;
    Directory second;
    if (doCacheWrite(prefix, context)) {
      first = cacheDirectory;
      second = in;
    } else {
      first = in;
      second = cacheDirectory;
    }

    IndexOutput out = null;
    try {
      while (true) {
        out = first.createTempOutput(prefix, suffix, context);
        String name = out.getName();
        toDelete.add(name);
        if (slowFileExists(second, name)) {
          out.close();
        } else {
          toDelete.remove(name);
          success = true;
          break;
        }
      }
    } finally {
      if (success) {
        IOUtils.deleteFiles(first, toDelete);
      } else {
        IOUtils.closeWhileHandlingException(out);
        IOUtils.deleteFilesIgnoringExceptions(first, toDelete);
      }
    }

    return out;
  }

  /** Returns true if the file exists
   *  (can be opened), false if it cannot be opened, and
   *  (unlike Java's File.exists) throws IOException if
   *  there's some unexpected error. */
  static boolean slowFileExists(Directory dir, String fileName) throws IOException {
    try {
      dir.fileLength(fileName);
      return true;
    } catch (NoSuchFileException | FileNotFoundException e) {
      return false;
    }
  }

  private void unCache(String fileName) throws IOException {
    // Must sync here because other sync methods have
    // if (cache.fileNameExists(name)) { ... } else { ... }:
    synchronized (this) {
      if (VERBOSE) {
        System.out.println("nrtdir.unCache name=" + fileName);
      }
      if (!cacheDirectory.fileExists(fileName)) {
        // Another thread beat us...
        return;
      }
      assert slowFileExists(in, fileName) == false: "fileName=" + fileName + " exists both in cache and in delegate";

      in.copyFrom(cacheDirectory, fileName, fileName, IOContext.DEFAULT);
      cacheSize.addAndGet(-cacheDirectory.fileLength(fileName));
      cacheDirectory.deleteFile(fileName);
    }
  }

  @Override
  public long ramBytesUsed() {
    return cacheSize.get();
  }
}
