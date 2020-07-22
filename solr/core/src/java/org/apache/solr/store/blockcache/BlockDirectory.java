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
package org.apache.solr.store.blockcache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Set;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.solr.core.ShutdownAwareDirectory;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @lucene.experimental
 */
public class BlockDirectory extends FilterDirectory implements ShutdownAwareDirectory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static final long BLOCK_SHIFT = Integer.getInteger("solr.hdfs.blockcache.blockshift", 13);

  public static final int BLOCK_SIZE = 1 << BLOCK_SHIFT;
  public static final long BLOCK_MOD = BLOCK_SIZE - 1;
  
  public static long getBlock(long pos) {
    return pos >>> BLOCK_SHIFT;
  }
  
  public static long getPosition(long pos) {
    return pos & BLOCK_MOD;
  }
  
  public static long getRealPosition(long block, long positionInBlock) {
    return (block << BLOCK_SHIFT) + positionInBlock;
  }
  
  public static Cache NO_CACHE = new Cache() {
    
    @Override
    public void update(String name, long blockId, int blockOffset,
        byte[] buffer, int offset, int length) {}
    
    @Override
    public boolean fetch(String name, long blockId, int blockOffset, byte[] b,
        int off, int lengthToReadInBlock) {
      return false;
    }
    
    @Override
    public void delete(String name) {
      
    }
    
    @Override
    public long size() {
      return 0;
    }
    
    @Override
    public void renameCacheFile(String source, String dest) {}

    @Override
    public void releaseResources() {}
  };
  
  private final int blockSize;
  private final String dirName;
  private final Cache cache;
  private final Set<String> blockCacheFileTypes;
  private final boolean blockCacheReadEnabled;
  private final boolean blockCacheWriteEnabled;

  private boolean cacheMerges;
  private boolean cacheReadOnce;

  public BlockDirectory(String dirName, Directory directory, Cache cache,
      Set<String> blockCacheFileTypes, boolean blockCacheReadEnabled,
      boolean blockCacheWriteEnabled) throws IOException {
    this(dirName, directory, cache, blockCacheFileTypes, blockCacheReadEnabled, blockCacheWriteEnabled, true, true);
  }
  
  public BlockDirectory(String dirName, Directory directory, Cache cache,
      Set<String> blockCacheFileTypes, boolean blockCacheReadEnabled,
      boolean blockCacheWriteEnabled, boolean cacheMerges, boolean cacheReadOnce) throws IOException {
    super(directory);
    this.cacheMerges = cacheMerges;
    this.cacheReadOnce = cacheReadOnce;
    this.dirName = dirName;
    blockSize = BLOCK_SIZE;
    this.cache = cache;
    if (blockCacheFileTypes == null || blockCacheFileTypes.isEmpty()) {
      this.blockCacheFileTypes = null;
    } else {
      this.blockCacheFileTypes = blockCacheFileTypes;
    }
    this.blockCacheReadEnabled = blockCacheReadEnabled;
    if (!blockCacheReadEnabled) {
      log.info("Block cache on read is disabled");
    }
    this.blockCacheWriteEnabled = blockCacheWriteEnabled;
    if (!blockCacheWriteEnabled) {
      log.info("Block cache on write is disabled");
    }
  }
  
  private IndexInput openInput(String name, int bufferSize, IOContext context)
      throws IOException {
    final IndexInput source = super.openInput(name, context);
    if (useReadCache(name, context)) {
      return new CachedIndexInput(source, blockSize, name,
          getFileCacheName(name), cache, bufferSize);
    }
    return source;
  }
  
  private boolean isCachableFile(String name) {
    for (String ext : blockCacheFileTypes) {
      if (name.endsWith(ext)) {
        return true;
      }
    }
    return false;
  }
  
  @Override
  public IndexInput openInput(final String name, IOContext context)
      throws IOException {
    return openInput(name, blockSize, context);
  }
  
  static class CachedIndexInput extends CustomBufferedIndexInput {
    private final Store store;
    private IndexInput source;
    private final int blockSize;
    private final long fileLength;
    private final String cacheName;
    private final Cache cache;
    
    public CachedIndexInput(IndexInput source, int blockSize, String name,
        String cacheName, Cache cache, int bufferSize) {
      super(name, bufferSize);
      this.source = source;
      this.blockSize = blockSize;
      fileLength = source.length();
      this.cacheName = cacheName;
      this.cache = cache;
      store = BufferStore.instance(blockSize);
    }
    
    @Override
    public IndexInput clone() {
      CachedIndexInput clone = (CachedIndexInput) super.clone();
      clone.source = source.clone();
      return clone;
    }
    
    @Override
    public long length() {
      return source.length();
    }
    
    @Override
    protected void seekInternal(long pos) throws IOException {}
    
    @Override
    protected void readInternal(byte[] b, int off, int len) throws IOException {
      long position = getFilePointer();
      while (len > 0) {
        int length = fetchBlock(position, b, off, len);
        position += length;
        len -= length;
        off += length;
      }
    }
    
    private int fetchBlock(long position, byte[] b, int off, int len)
        throws IOException {
      // read whole block into cache and then provide needed data
      long blockId = getBlock(position);
      int blockOffset = (int) getPosition(position);
      int lengthToReadInBlock = Math.min(len, blockSize - blockOffset);
      if (checkCache(blockId, blockOffset, b, off, lengthToReadInBlock)) {
        return lengthToReadInBlock;
      } else {
        readIntoCacheAndResult(blockId, blockOffset, b, off,
            lengthToReadInBlock);
      }
      return lengthToReadInBlock;
    }
    
    private void readIntoCacheAndResult(long blockId, int blockOffset,
        byte[] b, int off, int lengthToReadInBlock) throws IOException {
      long position = getRealPosition(blockId, 0);
      int length = (int) Math.min(blockSize, fileLength - position);
      source.seek(position);
      
      byte[] buf = store.takeBuffer(blockSize);
      source.readBytes(buf, 0, length);
      System.arraycopy(buf, blockOffset, b, off, lengthToReadInBlock);
      cache.update(cacheName, blockId, 0, buf, 0, blockSize);
      store.putBuffer(buf);
    }
    
    private boolean checkCache(long blockId, int blockOffset, byte[] b,
        int off, int lengthToReadInBlock) {
      return cache.fetch(cacheName, blockId, blockOffset, b, off,
          lengthToReadInBlock);
    }
    
    @Override
    protected void closeInternal() throws IOException {
      source.close();
    }
  }
  
  @Override
  public void closeOnShutdown() throws IOException {
    log.info("BlockDirectory closing on shutdown");
    // we are shutting down, no need to clean up cache
    super.close();
  }
  
  @Override
  public void close() throws IOException {
    try {
      String[] files = listAll();
      
      for (String file : files) {
        cache.delete(getFileCacheName(file));
      }
      
    } catch (FileNotFoundException e) {
      // the local file system folder may be gone
    } finally {
      super.close();
      cache.releaseResources();
    }
  }
  
  String getFileCacheName(String name) throws IOException {
    return getFileCacheLocation(name) + ":" + getFileModified(name);
  }
  
  private long getFileModified(String name) throws IOException {
    if (in instanceof FSDirectory) {
      File directory = ((FSDirectory) in).getDirectory().toFile();
      File file = new File(directory, name);
      if (!file.exists()) {
        throw new FileNotFoundException("File [" + name + "] not found");
      }
      return file.lastModified();
    } else if (in instanceof HdfsDirectory) {
      return ((HdfsDirectory) in).fileModified(name);
    } else {
      throw new UnsupportedOperationException();
    }
  }
  
  String getFileCacheLocation(String name) {
    return dirName + "/" + name;
  }
  
  /**
   * Expert: mostly for tests
   * 
   * @lucene.experimental
   */
  public Cache getCache() {
    return cache;
  }
  
  /**
   * Determine whether read caching should be used for a particular
   * file/context.
   */
  boolean useReadCache(String name, IOContext context) {
    if (!blockCacheReadEnabled) {
      return false;
    }
    if (blockCacheFileTypes != null && !isCachableFile(name)) {
      return false;
    }
    switch (context.context) {
      // depending on params, we don't cache on merges or when only reading once
      case MERGE: {
        return cacheMerges;
      }
      case READ: {
        if (context.readOnce) {
          return cacheReadOnce;
        } else {
          return true;
        }
      }
      default: {
        return true;
      }
    }
  }
  
  /**
   * Determine whether write caching should be used for a particular
   * file/context.
   */
  boolean useWriteCache(String name, IOContext context) {
    if (!blockCacheWriteEnabled || name.startsWith(IndexFileNames.PENDING_SEGMENTS)) {
      // for safety, don't bother caching pending commits.
      // the cache does support renaming (renameCacheFile), but thats a scary optimization.
      return false;
    }
    if (blockCacheFileTypes != null && !isCachableFile(name)) {
      return false;
    }
    switch (context.context) {
      case MERGE: {
        // we currently don't cache any merge context writes
        return false;
      }
      default: {
        return true;
      }
    }
  }
  
  @Override
  public IndexOutput createOutput(String name, IOContext context)
      throws IOException {
    final IndexOutput dest = super.createOutput(name, context);
    if (useWriteCache(name, context)) {
      return new CachedIndexOutput(this, dest, blockSize, name, cache, blockSize);
    }
    return dest;
  }

  public void deleteFile(String name) throws IOException {
    cache.delete(getFileCacheName(name));
    super.deleteFile(name);
  }
    
  public boolean isBlockCacheReadEnabled() {
    return blockCacheReadEnabled;
  }

  public boolean isBlockCacheWriteEnabled() {
    return blockCacheWriteEnabled;
  }
  
}
