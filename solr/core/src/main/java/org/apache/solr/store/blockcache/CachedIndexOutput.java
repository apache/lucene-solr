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

import java.io.IOException;

import org.apache.lucene.store.IndexOutput;

/**
 * Cache the blocks as they are written. The cache file name is the name of
 * the file until the file is closed, at which point the cache is updated
 * to include the last modified date (which is unknown until that point).
 * @lucene.experimental
 */
public class CachedIndexOutput extends ReusedBufferedIndexOutput {
  private final BlockDirectory directory;
  private final IndexOutput dest;
  private final int blockSize;
  private final String name;
  private final String location;
  private final Cache cache;
  
  public CachedIndexOutput(BlockDirectory directory, IndexOutput dest,
      int blockSize, String name, Cache cache, int bufferSize) {
    super("dest=" + dest + " name=" + name, name, bufferSize);
    this.directory = directory;
    this.dest = dest;
    this.blockSize = blockSize;
    this.name = name;
    this.location = directory.getFileCacheLocation(name);
    this.cache = cache;
  }

  @Override
  public void closeInternal() throws IOException {
    dest.close();
    cache.renameCacheFile(location, directory.getFileCacheName(name));
  }
  
  private int writeBlock(long position, byte[] b, int offset, int length)
      throws IOException {
    // read whole block into cache and then provide needed data
    long blockId = BlockDirectory.getBlock(position);
    int blockOffset = (int) BlockDirectory.getPosition(position);
    int lengthToWriteInBlock = Math.min(length, blockSize - blockOffset);
    
    // write the file and copy into the cache
    dest.writeBytes(b, offset, lengthToWriteInBlock);
    cache.update(location, blockId, blockOffset, b, offset,
        lengthToWriteInBlock);
    
    return lengthToWriteInBlock;
  }
  
  @Override
  public void writeInternal(byte[] b, int offset, int length)
      throws IOException {
    long position = getBufferStart();
    while (length > 0) {
      int len = writeBlock(position, b, offset, length);
      position += len;
      length -= len;
      offset += len;
    }
  }

  @Override
  public long getChecksum() throws IOException {
    flushBufferToCache();
    return dest.getChecksum();
  }
}
