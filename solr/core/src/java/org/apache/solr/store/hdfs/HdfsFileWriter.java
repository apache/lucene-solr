package org.apache.solr.store.hdfs;

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
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.lucene.store.DataOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsFileWriter extends DataOutput {
  public static Logger LOG = LoggerFactory.getLogger(HdfsFileWriter.class);
  
  public static final String HDFS_SYNC_BLOCK = "solr.hdfs.sync.block";
  
  private final Path path;
  private FSDataOutputStream outputStream;
  private long currentPosition;
  
  public HdfsFileWriter(FileSystem fileSystem, Path path) throws IOException {
    LOG.debug("Creating writer on {}", path);
    this.path = path;
    
    Configuration conf = fileSystem.getConf();
    FsServerDefaults fsDefaults = fileSystem.getServerDefaults(path);
    EnumSet<CreateFlag> flags = EnumSet.of(CreateFlag.CREATE,
        CreateFlag.OVERWRITE);
    if (Boolean.getBoolean(HDFS_SYNC_BLOCK)) {
      flags.add(CreateFlag.SYNC_BLOCK);
    }
    outputStream = fileSystem.create(path, FsPermission.getDefault()
        .applyUMask(FsPermission.getUMask(conf)), flags, fsDefaults
        .getFileBufferSize(), fsDefaults.getReplication(), fsDefaults
        .getBlockSize(), null);
  }
  
  public long length() {
    return currentPosition;
  }
  
  public void seek(long pos) throws IOException {
    LOG.error("Invalid seek called on {}", path);
    throw new IOException("Seek not supported");
  }
  
  public void flush() throws IOException {
    // flush to the network, not guarantees it makes it to the DN (vs hflush)
    outputStream.flush();
    LOG.debug("Flushed file {}", path);
  }
  
  public void close() throws IOException {
    outputStream.close();
    LOG.debug("Closed writer on {}", path);
  }
  
  @Override
  public void writeByte(byte b) throws IOException {
    outputStream.write(b & 0xFF);
    currentPosition++;
  }
  
  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    outputStream.write(b, offset, length);
    currentPosition += length;
  }
  
  public long getPosition() {
    return currentPosition;
  }
}
