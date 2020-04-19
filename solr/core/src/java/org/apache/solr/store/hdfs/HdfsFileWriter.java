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
package org.apache.solr.store.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.lucene.store.OutputStreamIndexOutput;

/**
 * @lucene.experimental
 */
public class HdfsFileWriter extends OutputStreamIndexOutput {
  
  public static final String HDFS_SYNC_BLOCK = "solr.hdfs.sync.block";
  public static final int BUFFER_SIZE = 16384;
  
  public HdfsFileWriter(FileSystem fileSystem, Path path, String name) throws IOException {
    super("fileSystem=" + fileSystem + " path=" + path, name, getOutputStream(fileSystem, path), BUFFER_SIZE);
  }
  
  private static final OutputStream getOutputStream(FileSystem fileSystem, Path path) throws IOException {
    Configuration conf = fileSystem.getConf();
    FsServerDefaults fsDefaults = fileSystem.getServerDefaults(path);
    short replication = fileSystem.getDefaultReplication(path);
    EnumSet<CreateFlag> flags = EnumSet.of(CreateFlag.CREATE);
    if (Boolean.getBoolean(HDFS_SYNC_BLOCK)) {
      flags.add(CreateFlag.SYNC_BLOCK);
    }
    return fileSystem.create(path, FsPermission.getDefault()
        .applyUMask(FsPermission.getUMask(conf)), flags, fsDefaults
        .getFileBufferSize(), replication, fsDefaults
        .getBlockSize(), null);
  }
}
