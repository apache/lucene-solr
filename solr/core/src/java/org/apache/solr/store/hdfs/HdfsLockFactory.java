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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockReleaseFailedException;
import org.apache.solr.util.IOUtils;

public class HdfsLockFactory extends LockFactory {
  
  private Path lockPath;
  private Configuration configuration;
  
  public HdfsLockFactory(Path lockPath, Configuration configuration) {
    this.lockPath = lockPath;
    this.configuration = configuration;
  }
  
  @Override
  public Lock makeLock(String lockName) {
    
    if (lockPrefix != null) {
      lockName = lockPrefix + "-" + lockName;
    }
    
    HdfsLock lock = new HdfsLock(lockPath, lockName, configuration);
    
    return lock;
  }
  
  @Override
  public void clearLock(String lockName) throws IOException {
    FileSystem fs = null;
    try {
      fs = FileSystem.newInstance(lockPath.toUri(), configuration);
      
      if (fs.exists(lockPath)) {
        if (lockPrefix != null) {
          lockName = lockPrefix + "-" + lockName;
        }
        
        Path lockFile = new Path(lockPath, lockName);

        if (fs.exists(lockFile) && !fs.delete(lockFile, false)) {
          throw new IOException("Cannot delete " + lockFile);
        }
      }
    } finally {
      IOUtils.closeQuietly(fs);
    }
  }
  
  public Path getLockPath() {
    return lockPath;
  }
  
  public void setLockPath(Path lockPath) {
    this.lockPath = lockPath;
  }
  
  static class HdfsLock extends Lock {
    
    private Path lockPath;
    private String lockName;
    private Configuration conf;
    
    public HdfsLock(Path lockPath, String lockName, Configuration conf) {
      this.lockPath = lockPath;
      this.lockName = lockName;
      this.conf = conf;
    }
    
    @Override
    public boolean obtain() throws IOException {
      FSDataOutputStream file = null;
      FileSystem fs = null;
      try {
        fs = FileSystem.newInstance(lockPath.toUri(), conf);
        
        file = fs.create(new Path(lockPath, lockName), false);
      } catch (IOException e) {
        return false;
      } finally {
        IOUtils.closeQuietly(file);
        IOUtils.closeQuietly(fs);
      }
      return true;
    }
    
    @Override
    public void release() throws IOException {
      FileSystem fs = FileSystem.newInstance(lockPath.toUri(), conf);
      try {
        if (fs.exists(new Path(lockPath, lockName))
            && !fs.delete(new Path(lockPath, lockName), false)) throw new LockReleaseFailedException(
            "failed to delete " + new Path(lockPath, lockName));
      } finally {
        IOUtils.closeQuietly(fs);
      }
    }
    
    @Override
    public boolean isLocked() throws IOException {
      boolean isLocked = false;
      FileSystem fs = FileSystem.newInstance(lockPath.toUri(), conf);
      try {
        isLocked = fs.exists(new Path(lockPath, lockName));
      } finally {
        IOUtils.closeQuietly(fs);
      }
      return isLocked;
    }
    
  }
  
}
