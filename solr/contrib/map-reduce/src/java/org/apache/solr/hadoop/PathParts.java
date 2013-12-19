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
package org.apache.solr.hadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

/**
 * Extracts various components of an HDFS Path
 */
public final class PathParts {

  private final String uploadURL;
  private final Configuration conf;
  private final FileSystem fs;
  private final Path normalizedPath;
  private FileStatus stats;

  public PathParts(String uploadURL, Configuration conf) throws IOException {
    if (uploadURL == null) {
      throw new IllegalArgumentException("Path must not be null: " + uploadURL);    
    }
    this.uploadURL = uploadURL;
    if (conf == null) {
      throw new IllegalArgumentException("Configuration must not be null: " + uploadURL);    
    }
    this.conf = conf;
    URI uri = stringToUri(uploadURL);
    this.fs = FileSystem.get(uri, conf);
    if (fs == null) {
      throw new IllegalArgumentException("File system must not be null: " + uploadURL);    
    }
    this.normalizedPath = fs.makeQualified(new Path(uri));
    if (!normalizedPath.isAbsolute()) {
      throw new IllegalArgumentException("Path must be absolute: " + uploadURL);    
    }
    if (getScheme() == null) {
      throw new IllegalArgumentException("Scheme must not be null: " + uploadURL);    
    }
    if (getHost() == null) {
      throw new IllegalArgumentException("Host must not be null: " + uploadURL);    
    }
    if (getPort() < 0) {
      throw new IllegalArgumentException("Port must not be negative: " + uploadURL);    
    }
  }
  
  public String getUploadURL() {
    return uploadURL;
  }

  public Path getUploadPath() {
    return new Path(getUploadURL());
  }
  
  public String getURIPath() {
    return normalizedPath.toUri().getPath();
  }

  public String getName() {
    return normalizedPath.getName();
  }

  public String getScheme() {
    return normalizedPath.toUri().getScheme();
  }

  public String getHost() {
    return normalizedPath.toUri().getHost();
  }

  public int getPort() {
    int port = normalizedPath.toUri().getPort();
    if (port == -1) {
      port = fs.getWorkingDirectory().toUri().getPort();
      if (port == -1) {
        port = NameNode.DEFAULT_PORT;
      }
    }
    return port;
  }

  public String getId() {
    return getScheme() + "://" + getHost() + ":" + getPort() + getURIPath();
  }
  
  public String getDownloadURL() {
    return getId();
  }

  public Configuration getConfiguration() {
    return conf;
  }

  public FileSystem getFileSystem() {
    return fs;
  }

  public FileStatus getFileStatus() throws IOException {
    if (stats == null) {
      stats = getFileSystem().getFileStatus(getUploadPath());
    }
    return stats;
  }
  
  private URI stringToUri(String pathString) {
    //return new Path(pathString).toUri().normalize();
    return URI.create(pathString).normalize();
  }
}
