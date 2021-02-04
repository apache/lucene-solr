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

package org.apache.solr.core.backup.repository;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.Objects;
import java.lang.invoke.MethodHandles;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NoLockFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.HdfsDirectoryFactory;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.apache.solr.store.hdfs.HdfsDirectory.HdfsIndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated since 8.6; see <a href="https://cwiki.apache.org/confluence/display/SOLR/Deprecations">Deprecations</a>
 */
public class HdfsBackupRepository implements BackupRepository {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String HDFS_UMASK_MODE_PARAM = "solr.hdfs.permissions.umask-mode";
  private static final String HDFS_COPY_BUFFER_SIZE_PARAM = "solr.hdfs.buffer.size";

  private HdfsDirectoryFactory factory;
  private Configuration hdfsConfig = null;
  private FileSystem fileSystem = null;
  private Path baseHdfsPath = null;
  @SuppressWarnings("rawtypes")
  private NamedList config = null;
  protected int copyBufferSize = HdfsDirectory.DEFAULT_BUFFER_SIZE;

  @SuppressWarnings("rawtypes")
  @Override
  public void init(NamedList args) {
    this.config = args;

    log.warn("HDFS support in Solr has been deprecated as of 8.6. See SOLR-14021 for details.");

    // Configure the size of the buffer used for copying index files to/from HDFS, if specified.
    if (args.get(HDFS_COPY_BUFFER_SIZE_PARAM) != null) {
      this.copyBufferSize = (Integer)args.get(HDFS_COPY_BUFFER_SIZE_PARAM);
      if (this.copyBufferSize <= 0) {
        throw new IllegalArgumentException("Value of " + HDFS_COPY_BUFFER_SIZE_PARAM + " must be > 0");
      }
    }

    String hdfsSolrHome = (String) Objects.requireNonNull(args.get(HdfsDirectoryFactory.HDFS_HOME),
        "Please specify " + HdfsDirectoryFactory.HDFS_HOME + " property.");
    Path path = new Path(hdfsSolrHome);
    while (path != null) { // Compute the path of root file-system (without requiring an additional system property).
      baseHdfsPath = path;
      path = path.getParent();
    }

    // We don't really need this factory instance. But we want to initialize it here to
    // make sure that all HDFS related initialization is at one place (and not duplicated here).
    factory = new HdfsDirectoryFactory();
    factory.init(args);
    this.hdfsConfig = factory.getConf(new Path(hdfsSolrHome));

    // Configure the umask mode if specified.
    if (args.get(HDFS_UMASK_MODE_PARAM) != null) {
      String umaskVal = (String)args.get(HDFS_UMASK_MODE_PARAM);
      this.hdfsConfig.set(FsPermission.UMASK_LABEL, umaskVal);
    }

    try {
      this.fileSystem = FileSystem.get(this.baseHdfsPath.toUri(), this.hdfsConfig);
    } catch (IOException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

  public void close() throws IOException {
    if (this.fileSystem != null) {
      this.fileSystem.close();
    }
    if (this.factory != null) {
      this.factory.close();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getConfigProperty(String name) {
    return (T) this.config.get(name);
  }

  @Override
  public URI createURI(String location) {
    Objects.requireNonNull(location);

    URI result = null;
    try {
      result = new URI(location);
      if (!result.isAbsolute()) {
        result = resolve(this.baseHdfsPath.toUri(), location);
      }
    } catch (URISyntaxException ex) {
      result = resolve(this.baseHdfsPath.toUri(), location);
    }

    return result;
  }

  @Override
  public URI resolve(URI baseUri, String... pathComponents) {
    Preconditions.checkArgument(baseUri.isAbsolute());

    Path result = new Path(baseUri);
    for (String path : pathComponents) {
      result = new Path(result, path);
    }

    return result.toUri();
  }

  @Override
  public boolean exists(URI path) throws IOException {
    return this.fileSystem.exists(new Path(path));
  }

  @Override
  public PathType getPathType(URI path) throws IOException {
    return this.fileSystem.isDirectory(new Path(path)) ? PathType.DIRECTORY : PathType.FILE;
  }

  @Override
  public String[] listAll(URI path) throws IOException {
    FileStatus[] status = this.fileSystem.listStatus(new Path(path));
    String[] result = new String[status.length];
    for (int i = 0; i < status.length; i++) {
      result[i] = status[i].getPath().getName();
    }
    return result;
  }

  @Override
  public IndexInput openInput(URI dirPath, String fileName, IOContext ctx) throws IOException {
    Path p = new Path(new Path(dirPath), fileName);
    return new HdfsIndexInput(fileName, this.fileSystem, p, HdfsDirectory.DEFAULT_BUFFER_SIZE);
  }

  @Override
  public OutputStream createOutput(URI path) throws IOException {
    return this.fileSystem.create(new Path(path));
  }

  @Override
  public void createDirectory(URI path) throws IOException {
    if (!this.fileSystem.mkdirs(new Path(path))) {
      throw new IOException("Unable to create a directory at following location " + path);
    }
  }

  @Override
  public void deleteDirectory(URI path) throws IOException {
    if (!this.fileSystem.delete(new Path(path), true)) {
      throw new IOException("Unable to delete a directory at following location " + path);
    }
  }

  @Override
  public void copyIndexFileFrom(Directory sourceDir, String sourceFileName, URI destDir, String destFileName) throws IOException {
    try (HdfsDirectory dir = new HdfsDirectory(new Path(destDir), NoLockFactory.INSTANCE,
            hdfsConfig, copyBufferSize)) {
      copyIndexFileFrom(sourceDir, sourceFileName, dir, destFileName);
    }
  }

  @Override
  public void copyIndexFileTo(URI sourceRepo, String sourceFileName, Directory dest, String destFileName) throws IOException {
    try (HdfsDirectory dir = new HdfsDirectory(new Path(sourceRepo), NoLockFactory.INSTANCE,
            hdfsConfig, copyBufferSize)) {
      dest.copyFrom(dir, sourceFileName, destFileName, DirectoryFactory.IOCONTEXT_NO_CACHE);
    }
  }

  @Override
  public void delete(URI path, Collection<String> files, boolean ignoreNoSuchFileException) throws IOException {
    if (files.isEmpty())
      return;

    for (String file : files) {
      Path filePath = new Path(new Path(path), file);
      boolean success = fileSystem.delete(filePath, false);
      if (!ignoreNoSuchFileException && !success) {
        throw new NoSuchFileException(filePath.toString());
      }
    }
  }

}
