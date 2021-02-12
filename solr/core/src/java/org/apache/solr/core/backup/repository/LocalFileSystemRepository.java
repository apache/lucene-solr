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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.Objects;

import com.google.common.base.Preconditions;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;

/**
 * A concrete implementation of {@linkplain BackupRepository} interface supporting backup/restore of Solr indexes to a
 * local file-system. (Note - This can even be used for a shared file-system if it is exposed via a local file-system
 * interface e.g. NFS).
 */
public class LocalFileSystemRepository implements BackupRepository {

  @SuppressWarnings("rawtypes")
  private NamedList config = null;

  @Override
  public void init(@SuppressWarnings("rawtypes") NamedList args) {
    this.config = args;
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
        result = Paths.get(location).toUri();
      }
    } catch (URISyntaxException ex) {
      result = Paths.get(location).toUri();
    }

    return result;
  }

  @Override
  public URI resolve(URI baseUri, String... pathComponents) {
    Preconditions.checkArgument(pathComponents.length > 0);

    Path result = Paths.get(baseUri);
    for (int i = 0; i < pathComponents.length; i++) {
      try {
        result = result.resolve(pathComponents[i]);
      } catch (Exception e) {
        // unlikely to happen
        throw new RuntimeException(e);
      }

    }

    return result.toUri();
  }

  @Override
  public void createDirectory(URI path) throws IOException {
    Path p = Paths.get(path);
    if (!Files.exists(p, LinkOption.NOFOLLOW_LINKS)) {
      Files.createDirectory(p);
    }
  }

  @Override
  public void deleteDirectory(URI path) throws IOException {
    Files.walkFileTree(Paths.get(path), new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  @Override
  public boolean exists(URI path) throws IOException {
    return Files.exists(Paths.get(path));
  }

  @Override
  public IndexInput openInput(URI dirPath, String fileName, IOContext ctx) throws IOException {
    try (FSDirectory dir = new SimpleFSDirectory(Paths.get(dirPath), NoLockFactory.INSTANCE)) {
      return dir.openInput(fileName, ctx);
    }
  }

  @Override
  public OutputStream createOutput(URI path) throws IOException {
    return Files.newOutputStream(Paths.get(path));
  }

  @Override
  public String[] listAll(URI dirPath) throws IOException {
    // It is better to check the existence of the directory first since
    // creating a FSDirectory will create a corresponds folder if the directory does not exist
    if (!exists(dirPath)) {
      return new String[0];
    }

    try (FSDirectory dir = new NIOFSDirectory(Paths.get(dirPath), NoLockFactory.INSTANCE)) {
      return dir.listAll();
    }
  }

  @Override
  public PathType getPathType(URI path) throws IOException {
    return Files.isDirectory(Paths.get(path)) ? PathType.DIRECTORY : PathType.FILE;
  }

  @Override
  public void copyIndexFileFrom(Directory sourceDir, String sourceFileName, URI destDir, String destFileName) throws IOException {
    try (FSDirectory dir = new NIOFSDirectory(Paths.get(destDir), NoLockFactory.INSTANCE)) {
      copyIndexFileFrom(sourceDir, sourceFileName, dir, destFileName);
    }
  }

  @Override
  public void copyIndexFileTo(URI sourceDir, String sourceFileName, Directory dest, String destFileName) throws IOException {
    try (FSDirectory dir = new NIOFSDirectory(Paths.get(sourceDir), NoLockFactory.INSTANCE)) {
      dest.copyFrom(dir, sourceFileName, destFileName, DirectoryFactory.IOCONTEXT_NO_CACHE);
    }
  }

  @Override
  public void delete(URI path, Collection<String> files, boolean ignoreNoSuchFileException) throws IOException {
    if (files.isEmpty())
      return;

    try (FSDirectory dir = new NIOFSDirectory(Paths.get(path), NoLockFactory.INSTANCE)) {
      for (String file : files) {
        try {
          dir.deleteFile(file);
        } catch (NoSuchFileException e) {
          if (!ignoreNoSuchFileException)
            throw e;
        }
      }
    }
  }

  @Override
  public void close() throws IOException {}
}
