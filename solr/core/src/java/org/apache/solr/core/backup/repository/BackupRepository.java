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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collection;
import java.util.Optional;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.backup.Checksum;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * This interface defines the functionality required to backup/restore Solr indexes to an arbitrary storage system.
 */
public interface BackupRepository extends NamedListInitializedPlugin, Closeable {

  /**
   * This enumeration defines the type of a given path.
   */
  enum PathType {
    DIRECTORY, FILE
  }

  /**
   * This method returns the location where the backup should be stored (or restored from).
   *
   * @param override The location parameter supplied by the user.
   * @return If <code>override</code> is not null then return the same value
   *         Otherwise return the default configuration value for the {@linkplain CoreAdminParams#BACKUP_LOCATION} parameter.
   */
  default String getBackupLocation(String override) {
    return Optional.ofNullable(override).orElse(getConfigProperty(CoreAdminParams.BACKUP_LOCATION));
  }

  /**
   * This method returns the value of the specified configuration property.
   */
  <T> T getConfigProperty(String name);

  /**
   * This method returns the URI representation for the specified path.
   * Note - the specified path could be a fully qualified URI OR a relative path for a file-system.
   *
   * @param path The path specified by the user.
   * @return the URI representation of the user supplied value
   */
  URI createURI(String path);

  /**
   * This method returns the directory URI representation for the specified path.
   * Note - the specified path could be a fully qualified URI OR a relative path for a file-system.
   *
   * @param path The path specified by the user.
   * @return the URI representation of the user supplied value, ensured to look like a directory.
   */
  default URI createDirectoryURI(String path) {
    return createURI(path);
  }

  /**
   * This method resolves a URI using the specified path components (as method arguments).
   *
   * @param baseUri The base URI to use for creating the path
   * @param pathComponents
   *          The directory (or file-name) to be included in the URI.
   * @return A URI containing absolute path
   */
  URI resolve(URI baseUri, String... pathComponents);

  /**
   * This method resolves a directory URI using the specified path components (as method arguments).
   *
   * @param baseUri The base URI to use for creating the path
   * @param pathComponents
   *          The directory (or file-name) to be included in the URI.
   * @return A URI containing absolute path, ensured to look like a directory
   */
  default URI resolveDirectory(URI baseUri, String... pathComponents) {
    return resolve(baseUri, pathComponents);
  }

  /**
   * This method checks if the specified path exists in this repository.
   *
   * @param path
   *          The path whose existence needs to be checked.
   * @return if the specified path exists in this repository.
   * @throws IOException
   *           in case of errors
   */
  boolean exists(URI path) throws IOException;

  /**
   * This method returns the type of a specified path
   *
   * @param path
   *          The path whose type needs to be checked.
   * @return the {@linkplain PathType} for the specified path
   * @throws IOException
   *           in case of errors
   */
  PathType getPathType(URI path) throws IOException;

  /**
   * This method returns all the entries (files and directories) in the specified directory.
   *
   * @param path
   *          The directory path
   * @return an array of strings, one for each entry in the directory
   * @throws IOException
   *           in case of errors
   */
  String[] listAll(URI path) throws IOException;

  /**
   * This method returns a Lucene input stream reading an existing file.
   *
   * @param dirPath
   *          The parent directory of the file to be read
   * @param fileName
   *          The name of the file to be read
   * @param ctx
   *          the Lucene IO context
   * @return Lucene {@linkplain IndexInput} reference
   * @throws IOException
   *           in case of errors
   */
  IndexInput openInput(URI dirPath, String fileName, IOContext ctx) throws IOException;

  /**
   * This method returns a {@linkplain OutputStream} instance for the specified <code>path</code>
   *
   * @param path
   *          The path for which {@linkplain OutputStream} needs to be created
   * @return {@linkplain OutputStream} instance for the specified <code>path</code>
   * @throws IOException
   *           in case of errors
   */
  OutputStream createOutput(URI path) throws IOException;

  // TODO define whether this should also create any nonexistent parent directories. (i.e. is this 'mkdir', or 'mkdir -p')
  /**
   * This method creates a directory at the specified path.
   * If the directory already exist, this will be a no-op.
   *
   * @param path
   *          The path where the directory needs to be created.
   * @throws IOException
   *           in case of errors
   */
  void createDirectory(URI path) throws IOException;

  /**
   * This method deletes a directory at the specified path.
   *
   * @param path
   *          The path referring to the directory to be deleted.
   * @throws IOException
   *           in case of errors
   */
  void deleteDirectory(URI path) throws IOException;

  /**
   * Copy a file from specified <code>sourceDir</code> to the destination repository (i.e. backup).
   *
   * @param sourceDir
   *          The source directory hosting the file to be copied.
   * @param fileName
   *          The name of the file to by copied
   * @param dest
   *          The destination backup location.
   * @throws IOException
   *           in case of errors
   */
  default void copyFileFrom(Directory sourceDir, String fileName, URI dest) throws IOException {
    copyIndexFileFrom(sourceDir, fileName, dest, fileName);
  }

  /**
   * Copy a file from specified <code>sourceRepo</code> to the destination directory (i.e. restore).
   *
   * @param sourceRepo
   *          The source URI hosting the file to be copied.
   * @param fileName
   *          The name of the file to by copied
   * @param dest
   *          The destination where the file should be copied.
   * @throws IOException
   *           in case of errors.
   */
  default void copyFileTo(URI sourceRepo, String fileName, Directory dest) throws IOException {
    copyIndexFileTo(sourceRepo, fileName, dest, fileName);
  }

  /**
   * List all files or directories directly under {@code path}.
   * @return an empty array in case of IOException
   */
  default String[] listAllOrEmpty(URI path) {
    try {
      return this.listAll(path);
    } catch (IOException e) {
      return new String[0];
    }
  }

  default void copyIndexFileFrom(Directory sourceDir, String sourceFileName, Directory destDir, String destFileName) throws IOException {
    boolean success = false;
    try (ChecksumIndexInput is = sourceDir.openChecksumInput(sourceFileName, DirectoryFactory.IOCONTEXT_NO_CACHE);
         IndexOutput os = destDir.createOutput(destFileName, DirectoryFactory.IOCONTEXT_NO_CACHE)) {
      os.copyBytes(is, is.length() - CodecUtil.footerLength());

      // ensure that index file is not corrupted
      CodecUtil.checkFooter(is);
      CodecUtil.writeFooter(os);
      success = true;
    } finally {
      if (!success) {
        IOUtils.deleteFilesIgnoringExceptions(destDir, destFileName);
      }
    }
  }

  /**
   * Delete {@code files} at {@code path}
   * @since 8.3.0
   */
  default void delete(URI path, Collection<String> files, boolean ignoreNoSuchFileException) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Get checksum of {@code fileName} at {@code dir}.
   * This method only be called on Lucene index files
   * @since 8.3.0
   */
  default Checksum checksum(Directory dir, String fileName) throws IOException {
    try (IndexInput in = dir.openChecksumInput(fileName, IOContext.READONCE)) {
      return new Checksum(CodecUtil.retrieveChecksum(in), in.length());
    }
  }

  /**
   * Copy an index file from specified <code>sourceDir</code> to the destination repository (i.e. backup).
   *
   * @param sourceDir
   *          The source directory hosting the file to be copied.
   * @param sourceFileName
   *          The name of the file to by copied
   * @param destDir
   *          The destination backup location.
   * @throws IOException
   *          in case of errors
   * @throws CorruptIndexException
   *          in case checksum of the file does not match with precomputed checksum stored at the end of the file
   * @since 8.3.0
   */
  default void copyIndexFileFrom(Directory sourceDir, String sourceFileName, URI destDir, String destFileName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Copy an index file from specified <code>sourceRepo</code> to the destination directory (i.e. restore).
   *
   * @param sourceRepo
   *          The source URI hosting the file to be copied.
   * @param dest
   *          The destination where the file should be copied.
   * @throws IOException
   *           in case of errors.
   * @since 8.3.0
   */
  default void copyIndexFileTo(URI sourceRepo, String sourceFileName, Directory dest, String destFileName) throws IOException {
    throw new UnsupportedOperationException();
  }
}
