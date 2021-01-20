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
package org.apache.lucene.util;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import org.apache.lucene.mockfile.FilterFileSystemProvider;
import org.apache.lucene.mockfile.FilterPath;

/** Simple test methods for IOUtils */
public class TestIOUtils extends LuceneTestCase {

  public void testDeleteFileIgnoringExceptions() throws Exception {
    Path dir = createTempDir();
    Path file1 = dir.resolve("file1");
    Files.createFile(file1);
    IOUtils.deleteFilesIgnoringExceptions(file1);
    assertFalse(Files.exists(file1));
    // actually deletes
  }

  public void testDontDeleteFileIgnoringExceptions() throws Exception {
    Path dir = createTempDir();
    Path file1 = dir.resolve("file1");
    IOUtils.deleteFilesIgnoringExceptions(file1);
    // no exception
  }

  public void testDeleteTwoFilesIgnoringExceptions() throws Exception {
    Path dir = createTempDir();
    Path file1 = dir.resolve("file1");
    Path file2 = dir.resolve("file2");
    // only create file2
    Files.createFile(file2);
    IOUtils.deleteFilesIgnoringExceptions(file1, file2);
    assertFalse(Files.exists(file2));
    // no exception
    // actually deletes file2
  }

  public void testDeleteFileIfExists() throws Exception {
    Path dir = createTempDir();
    Path file1 = dir.resolve("file1");
    Files.createFile(file1);
    IOUtils.deleteFilesIfExist(file1);
    assertFalse(Files.exists(file1));
    // actually deletes
  }

  public void testDontDeleteDoesntExist() throws Exception {
    Path dir = createTempDir();
    Path file1 = dir.resolve("file1");
    IOUtils.deleteFilesIfExist(file1);
    // no exception
  }

  public void testDeleteTwoFilesIfExist() throws Exception {
    Path dir = createTempDir();
    Path file1 = dir.resolve("file1");
    Path file2 = dir.resolve("file2");
    // only create file2
    Files.createFile(file2);
    IOUtils.deleteFilesIfExist(file1, file2);
    assertFalse(Files.exists(file2));
    // no exception
    // actually deletes file2
  }

  public void testFsyncDirectory() throws Exception {
    Path dir = createTempDir();
    dir = FilterPath.unwrap(dir).toRealPath();

    Path devdir = dir.resolve("dev");
    Files.createDirectories(devdir);
    IOUtils.fsync(devdir, true);
    // no exception
  }

  private static final class AccessDeniedWhileOpeningDirectoryFileSystem
      extends FilterFileSystemProvider {

    AccessDeniedWhileOpeningDirectoryFileSystem(final FileSystem delegate) {
      super("access_denied://", Objects.requireNonNull(delegate));
    }

    @Override
    public FileChannel newFileChannel(
        final Path path, final Set<? extends OpenOption> options, final FileAttribute<?>... attrs)
        throws IOException {
      if (Files.isDirectory(path)) {
        throw new AccessDeniedException(path.toString());
      }
      return delegate.newFileChannel(path, options, attrs);
    }
  }

  public void testFsyncAccessDeniedOpeningDirectory() throws Exception {
    final Path path = createTempDir().toRealPath();
    final FileSystem fs =
        new AccessDeniedWhileOpeningDirectoryFileSystem(path.getFileSystem())
            .getFileSystem(URI.create("file:///"));
    final Path wrapped = new FilterPath(path, fs);
    if (Constants.WINDOWS) {
      // no exception, we early return and do not even try to open the directory
      IOUtils.fsync(wrapped, true);
    } else {
      expectThrows(AccessDeniedException.class, () -> IOUtils.fsync(wrapped, true));
    }
  }

  public void testFsyncNonExistentDirectory() throws Exception {
    final Path dir = FilterPath.unwrap(createTempDir()).toRealPath();
    final Path nonExistentDir = dir.resolve("non-existent");
    expectThrows(NoSuchFileException.class, () -> IOUtils.fsync(nonExistentDir, true));
  }

  public void testFsyncFile() throws Exception {
    Path dir = createTempDir();
    dir = FilterPath.unwrap(dir).toRealPath();

    Path devdir = dir.resolve("dev");
    Files.createDirectories(devdir);
    Path somefile = devdir.resolve("somefile");
    try (OutputStream o = Files.newOutputStream(somefile)) {
      o.write("0\n".getBytes(StandardCharsets.US_ASCII));
    }
    IOUtils.fsync(somefile, false);
    // no exception
  }

  public void testApplyToAll() {
    ArrayList<Integer> closed = new ArrayList<>();
    RuntimeException runtimeException =
        expectThrows(
            RuntimeException.class,
            () ->
                IOUtils.applyToAll(
                    Arrays.asList(1, 2),
                    i -> {
                      closed.add(i);
                      throw new RuntimeException("" + i);
                    }));
    assertEquals("1", runtimeException.getMessage());
    assertEquals(1, runtimeException.getSuppressed().length);
    assertEquals("2", runtimeException.getSuppressed()[0].getMessage());
    assertEquals(2, closed.size());
    assertEquals(1, closed.get(0).intValue());
    assertEquals(2, closed.get(1).intValue());
  }
}
