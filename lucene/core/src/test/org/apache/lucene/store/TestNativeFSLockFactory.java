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
package org.apache.lucene.store;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;
import org.apache.lucene.mockfile.FilterFileSystemProvider;
import org.apache.lucene.mockfile.FilterPath;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;

/** Simple tests for NativeFSLockFactory */
public class TestNativeFSLockFactory extends BaseLockFactoryTestCase {

  @Override
  protected Directory getDirectory(Path path) throws IOException {
    return newFSDirectory(path, NativeFSLockFactory.INSTANCE);
  }

  /** Verify NativeFSLockFactory works correctly if the lock file exists */
  public void testLockFileExists() throws IOException {
    Path tempDir = createTempDir();
    Path lockFile = tempDir.resolve("test.lock");
    Files.createFile(lockFile);

    Directory dir = getDirectory(tempDir);
    Lock l = dir.obtainLock("test.lock");
    l.close();
    dir.close();
  }

  /** release the lock and test ensureValid fails */
  public void testInvalidateLock() throws IOException {
    Directory dir = getDirectory(createTempDir());
    NativeFSLockFactory.NativeFSLock lock =
        (NativeFSLockFactory.NativeFSLock) dir.obtainLock("test.lock");
    lock.ensureValid();
    lock.lock.release();
    expectThrows(
        AlreadyClosedException.class,
        () -> {
          lock.ensureValid();
        });

    IOUtils.closeWhileHandlingException(lock);
    dir.close();
  }

  /** close the channel and test ensureValid fails */
  public void testInvalidateChannel() throws IOException {
    Directory dir = getDirectory(createTempDir());
    NativeFSLockFactory.NativeFSLock lock =
        (NativeFSLockFactory.NativeFSLock) dir.obtainLock("test.lock");
    lock.ensureValid();
    lock.channel.close();
    expectThrows(
        AlreadyClosedException.class,
        () -> {
          lock.ensureValid();
        });

    IOUtils.closeWhileHandlingException(lock);
    dir.close();
  }

  /** delete the lockfile and test ensureValid fails */
  public void testDeleteLockFile() throws IOException {
    try (Directory dir = getDirectory(createTempDir())) {
      assumeFalse("we must be able to delete an open file", TestUtil.hasWindowsFS(dir));

      Lock lock = dir.obtainLock("test.lock");
      lock.ensureValid();

      dir.deleteFile("test.lock");

      expectThrows(
          IOException.class,
          () -> {
            lock.ensureValid();
          });

      IOUtils.closeWhileHandlingException(lock);
    }
  }

  /** MockFileSystem that throws AccessDeniedException on creating test.lock */
  static class MockBadPermissionsFileSystem extends FilterFileSystemProvider {
    public MockBadPermissionsFileSystem(FileSystem delegateInstance) {
      super("mockbadpermissions://", delegateInstance);
    }

    @Override
    public SeekableByteChannel newByteChannel(
        Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs)
        throws IOException {
      if (path.getFileName().toString().equals("test.lock")) {
        throw new AccessDeniedException(path.toString(), null, "fake access denied");
      }
      return super.newByteChannel(path, options, attrs);
    }
  }

  public void testBadPermissions() throws IOException {
    // create a mock filesystem that will throw exc on creating test.lock
    Path tmpDir = createTempDir();
    tmpDir = FilterPath.unwrap(tmpDir).toRealPath();
    FileSystem mock = new MockBadPermissionsFileSystem(tmpDir.getFileSystem()).getFileSystem(null);
    Path mockPath = mock.getPath(tmpDir.toString());

    // we should get an IOException (typically NoSuchFileException but no guarantee) with
    // our fake AccessDenied added as suppressed.
    Directory dir = getDirectory(mockPath.resolve("indexDir"));
    IOException expected =
        expectThrows(
            IOException.class,
            () -> {
              dir.obtainLock("test.lock");
            });
    AccessDeniedException suppressed = (AccessDeniedException) expected.getSuppressed()[0];
    assertTrue(suppressed.getMessage().contains("fake access denied"));

    dir.close();
  }
}
