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
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.lucene.mockfile.FilterFileSystem;
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
  
  public void testSpinsBasics() throws Exception {
    Path dir = createTempDir();
    // no exception, directory exists
    IOUtils.spins(dir);
    Path file = dir.resolve("exists");
    Files.createFile(file);
    // no exception, file exists
    IOUtils.spins(file);
    
    // exception: file doesn't exist
    Path fake = dir.resolve("nonexistent");
    expectThrows(IOException.class, () -> {
      IOUtils.spins(fake);
    });
  }
  
  // fake up a filestore to test some underlying methods
  static class MockFileStore extends FileStore {
    final String description;
    final String type;
    final String name;
    
    MockFileStore(String description, String type, String name) {
      this.description = description;
      this.type = type;
      this.name = name;
    }

    @Override
    public String type() {
      return type;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String toString() {
      return description;
    }


    // TODO: we can enable mocking of these when we need them later:

    @Override
    public boolean isReadOnly() {
      return false;
    }

    @Override
    public long getTotalSpace() throws IOException {
      return 1000;
    }

    @Override
    public long getUsableSpace() throws IOException {
      return 800;
    }

    @Override
    public long getUnallocatedSpace() throws IOException {
      return 1000;
    }

    @Override
    public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
      return false;
    }

    @Override
    public boolean supportsFileAttributeView(String name) {
      return false;
    }

    @Override
    public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
      return null;
    }

    @Override
    public Object getAttribute(String attribute) throws IOException {
      return null;
    }
  }
  
  public void testGetMountPoint() throws Exception {
    assertEquals("/", IOUtils.getMountPoint(new MockFileStore("/ (/dev/sda1)", "ext4", "/dev/sda1")));
    assertEquals("/test/ space(((trash)))/", IOUtils.getMountPoint(new MockFileStore("/test/ space(((trash)))/ (/dev/sda1)", "ext3", "/dev/sda1")));
    assertEquals("/", IOUtils.getMountPoint(new MockFileStore("/ (notreal)", "ext2", "notreal")));
  }
  
  /** mock linux that takes mappings of test files, to their associated filesystems.
   *  it will chroot /dev and /sys requests to root, so you can mock those too.
   *  <p>
   *  It is hacky by definition, so don't try putting it around a complex chain or anything.
   */
  static class MockLinuxFileSystemProvider extends FilterFileSystemProvider {
    final Map<String,FileStore> filesToStore;
    final Path root;
    
    public MockLinuxFileSystemProvider(FileSystem delegateInstance, final Map<String,FileStore> filesToStore, Path root) {
      super("mocklinux://", delegateInstance);
      final Collection<FileStore> allStores = new HashSet<>(filesToStore.values());
      this.fileSystem = new FilterFileSystem(this, delegateInstance) {
        @Override
        public Iterable<FileStore> getFileStores() {
          return allStores;
        }

        @Override
        public Path getPath(String first, String... more) {
          return new MockLinuxPath(delegateInstance.getPath(first, more), this);
        }
      };
      this.filesToStore = filesToStore;
      this.root = new MockLinuxPath(root, this.fileSystem);
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException {
      FileStore ret = filesToStore.get(path.toString());
      if (ret == null) {
        throw new IllegalArgumentException("this mock doesnt know wtf to do with: " + path);
      }
      // act like the linux fs provider here, return a crazy rootfs one
      if (ret.toString().startsWith(root + " (")) {
        return new MockFileStore(root + " (rootfs)", "rootfs", "rootfs");
      }

      return ret;
    }

    Path maybeChroot(Path path) {
      if (path.toAbsolutePath().startsWith("/sys") || path.toAbsolutePath().startsWith("/dev")) {
        // map to our chrooted location;
        return path.getRoot().resolve(root).resolve(path.toString().substring(1));
      } else {
        return path;
      }
    }
    
    @Override
    protected Path toDelegate(Path path) {
      return super.toDelegate(maybeChroot(path));
    }

    class MockLinuxPath extends FilterPath {
      MockLinuxPath(Path delegate, FileSystem fileSystem) {
        super(delegate, fileSystem);
      }
      
      @Override
      public Path toRealPath(LinkOption... options) throws IOException {
        Path p = maybeChroot(this);
        if (p == this) {
          return super.toRealPath(options);
        } else {
          return p.toRealPath(options);
        }
      }

      @Override
      protected Path wrap(Path other) {
        return new MockLinuxPath(other, fileSystem);
      }
    }
  }
  
  public void testGetFileStore() throws Exception {
    Path dir = createTempDir();
    dir = FilterPath.unwrap(dir).toRealPath();
    
    // now we can create some fake mount points:
    FileStore root = new MockFileStore(dir.toString() + " (/dev/sda1)", "ntfs", "/dev/sda1");
    FileStore usr = new MockFileStore(dir.resolve("usr").toString() + " (/dev/sda2)", "xfs", "/dev/sda2");

    // associate some preset files to these
    Map<String,FileStore> mappings = new HashMap<>();
    mappings.put(dir.toString(), root);
    mappings.put(dir.resolve("foo.txt").toString(), root);
    mappings.put(dir.resolve("usr").toString(), usr);
    mappings.put(dir.resolve("usr/bar.txt").toString(), usr);
    
    FileSystem mockLinux = new MockLinuxFileSystemProvider(dir.getFileSystem(), mappings, dir).getFileSystem(null);
    Path mockPath = mockLinux.getPath(dir.toString());
    
    // sanity check our mock:
    assertSame(usr, Files.getFileStore(mockPath.resolve("usr")));
    assertSame(usr, Files.getFileStore(mockPath.resolve("usr/bar.txt")));
    // for root filesystem we get a crappy one
    assertNotSame(root, Files.getFileStore(mockPath));
    assertNotSame(usr, Files.getFileStore(mockPath));
    assertNotSame(root, Files.getFileStore(mockPath.resolve("foo.txt")));
    assertNotSame(usr, Files.getFileStore(mockPath.resolve("foo.txt")));
    
    // now test our method:
    assertSame(usr, IOUtils.getFileStore(mockPath.resolve("usr")));
    assertSame(usr, IOUtils.getFileStore(mockPath.resolve("usr/bar.txt")));
    assertSame(root, IOUtils.getFileStore(mockPath));
    assertSame(root, IOUtils.getFileStore(mockPath.resolve("foo.txt")));
  }
  
  public void testTmpfsDoesntSpin() throws Exception {
    Path dir = createTempDir();
    dir = FilterPath.unwrap(dir).toRealPath();
    
    // fake tmpfs
    FileStore root = new MockFileStore(dir.toString() + " (/dev/sda1)", "tmpfs", "/dev/sda1");
    Map<String,FileStore> mappings = Collections.singletonMap(dir.toString(), root);
    FileSystem mockLinux = new MockLinuxFileSystemProvider(dir.getFileSystem(), mappings, dir).getFileSystem(null);
    
    Path mockPath = mockLinux.getPath(dir.toString());
    assertFalse(IOUtils.spinsLinux(mockPath));
  }
  
  public void testNfsSpins() throws Exception {
    Path dir = createTempDir();
    dir = FilterPath.unwrap(dir).toRealPath();
    
    // fake nfs
    FileStore root = new MockFileStore(dir.toString() + " (somenfsserver:/some/mount)", "nfs", "somenfsserver:/some/mount");
    Map<String,FileStore> mappings = Collections.singletonMap(dir.toString(), root);
    FileSystem mockLinux = new MockLinuxFileSystemProvider(dir.getFileSystem(), mappings, dir).getFileSystem(null);
    
    Path mockPath = mockLinux.getPath(dir.toString());
    assertTrue(IOUtils.spinsLinux(mockPath));
  }
  
  public void testSSD() throws Exception {
    assumeFalse("windows is not supported", Constants.WINDOWS);
    Path dir = createTempDir();
    dir = FilterPath.unwrap(dir).toRealPath();
    
    // fake ssd
    FileStore root = new MockFileStore(dir.toString() + " (/dev/zzz1)", "btrfs", "/dev/zzz1");
    // make a fake /dev/zzz1 for it
    Path devdir = dir.resolve("dev");
    Files.createDirectories(devdir);
    Files.createFile(devdir.resolve("zzz1"));
    // make a fake /sys/block/zzz/queue/rotational file for it
    Path sysdir = dir.resolve("sys").resolve("block").resolve("zzz").resolve("queue");
    Files.createDirectories(sysdir);
    try (OutputStream o = Files.newOutputStream(sysdir.resolve("rotational"))) {
      o.write("0\n".getBytes(StandardCharsets.US_ASCII));
    }
    Map<String,FileStore> mappings = Collections.singletonMap(dir.toString(), root);
    FileSystem mockLinux = new MockLinuxFileSystemProvider(dir.getFileSystem(), mappings, dir).getFileSystem(null);
    
    Path mockPath = mockLinux.getPath(dir.toString());
    assertFalse(IOUtils.spinsLinux(mockPath));
  }
  
  public void testNVME() throws Exception {
    assumeFalse("windows is not supported", Constants.WINDOWS);
    Path dir = createTempDir();
    dir = FilterPath.unwrap(dir).toRealPath();
    
    // fake ssd
    FileStore root = new MockFileStore(dir.toString() + " (/dev/nvme0n1p1)", "btrfs", "/dev/nvme0n1p1");
    // make a fake /dev/nvme0n1p1 for it
    Path devdir = dir.resolve("dev");
    Files.createDirectories(devdir);
    Files.createFile(devdir.resolve("nvme0n1p1"));
    // make a fake /sys/block/nvme0n1/queue/rotational file for it
    Path sysdir = dir.resolve("sys").resolve("block").resolve("nvme0n1").resolve("queue");
    Files.createDirectories(sysdir);
    try (OutputStream o = Files.newOutputStream(sysdir.resolve("rotational"))) {
      o.write("0\n".getBytes(StandardCharsets.US_ASCII));
    }
    // As test for the longest path match, add some other devices (that have no queue/rotational), too:
    Files.createFile(dir.resolve("sys").resolve("block").resolve("nvme0"));
    Files.createFile(dir.resolve("sys").resolve("block").resolve("dummy"));
    Files.createFile(dir.resolve("sys").resolve("block").resolve("nvm"));
    Map<String,FileStore> mappings = Collections.singletonMap(dir.toString(), root);
    FileSystem mockLinux = new MockLinuxFileSystemProvider(dir.getFileSystem(), mappings, dir).getFileSystem(null);
    
    Path mockPath = mockLinux.getPath(dir.toString());
    assertFalse(IOUtils.spinsLinux(mockPath));
  }
  
  public void testRotatingPlatters() throws Exception {
    assumeFalse("windows is not supported", Constants.WINDOWS);
    Path dir = createTempDir();
    dir = FilterPath.unwrap(dir).toRealPath();
    
    // fake ssd
    FileStore root = new MockFileStore(dir.toString() + " (/dev/zzz1)", "reiser4", "/dev/zzz1");
    // make a fake /dev/zzz1 for it
    Path devdir = dir.resolve("dev");
    Files.createDirectories(devdir);
    Files.createFile(devdir.resolve("zzz1"));
    // make a fake /sys/block/zzz/queue/rotational file for it
    Path sysdir = dir.resolve("sys").resolve("block").resolve("zzz").resolve("queue");
    Files.createDirectories(sysdir);
    try (OutputStream o = Files.newOutputStream(sysdir.resolve("rotational"))) {
      o.write("1\n".getBytes(StandardCharsets.US_ASCII));
    }
    Map<String,FileStore> mappings = Collections.singletonMap(dir.toString(), root);
    FileSystem mockLinux = new MockLinuxFileSystemProvider(dir.getFileSystem(), mappings, dir).getFileSystem(null);
    
    Path mockPath = mockLinux.getPath(dir.toString());
    assertTrue(IOUtils.spinsLinux(mockPath));
  }
  
  public void testManyPartitions() throws Exception {
    assumeFalse("windows is not supported", Constants.WINDOWS);
    Path dir = createTempDir();
    dir = FilterPath.unwrap(dir).toRealPath();
    
    // fake ssd
    FileStore root = new MockFileStore(dir.toString() + " (/dev/zzz12)", "zfs", "/dev/zzz12");
    // make a fake /dev/zzz11 for it
    Path devdir = dir.resolve("dev");
    Files.createDirectories(devdir);
    Files.createFile(devdir.resolve("zzz12"));
    // make a fake /sys/block/zzz/queue/rotational file for it
    Path sysdir = dir.resolve("sys").resolve("block").resolve("zzz").resolve("queue");
    Files.createDirectories(sysdir);
    try (OutputStream o = Files.newOutputStream(sysdir.resolve("rotational"))) {
      o.write("0\n".getBytes(StandardCharsets.US_ASCII));
    }
    Map<String,FileStore> mappings = Collections.singletonMap(dir.toString(), root);
    FileSystem mockLinux = new MockLinuxFileSystemProvider(dir.getFileSystem(), mappings, dir).getFileSystem(null);
    
    Path mockPath = mockLinux.getPath(dir.toString());
    assertFalse(IOUtils.spinsLinux(mockPath));
  }
  
  public void testSymlinkSSD() throws Exception {
    assumeFalse("windows is not supported", Constants.WINDOWS);
    Path dir = createTempDir();
    dir = FilterPath.unwrap(dir).toRealPath();
    
    // fake SSD with a symlink mount (Ubuntu-like):
    Random rnd = random();
    String partitionUUID = new UUID(rnd.nextLong(), rnd.nextLong()).toString();
    FileStore root = new MockFileStore(dir.toString() + " (/dev/disk/by-uuid/"+partitionUUID+")", "btrfs", "/dev/disk/by-uuid/"+partitionUUID);
    // make a fake /dev/sda1 for it
    Path devdir = dir.resolve("dev");
    Files.createDirectories(devdir);
    Path deviceFile = devdir.resolve("sda1");
    Files.createFile(deviceFile);
    // create a symlink to the above device file
    Path symlinkdir = devdir.resolve("disk").resolve("by-uuid");
    Files.createDirectories(symlinkdir);
    try {
      Files.createSymbolicLink(symlinkdir.resolve(partitionUUID), deviceFile);
    } catch (UnsupportedOperationException | IOException e) {
      assumeNoException("test requires filesystem that supports symbolic links", e);
    }
    // make a fake /sys/block/sda/queue/rotational file for it
    Path sysdir = dir.resolve("sys").resolve("block").resolve("sda").resolve("queue");
    Files.createDirectories(sysdir);
    try (OutputStream o = Files.newOutputStream(sysdir.resolve("rotational"))) {
      o.write("0\n".getBytes(StandardCharsets.US_ASCII));
    }
    Map<String,FileStore> mappings = Collections.singletonMap(dir.toString(), root);
    FileSystem mockLinux = new MockLinuxFileSystemProvider(dir.getFileSystem(), mappings, dir).getFileSystem(null);
    
    Path mockPath = mockLinux.getPath(dir.toString());
    assertFalse(IOUtils.spinsLinux(mockPath));
  }
  
  public void testFsyncDirectory() throws Exception {
    Path dir = createTempDir();
    dir = FilterPath.unwrap(dir).toRealPath();

    Path devdir = dir.resolve("dev");
    Files.createDirectories(devdir);
    IOUtils.fsync(devdir, true);
    // no exception
  }

  private static final class AccessDeniedWhileOpeningDirectoryFileSystem extends FilterFileSystemProvider {

    AccessDeniedWhileOpeningDirectoryFileSystem(final FileSystem delegate) {
      super("access_denied://", Objects.requireNonNull(delegate));
    }

    @Override
    public FileChannel newFileChannel(
        final Path path,
        final Set<? extends OpenOption> options,
        final FileAttribute<?>... attrs) throws IOException {
      if (Files.isDirectory(path)) {
        throw new AccessDeniedException(path.toString());
      }
      return delegate.newFileChannel(path, options, attrs);
    }

  }

  public void testFsyncAccessDeniedOpeningDirectory() throws Exception {
    final Path path = createTempDir().toRealPath();
    final FileSystem fs = new AccessDeniedWhileOpeningDirectoryFileSystem(path.getFileSystem()).getFileSystem(URI.create("file:///"));
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
    RuntimeException runtimeException = expectThrows(RuntimeException.class, () ->
        IOUtils.applyToAll(Arrays.asList(1, 2), i -> {
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
