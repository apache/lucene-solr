package org.apache.lucene.util;

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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessMode;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.lucene.mockfile.FilterFileStore;
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
    try {
      IOUtils.spins(fake);
      fail();
    } catch (IOException expected) {
      // ok
    }
  }
  
  // fake up a filestore to test some underlying methods
  static class MockFileStore extends FilterFileStore {
    final String description;
    final String type;
    
    MockFileStore(FileStore delegate, String description) {
      this(delegate, description, "mockfs");
    }
    
    MockFileStore(FileStore delegate, String description, String type) {
      super(delegate, "justafake://");
      this.description = description;
      this.type = type;
    }

    @Override
    public String type() {
      return type;
    }

    @Override
    public String toString() {
      return description;
    }
  }
  
  public void testGetBlockDevice() throws Exception {
    Path dir = createTempDir();
    FileStore actual = Files.getFileStore(dir);

    assertEquals("/dev/sda1", IOUtils.getBlockDevice(new MockFileStore(actual, "/ (/dev/sda1)")));
    assertEquals("/dev/sda1", IOUtils.getBlockDevice(new MockFileStore(actual, "/test/ space(((trash)))/ (/dev/sda1)")));
    assertEquals("notreal", IOUtils.getBlockDevice(new MockFileStore(actual, "/ (notreal)")));
  }
  
  public void testGetMountPoint() throws Exception {
    Path dir = createTempDir();
    FileStore actual = Files.getFileStore(dir);

    assertEquals("/", IOUtils.getMountPoint(new MockFileStore(actual, "/ (/dev/sda1)")));
    assertEquals("/test/ space(((trash)))/", IOUtils.getMountPoint(new MockFileStore(actual, "/test/ space(((trash)))/ (/dev/sda1)")));
    assertEquals("/", IOUtils.getMountPoint(new MockFileStore(actual, "/ (notreal)")));
  }
  
  /** mock linux that takes mappings of test files, to their associated filesystems.
   *  it will chroot /dev and /sys requests to root, so you can mock those too.
   *  <p>
   *  It is hacky by definition, so don't try putting it around a complex chain or anything.
   *  Use FilterPath.unwrap
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
          return new MockLinuxPath(super.getPath(first, more), this);
        }
      };
      this.filesToStore = filesToStore;
      this.root = root;
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException {
      FileStore ret = filesToStore.get(path.toString());
      if (ret == null) {
        throw new IllegalArgumentException("this mock doesnt know wtf to do with: " + path);
      }
      // act like the linux fs provider here, return a crazy rootfs one
      if (ret.toString().startsWith(root + " (")) {
        return new MockFileStore(ret, root + " (rootfs)", "rootfs");
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
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
      // TODO: kinda screwed up how we do this, but it's easy to get lost. just unravel completely.
      delegate.checkAccess(maybeChroot(FilterPath.unwrap(path)), modes);
    }

    @Override
    public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
      return super.newInputStream(maybeChroot(path), options);
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
    FileStore root = new MockFileStore(Files.getFileStore(dir), dir.toString() + " (/dev/sda1)");
    FileStore usr = new MockFileStore(Files.getFileStore(dir), dir.resolve("usr").toString() + " (/dev/sda2)");

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
    FileStore root = new MockFileStore(Files.getFileStore(dir), dir.toString() + " (/dev/sda1)", "tmpfs");
    Map<String,FileStore> mappings = Collections.singletonMap(dir.toString(), root);
    FileSystem mockLinux = new MockLinuxFileSystemProvider(dir.getFileSystem(), mappings, dir).getFileSystem(null);
    
    Path mockPath = mockLinux.getPath(dir.toString());
    assertFalse(IOUtils.spinsLinux(mockPath));
  }
  
  public void testNfsSpins() throws Exception {
    Path dir = createTempDir();
    dir = FilterPath.unwrap(dir).toRealPath();
    
    // fake nfs
    FileStore root = new MockFileStore(Files.getFileStore(dir), dir.toString() + " (somenfsserver:/some/mount)", "nfs");
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
    FileStore root = new MockFileStore(Files.getFileStore(dir), dir.toString() + " (/dev/zzz1)");
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
  
  public void testRotatingPlatters() throws Exception {
    assumeFalse("windows is not supported", Constants.WINDOWS);
    Path dir = createTempDir();
    dir = FilterPath.unwrap(dir).toRealPath();
    
    // fake ssd
    FileStore root = new MockFileStore(Files.getFileStore(dir), dir.toString() + " (/dev/zzz1)");
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
    FileStore root = new MockFileStore(Files.getFileStore(dir), dir.toString() + " (/dev/zzz12)");
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
  
}
