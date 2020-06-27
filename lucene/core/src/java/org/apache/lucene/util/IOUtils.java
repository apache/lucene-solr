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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.RAMDirectory;

/** This class emulates the new Java 7 "Try-With-Resources" statement.
 * Remove once Lucene is on Java 7.
 * @lucene.internal */
public final class IOUtils {
  
  /**
   * UTF-8 charset string.
   * <p>Where possible, use {@link StandardCharsets#UTF_8} instead,
   * as using the String constant may slow things down.
   * @see StandardCharsets#UTF_8
   */
  public static final String UTF_8 = StandardCharsets.UTF_8.name();
  
  private IOUtils() {} // no instance

  /**
   * Closes all given <tt>Closeable</tt>s.  Some of the
   * <tt>Closeable</tt>s may be null; they are
   * ignored.  After everything is closed, the method either
   * throws the first exception it hit while closing, or
   * completes normally if there were no exceptions.
   * 
   * @param objects
   *          objects to call <tt>close()</tt> on
   */
  public static void close(Closeable... objects) throws IOException {
    close(Arrays.asList(objects));
  }
  
  /**
   * Closes all given <tt>Closeable</tt>s.
   * @see #close(Closeable...)
   */
  public static void close(Iterable<? extends Closeable> objects) throws IOException {
    Throwable th = null;
    for (Closeable object : objects) {
      try {
        if (object != null) {
          object.close();
        }
      } catch (Throwable t) {
        th = useOrSuppress(th, t);
      }
    }

    if (th != null) {
      throw rethrowAlways(th);
    }
  }

  /**
   * Closes all given <tt>Closeable</tt>s, suppressing all thrown exceptions.
   * Some of the <tt>Closeable</tt>s may be null, they are ignored.
   * 
   * @param objects
   *          objects to call <tt>close()</tt> on
   */
  public static void closeWhileHandlingException(Closeable... objects) {
    closeWhileHandlingException(Arrays.asList(objects));
  }
  
  /**
   * Closes all given <tt>Closeable</tt>s, suppressing all thrown non {@link VirtualMachineError} exceptions.
   * Even if a {@link VirtualMachineError} is thrown all given closeable are closed.
   * @see #closeWhileHandlingException(Closeable...)
   */
  public static void closeWhileHandlingException(Iterable<? extends Closeable> objects) {
    VirtualMachineError firstError = null;
    Throwable firstThrowable = null;
    for (Closeable object : objects) {
      try {
        if (object != null) {
          object.close();
        }
      } catch (VirtualMachineError e) {
        firstError = useOrSuppress(firstError, e);
      } catch (Throwable t) {
        firstThrowable = useOrSuppress(firstThrowable, t);
      }
    }
    if (firstError != null) {
      // we ensure that we bubble up any errors. We can't recover from these but need to make sure they are
      // bubbled up. if a non-VMError is thrown we also add the suppressed exceptions to it.
      if (firstThrowable != null) {
        firstError.addSuppressed(firstThrowable);
      }
      throw firstError;
    }
  }
  
  /**
   * Wrapping the given {@link InputStream} in a reader using a {@link CharsetDecoder}.
   * Unlike Java's defaults this reader will throw an exception if your it detects 
   * the read charset doesn't match the expected {@link Charset}. 
   * <p>
   * Decoding readers are useful to load configuration files, stopword lists or synonym files
   * to detect character set problems. However, it's not recommended to use as a common purpose 
   * reader.
   * 
   * @param stream the stream to wrap in a reader
   * @param charSet the expected charset
   * @return a wrapping reader
   */
  public static Reader getDecodingReader(InputStream stream, Charset charSet) {
    final CharsetDecoder charSetDecoder = charSet.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT);
    return new BufferedReader(new InputStreamReader(stream, charSetDecoder));
  }

  /**
   * Opens a Reader for the given resource using a {@link CharsetDecoder}.
   * Unlike Java's defaults this reader will throw an exception if your it detects 
   * the read charset doesn't match the expected {@link Charset}. 
   * <p>
   * Decoding readers are useful to load configuration files, stopword lists or synonym files
   * to detect character set problems. However, it's not recommended to use as a common purpose 
   * reader.
   * @param clazz the class used to locate the resource
   * @param resource the resource name to load
   * @param charSet the expected charset
   * @return a reader to read the given file
   * 
   */
  public static Reader getDecodingReader(Class<?> clazz, String resource, Charset charSet) throws IOException {
    InputStream stream = null;
    boolean success = false;
    try {
      stream = clazz
      .getResourceAsStream(resource);
      final Reader reader = getDecodingReader(stream, charSet);
      success = true;
      return reader;
    } finally {
      if (!success) {
        IOUtils.close(stream);
      }
    }
  }
  
  /**
   * Deletes all given files, suppressing all thrown IOExceptions.
   * <p>
   * Note that the files should not be null.
   */
  public static void deleteFilesIgnoringExceptions(Directory dir, Collection<String> files) {
    for(String name : files) {
      try {
        dir.deleteFile(name);
      } catch (Throwable ignored) {
        // ignore
      }
    }
  }

  public static void deleteFilesIgnoringExceptions(Directory dir, String... files) {
    deleteFilesIgnoringExceptions(dir, Arrays.asList(files));
  }
  
  /**
   * Deletes all given file names.  Some of the
   * file names may be null; they are
   * ignored.  After everything is deleted, the method either
   * throws the first exception it hit while deleting, or
   * completes normally if there were no exceptions.
   * 
   * @param dir Directory to delete files from
   * @param names file names to delete
   */
  public static void deleteFiles(Directory dir, Collection<String> names) throws IOException {
    Throwable th = null;
    for (String name : names) {
      if (name != null) {
        try {
          dir.deleteFile(name);
        } catch (Throwable t) {
          th = useOrSuppress(th, t);
        }
      }
    }

    if (th != null) {
      throw rethrowAlways(th);
    }
  }

  /**
   * Deletes all given files, suppressing all thrown IOExceptions.
   * <p>
   * Some of the files may be null, if so they are ignored.
   */
  public static void deleteFilesIgnoringExceptions(Path... files) {
    deleteFilesIgnoringExceptions(Arrays.asList(files));
  }
  
  /**
   * Deletes all given files, suppressing all thrown IOExceptions.
   * <p>
   * Some of the files may be null, if so they are ignored.
   */
  public static void deleteFilesIgnoringExceptions(Collection<? extends Path> files) {
    for (Path name : files) {
      if (name != null) {
        try {
          Files.delete(name);
        } catch (Throwable ignored) {
          // ignore
        }
      }
    }
  }
  
  /**
   * Deletes all given <tt>Path</tt>s, if they exist.  Some of the
   * <tt>File</tt>s may be null; they are
   * ignored.  After everything is deleted, the method either
   * throws the first exception it hit while deleting, or
   * completes normally if there were no exceptions.
   * 
   * @param files files to delete
   */
  public static void deleteFilesIfExist(Path... files) throws IOException {
    deleteFilesIfExist(Arrays.asList(files));
  }
  
  /**
   * Deletes all given <tt>Path</tt>s, if they exist.  Some of the
   * <tt>File</tt>s may be null; they are
   * ignored.  After everything is deleted, the method either
   * throws the first exception it hit while deleting, or
   * completes normally if there were no exceptions.
   * 
   * @param files files to delete
   */
  public static void deleteFilesIfExist(Collection<? extends Path> files) throws IOException {
    Throwable th = null;
    for (Path file : files) {
      try {
        if (file != null) {
          Files.deleteIfExists(file);
        }
      } catch (Throwable t) {
        th = useOrSuppress(th, t);
      }
    }

    if (th != null) {
      throw rethrowAlways(th);
    }
  }
  
  /**
   * Deletes one or more files or directories (and everything underneath it).
   * 
   * @throws IOException if any of the given files (or their subhierarchy files in case
   * of directories) cannot be removed.
   */
  public static void rm(Path... locations) throws IOException {
    LinkedHashMap<Path,Throwable> unremoved = rm(new LinkedHashMap<Path,Throwable>(), locations);
    if (!unremoved.isEmpty()) {
      StringBuilder b = new StringBuilder("Could not remove the following files (in the order of attempts):\n");
      for (Map.Entry<Path,Throwable> kv : unremoved.entrySet()) {
        b.append("   ")
         .append(kv.getKey().toAbsolutePath())
         .append(": ")
         .append(kv.getValue())
         .append("\n");
      }
      throw new IOException(b.toString());
    }
  }

  private static LinkedHashMap<Path,Throwable> rm(final LinkedHashMap<Path,Throwable> unremoved, Path... locations) {
    if (locations != null) {
      for (Path location : locations) {
        // TODO: remove this leniency!
        if (location != null && Files.exists(location)) {
          try {
            Files.walkFileTree(location, new FileVisitor<Path>() {            
              @Override
              public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                return FileVisitResult.CONTINUE;
              }
              
              @Override
              public FileVisitResult postVisitDirectory(Path dir, IOException impossible) throws IOException {
                assert impossible == null;
                
                try {
                  Files.delete(dir);
                } catch (IOException e) {
                  unremoved.put(dir, e);
                }
                return FileVisitResult.CONTINUE;
              }
              
              @Override
              public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                try {
                  Files.delete(file);
                } catch (IOException exc) {
                  unremoved.put(file, exc);
                }
                return FileVisitResult.CONTINUE;
              }
              
              @Override
              public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                if (exc != null) {
                  unremoved.put(file, exc);
                }
                return FileVisitResult.CONTINUE;
              }
            });
          } catch (IOException impossible) {
            throw new AssertionError("visitor threw exception", impossible);
          }
        }
      }
    }
    return unremoved;
  }

  /**
   * This utility method takes a previously caught (non-null)
   * {@code Throwable} and rethrows either the original argument
   * if it was a subclass of the {@code IOException} or an 
   * {@code RuntimeException} with the cause set to the argument.
   * 
   * <p>This method <strong>never returns any value</strong>, even though it declares
   * a return value of type {@link Error}. The return value declaration
   * is very useful to let the compiler know that the code path following
   * the invocation of this method is unreachable. So in most cases the
   * invocation of this method will be guarded by an {@code if} and
   * used together with a {@code throw} statement, as in:
   * </p>
   * <pre>{@code
   *   if (t != null) throw IOUtils.rethrowAlways(t)
   * }
   * </pre>
   * 
   * @param th The throwable to rethrow, <strong>must not be null</strong>.
   * @return This method always results in an exception, it never returns any value. 
   *         See method documentation for detailsa and usage example.
   * @throws IOException if the argument was an instance of IOException
   * @throws RuntimeException with the {@link RuntimeException#getCause()} set
   *         to the argument, if it was not an instance of IOException. 
   */
  public static Error rethrowAlways(Throwable th) throws IOException, RuntimeException {
    if (th == null) {
      throw new AssertionError("rethrow argument must not be null.");
    }

    if (th instanceof IOException) {
      throw (IOException) th;
    }

    if (th instanceof RuntimeException) {
      throw (RuntimeException) th;
    }

    if (th instanceof Error) {
      throw (Error) th;
    }

    throw new RuntimeException(th);
  }

  /**
   * Rethrows the argument as {@code IOException} or {@code RuntimeException} 
   * if it's not null.
   * 
   * @deprecated This method is deprecated in favor of {@link #rethrowAlways}. Code should
   * be updated to {@link #rethrowAlways} and guarded with an additional null-argument check
   * (because {@link #rethrowAlways} is not accepting null arguments). 
   */
  @Deprecated
  public static void reThrow(Throwable th) throws IOException {
    if (th != null) {
      throw rethrowAlways(th);
    }
  }
  
  /**
   * @deprecated This method is deprecated in favor of {@link #rethrowAlways}. Code should
   * be updated to {@link #rethrowAlways} and guarded with an additional null-argument check
   * (because {@link #rethrowAlways} is not accepting null arguments). 
   */
  @Deprecated
  public static void reThrowUnchecked(Throwable th) {
    if (th != null) {
      if (th instanceof Error) {
        throw (Error) th;
      }
      if (th instanceof RuntimeException) {
        throw (RuntimeException) th;
      }
      throw new RuntimeException(th);
    }    
  }
  
  /**
   * Ensure that any writes to the given file is written to the storage device that contains it.
   * @param fileToSync the file to fsync
   * @param isDir if true, the given file is a directory (we open for read and ignore IOExceptions,
   *  because not all file systems and operating systems allow to fsync on a directory)
   */
  public static void fsync(Path fileToSync, boolean isDir) throws IOException {
    // If the file is a directory we have to open read-only, for regular files we must open r/w for the fsync to have an effect.
    // See http://blog.httrack.com/blog/2013/11/15/everything-you-always-wanted-to-know-about-fsync/
    if (isDir && Constants.WINDOWS) {
      // opening a directory on Windows fails, directories can not be fsynced there
      if (Files.exists(fileToSync) == false) {
        // yet do not suppress trying to fsync directories that do not exist
        throw new NoSuchFileException(fileToSync.toString());
      }
      return;
    }
    try (final FileChannel file = FileChannel.open(fileToSync, isDir ? StandardOpenOption.READ : StandardOpenOption.WRITE)) {
      try {
        file.force(true);
      } catch (final IOException e) {
        if (isDir) {
          assert (Constants.LINUX || Constants.MAC_OS_X) == false :
              "On Linux and MacOSX fsyncing a directory should not throw IOException, " +
                  "we just don't want to rely on that in production (undocumented). Got: " + e;
          // Ignore exception if it is a directory
          return;
        }
        // Throw original exception
        throw e;
      }
    }
  }

  /** If the dir is an {@link FSDirectory} or wraps one via possibly
   *  nested {@link FilterDirectory} or {@link FileSwitchDirectory},
   *  this returns {@link #spins(Path)} for the wrapped directory,
   *  else, true.
   *
   *  @throws IOException if {@code path} does not exist.
   *
   *  @lucene.internal */
  public static boolean spins(Directory dir) throws IOException {
    dir = FilterDirectory.unwrap(dir);
    if (dir instanceof FileSwitchDirectory) {
      FileSwitchDirectory fsd = (FileSwitchDirectory) dir;
      // Spinning is contagious:
      return spins(fsd.getPrimaryDir()) || spins(fsd.getSecondaryDir());
    } else if (dir instanceof RAMDirectory) {
      return false;
    } else if (dir instanceof FSDirectory) {
      return spins(((FSDirectory) dir).getDirectory());
    } else {
      return true;
    }
  }

  /** Rough Linux-only heuristics to determine whether the provided
   *  {@code Path} is backed by spinning storage.  For example, this
   *  returns false if the disk is a solid-state disk.
   *
   *  @param path a location to check which must exist. the mount point will be determined from this location.
   *  @return false if the storage is non-rotational (e.g. an SSD), or true if it is spinning or could not be determined
   *  @throws IOException if {@code path} does not exist.
   *
   *  @lucene.internal */
  public static boolean spins(Path path) throws IOException {
    // resolve symlinks (this will throw exception if the path does not exist)
    path = path.toRealPath();
    
    // Super cowboy approach, but seems to work!
    if (!Constants.LINUX) {
      return true; // no detection
    }

    try {
      return spinsLinux(path);
    } catch (Exception exc) {
      // our crazy heuristics can easily trigger SecurityException, AIOOBE, etc ...
      return true;
    }
  }
  
  // following methods are package-private for testing ONLY
  
  // note: requires a real or fake linux filesystem!
  static boolean spinsLinux(Path path) throws IOException {
    FileStore store = getFileStore(path);
    
    // if fs type is tmpfs, it doesn't spin.
    // this won't have a corresponding block device
    if ("tmpfs".equals(store.type())) {
      return false;
    }
    
    // get block device name
    String devName = store.name();

    // not a device (e.g. NFS server)
    if (!devName.startsWith("/")) {
      return true;
    }
    
    // resolve any symlinks to real block device (e.g. LVM)
    // /dev/sda0 -> sda0
    // /devices/XXX -> sda0
    devName = path.getRoot().resolve(devName).toRealPath().getFileName().toString();
  
    // now try to find the longest matching device folder in /sys/block
    // (that starts with our dev name):
    Path sysinfo = path.getRoot().resolve("sys").resolve("block");
    Path devsysinfo = null;
    int matchlen = 0;
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(sysinfo)) {
      for (Path device : stream) {
        String name = device.getFileName().toString();
        if (name.length() > matchlen && devName.startsWith(name)) {
          devsysinfo = device;
          matchlen = name.length();
        }
      }
    }
    
    if (devsysinfo == null) {
      return true; // give up
    }
    
    // read first byte from rotational, it's a 1 if it spins.
    Path rotational = devsysinfo.resolve("queue").resolve("rotational");
    try (InputStream stream = Files.newInputStream(rotational)) {
      return stream.read() == '1'; 
    }
  }
  
  // Files.getFileStore(Path) useless here!
  // don't complain, just try it yourself
  static FileStore getFileStore(Path path) throws IOException {
    FileStore store = Files.getFileStore(path);
    String mount = getMountPoint(store);

    // find the "matching" FileStore from system list, it's the one we want, but only return
    // that if it's unambiguous (only one matching):
    FileStore sameMountPoint = null;
    for (FileStore fs : path.getFileSystem().getFileStores()) {
      if (mount.equals(getMountPoint(fs))) {
        if (sameMountPoint == null) {
          sameMountPoint = fs;
        } else {
          // more than one filesystem has the same mount point; something is wrong!
          // fall back to crappy one we got from Files.getFileStore
          return store;
        }
      }
    }

    if (sameMountPoint != null) {
      // ok, we found only one, use it:
      return sameMountPoint;
    } else {
      // fall back to crappy one we got from Files.getFileStore
      return store;    
    }
  }
  
  // these are hacks that are not guaranteed, may change across JVM versions, etc.
  static String getMountPoint(FileStore store) {
    String desc = store.toString();
    int index = desc.lastIndexOf(" (");
    if (index != -1) {
      return desc.substring(0, index);
    } else {
      return desc;
    }
  }

  /**
   * Returns the second throwable if the first is null otherwise adds the second as suppressed to the first
   * and returns it.
   */
  public static <T extends Throwable> T useOrSuppress(T first, T second) {
    if (first == null) {
      return second;
    } else {
      first.addSuppressed(second);
    }
    return first;
  }

  /**
   * Applies the consumer to all non-null elements in the collection even if an exception is thrown. The first exception
   * thrown by the consumer is re-thrown and subsequent exceptions are suppressed.
   */
  public static <T> void applyToAll(Collection<T> collection, IOConsumer<T> consumer) throws IOException {
    IOUtils.close(collection.stream().filter(Objects::nonNull).map(t -> (Closeable) () -> consumer.accept(t))::iterator);
  }

  /**
   * An IO operation with a single input.
   * @see java.util.function.Consumer
   */
  @FunctionalInterface
  public interface IOConsumer<T> {
    /**
     * Performs this operation on the given argument.
     */
    void accept(T input) throws IOException;
  }

  /**
   * A Function that may throw an IOException
   * @see java.util.function.Function
   */
  @FunctionalInterface
  public interface IOFunction<T, R> {
    R apply(T t) throws IOException;
  }

}
