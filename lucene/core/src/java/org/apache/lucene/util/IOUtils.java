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

import org.apache.lucene.store.Directory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;

/** This class emulates the new Java 7 "Try-With-Resources" statement.
 * Remove once Lucene is on Java 7.
 * @lucene.internal */
public final class IOUtils {
  
  /**
   * UTF-8 {@link Charset} instance to prevent repeated
   * {@link Charset#forName(String)} lookups
   * @deprecated Use {@link StandardCharsets#UTF_8} instead.
   */
  @Deprecated
  public static final Charset CHARSET_UTF_8 = StandardCharsets.UTF_8;
  
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
    Throwable th = null;

    for (Closeable object : objects) {
      try {
        if (object != null) {
          object.close();
        }
      } catch (Throwable t) {
        addSuppressed(th, t);
        if (th == null) {
          th = t;
        }
      }
    }

    reThrow(th);
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
        addSuppressed(th, t);
        if (th == null) {
          th = t;
        }
      }
    }

    reThrow(th);
  }

  /**
   * Closes all given <tt>Closeable</tt>s, suppressing all thrown exceptions.
   * Some of the <tt>Closeable</tt>s may be null, they are ignored.
   * 
   * @param objects
   *          objects to call <tt>close()</tt> on
   */
  public static void closeWhileHandlingException(Closeable... objects) {
    for (Closeable object : objects) {
      try {
        if (object != null) {
          object.close();
        }
      } catch (Throwable t) {
      }
    }
  }
  
  /**
   * Closes all given <tt>Closeable</tt>s, suppressing all thrown exceptions.
   * @see #closeWhileHandlingException(Closeable...)
   */
  public static void closeWhileHandlingException(Iterable<? extends Closeable> objects) {
    for (Closeable object : objects) {
      try {
        if (object != null) {
          object.close();
        }
      } catch (Throwable t) {
      }
    }
  }
  
  /** adds a Throwable to the list of suppressed Exceptions of the first Throwable
   * @param exception this exception should get the suppressed one added
   * @param suppressed the suppressed exception
   */
  private static void addSuppressed(Throwable exception, Throwable suppressed) {
    if (exception != null && suppressed != null) {
      exception.addSuppressed(suppressed);
    }
  }
  
  /**
   * Wrapping the given {@link InputStream} in a reader using a {@link CharsetDecoder}.
   * Unlike Java's defaults this reader will throw an exception if your it detects 
   * the read charset doesn't match the expected {@link Charset}. 
   * <p>
   * Decoding readers are useful to load configuration files, stopword lists or synonym files
   * to detect character set problems. However, its not recommended to use as a common purpose 
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
   * Opens a Reader for the given {@link File} using a {@link CharsetDecoder}.
   * Unlike Java's defaults this reader will throw an exception if your it detects 
   * the read charset doesn't match the expected {@link Charset}. 
   * <p>
   * Decoding readers are useful to load configuration files, stopword lists or synonym files
   * to detect character set problems. However, its not recommended to use as a common purpose 
   * reader.
   * @param file the file to open a reader on
   * @param charSet the expected charset
   * @return a reader to read the given file
   */
  public static Reader getDecodingReader(File file, Charset charSet) throws IOException {
    FileInputStream stream = null;
    boolean success = false;
    try {
      stream = new FileInputStream(file);
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
   * Opens a Reader for the given resource using a {@link CharsetDecoder}.
   * Unlike Java's defaults this reader will throw an exception if your it detects 
   * the read charset doesn't match the expected {@link Charset}. 
   * <p>
   * Decoding readers are useful to load configuration files, stopword lists or synonym files
   * to detect character set problems. However, its not recommended to use as a common purpose 
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
  public static void deleteFilesIgnoringExceptions(Directory dir, String... files) {
    for (String name : files) {
      try {
        dir.deleteFile(name);
      } catch (Throwable ignored) {
        // ignore
      }
    }
  }

  /**
   * Copy one file's contents to another file. The target will be overwritten
   * if it exists. The source must exist.
   */
  public static void copy(File source, File target) throws IOException {
    FileInputStream fis = null;
    FileOutputStream fos = null;
    try {
      fis = new FileInputStream(source);
      fos = new FileOutputStream(target);
      
      final byte [] buffer = new byte [1024 * 8];
      int len;
      while ((len = fis.read(buffer)) > 0) {
        fos.write(buffer, 0, len);
      }
    } finally {
      close(fis, fos);
    }
  }

  /**
   * Simple utilty method that takes a previously caught
   * {@code Throwable} and rethrows either {@code
   * IOException} or an unchecked exception.  If the
   * argument is null then this method does nothing.
   */
  public static void reThrow(Throwable th) throws IOException {
    if (th != null) {
      if (th instanceof IOException) {
        throw (IOException) th;
      }
      reThrowUnchecked(th);
    }
  }

  /**
   * Simple utilty method that takes a previously caught
   * {@code Throwable} and rethrows it as an unchecked exception.
   * If the argument is null then this method does nothing.
   */
  public static void reThrowUnchecked(Throwable th) {
    if (th != null) {
      if (th instanceof RuntimeException) {
        throw (RuntimeException) th;
      }
      if (th instanceof Error) {
        throw (Error) th;
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
  public static void fsync(File fileToSync, boolean isDir) throws IOException {
    IOException exc = null;
    
    // If the file is a directory we have to open read-only, for regular files we must open r/w for the fsync to have an effect.
    // See http://blog.httrack.com/blog/2013/11/15/everything-you-always-wanted-to-know-about-fsync/
    try (final FileChannel file = FileChannel.open(fileToSync.toPath(), isDir ? StandardOpenOption.READ : StandardOpenOption.WRITE)) {
      for (int retry = 0; retry < 5; retry++) {
        try {
          file.force(true);
          return;
        } catch (IOException ioe) {
          if (exc == null) {
            exc = ioe;
          }
          try {
            // Pause 5 msec
            Thread.sleep(5L);
          } catch (InterruptedException ie) {
            ThreadInterruptedException ex = new ThreadInterruptedException(ie);
            ex.addSuppressed(exc);
            throw ex;
          }
        }
      }
    } catch (IOException ioe) {
      if (exc == null) {
        exc = ioe;
      }
    }
    
    if (isDir) {
      assert (Constants.LINUX || Constants.MAC_OS_X) == false :
        "On Linux and MacOSX fsyncing a directory should not throw IOException, "+
        "we just don't want to rely on that in production (undocumented). Got: " + exc;
      // Ignore exception if it is a directory
      return;
    }
    
    // Throw original exception
    throw exc;
  }
}
