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
package org.apache.lucene.benchmark.byTask.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

/** Stream utilities. */
public class StreamUtils {

  /** Buffer size used across the benchmark package */
  public static final int BUFFER_SIZE = 1 << 16; // 64K

  /** File format type */
  public enum Type {
    /** BZIP2 is automatically used for <b>.bz2</b> and <b>.bzip2</b> extensions. */
    BZIP2(CompressorStreamFactory.BZIP2),
    /** GZIP is automatically used for <b>.gz</b> and <b>.gzip</b> extensions. */
    GZIP(CompressorStreamFactory.GZIP),
    /** Plain text is used for anything which is not GZIP or BZIP. */
    PLAIN(null);
    private final String csfType;

    Type(String csfType) {
      this.csfType = csfType;
    }

    private InputStream inputStream(InputStream in) throws IOException {
      try {
        return csfType == null
            ? in
            : new CompressorStreamFactory().createCompressorInputStream(csfType, in);
      } catch (CompressorException e) {
        throw new IOException(e.getMessage(), e);
      }
    }

    private OutputStream outputStream(OutputStream os) throws IOException {
      try {
        return csfType == null
            ? os
            : new CompressorStreamFactory().createCompressorOutputStream(csfType, os);
      } catch (CompressorException e) {
        throw new IOException(e.getMessage(), e);
      }
    }
  }

  private static final Map<String, Type> extensionToType = new HashMap<>();

  static {
    // these in are lower case, we will lower case at the test as well
    extensionToType.put(".bz2", Type.BZIP2);
    extensionToType.put(".bzip", Type.BZIP2);
    extensionToType.put(".gz", Type.GZIP);
    extensionToType.put(".gzip", Type.GZIP);
  }

  /**
   * Returns an {@link InputStream} over the requested file. This method attempts to identify the
   * appropriate {@link InputStream} instance to return based on the file name (e.g., if it ends
   * with .bz2 or .bzip, return a 'bzip' {@link InputStream}).
   */
  public static InputStream inputStream(Path file) throws IOException {
    // First, create a FileInputStream, as this will be required by all types.
    // Wrap with BufferedInputStream for better performance
    InputStream in = new BufferedInputStream(Files.newInputStream(file), BUFFER_SIZE);
    return fileType(file).inputStream(in);
  }

  /** Return the type of the file, or null if unknown */
  private static Type fileType(Path file) {
    Type type = null;
    String fileName = file.getFileName().toString();
    int idx = fileName.lastIndexOf('.');
    if (idx != -1) {
      type = extensionToType.get(fileName.substring(idx).toLowerCase(Locale.ROOT));
    }
    return type == null ? Type.PLAIN : type;
  }

  /**
   * Returns an {@link OutputStream} over the requested file, identifying the appropriate {@link
   * OutputStream} instance similar to {@link #inputStream(Path)}.
   */
  public static OutputStream outputStream(Path file) throws IOException {
    // First, create a FileInputStream, as this will be required by all types.
    // Wrap with BufferedInputStream for better performance
    OutputStream os = new BufferedOutputStream(Files.newOutputStream(file), BUFFER_SIZE);
    return fileType(file).outputStream(os);
  }
}
