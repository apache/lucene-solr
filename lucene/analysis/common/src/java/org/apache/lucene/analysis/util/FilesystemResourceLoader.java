package org.apache.lucene.analysis.util;

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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Simple {@link ResourceLoader} that opens resource files
 * from the local file system, optionally resolving against
 * a base directory.
 * 
 * <p>This loader wraps a delegate {@link ResourceLoader}
 * that is used to resolve all files, the current base directory
 * does not contain. {@link #newInstance} is always resolved
 * against the delegate, as a {@link ClassLoader} is needed.
 * 
 * <p>You can chain several {@code FilesystemResourceLoader}s
 * to allow lookup of files in more than one base directory.
 */
public final class FilesystemResourceLoader implements ResourceLoader {
  private final File baseDirectory;
  private final ResourceLoader delegate;
  
  /**
   * Creates a resource loader that requires absolute filenames or relative to CWD
   * to resolve resources. Files not found in file system and class lookups
   * are delegated to context classloader.
   */
  public FilesystemResourceLoader() {
    this((File) null);
  }

  /**
   * Creates a resource loader that resolves resources against the given
   * base directory (may be {@code null} to refer to CWD).
   * Files not found in file system and class lookups are delegated to context
   * classloader.
   */
  public FilesystemResourceLoader(File baseDirectory) {
    this(baseDirectory, new ClasspathResourceLoader());
  }

  /**
   * Creates a resource loader that resolves resources against the given
   * base directory (may be {@code null} to refer to CWD).
   * Files not found in file system and class lookups are delegated
   * to the given delegate {@link ResourceLoader}.
   */
  public FilesystemResourceLoader(File baseDirectory, ResourceLoader delegate) {
    if (baseDirectory != null && !baseDirectory.isDirectory())
      throw new IllegalArgumentException("baseDirectory is not a directory or null");
    if (delegate == null)
      throw new IllegalArgumentException("delegate ResourceLoader may not be null");
    this.baseDirectory = baseDirectory;
    this.delegate = delegate;
  }

  @Override
  public InputStream openResource(String resource) throws IOException {
    try {
      File file = new File (resource);
      if (baseDirectory != null && !file.isAbsolute()) {
        file = new File(baseDirectory, resource);
      }
      return new FileInputStream(file);
    } catch (FileNotFoundException fnfe) {
      return delegate.openResource(resource);
    }
  }

  @Override
  public <T> T newInstance(String cname, Class<T> expectedType) {
    return delegate.newInstance(cname, expectedType);
  }

  @Override
  public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
    return delegate.findClass(cname, expectedType);
  }
}
