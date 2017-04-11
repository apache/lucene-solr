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

package org.apache.solr.util.modules;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.SolrResourceNotFoundException;

/**
 * Resource loader that also knows about plugins
 */
public class ModulesResourceLoader implements ResourceLoader, Closeable {

  private ModulesClassLoader modulesClassLoader;

  public ModulesResourceLoader(ModulesClassLoader modulesClassLoader) {
    this.modulesClassLoader = modulesClassLoader;
  }

  /**
   * Opens a named resource
   *
   * @param resource
   */
  @Override
  public InputStream openResource(String resource) throws IOException {
    InputStream is = modulesClassLoader.getResourceAsStream(resource.replace(File.separatorChar, '/'));
    if (is == null) {
      throw new SolrResourceNotFoundException("Can't find resource '" + resource + "' in any module");
    }
    return is;
  }

  /**
   * Finds class of the name and expected type
   *
   * @param cname
   * @param expectedType
   */
  @Override
  public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
    return null;
  }

  /**
   * Creates an instance of the name and expected type
   *
   * @param cname
   * @param expectedType
   */
  @Override
  public <T> T newInstance(String cname, Class<T> expectedType) {
    return null;
  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   * <p>
   * <p> As noted in {@link AutoCloseable#close()}, cases where the
   * close may fail require careful attention. It is strongly advised
   * to relinquish the underlying resources and to internally
   * <em>mark</em> the {@code Closeable} as closed, prior to throwing
   * the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {

  }
}
