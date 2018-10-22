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
package org.apache.solr.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

/**
 *
 * @since solr 1.2
 */
public interface ContentStream {
  String getName();
  String getSourceInfo();
  String getContentType();
  
  /**
   * @return the stream size or <code>null</code> if not known
   */
  Long getSize(); // size if we know it, otherwise null
  
  /**
   * Get an open stream.  You are responsible for closing it.  Consider using 
   * something like:
   * <pre>
   *   InputStream stream = stream.getStream();
   *   try {
   *     // use the stream...
   *   }
   *   finally {
   *     IOUtils.closeQuietly(stream);
   *   }
   *  </pre>
   *  
   * Only the first call to <code>getStream()</code> or <code>getReader()</code>
   * is guaranteed to work.  The runtime behavior for additional calls is undefined.
   *
   * Note: you must call <code>getStream()</code> or <code>getReader()</code> before
   * the attributes (name, contentType, etc) are guaranteed to be set.  Streams may be
   * lazy loaded only when this method is called.
   */
  InputStream getStream() throws IOException;

  /**
   * Get an open stream.  You are responsible for closing it.  Consider using 
   * something like:
   * <pre>
   *   Reader reader = stream.getReader();
   *   try {
   *     // use the reader...
   *   }
   *   finally {
   *     IOUtils.closeQuietly(reader);
   *   }
   *  </pre>
   *  
   * Only the first call to <code>getStream()</code> or <code>getReader()</code>
   * is guaranteed to work.  The runtime behavior for additional calls is undefined.
   *
   * Note: you must call <code>getStream()</code> or <code>getReader()</code> before
   * the attributes (name, contentType, etc) are guaranteed to be set.  Streams may be
   * lazy loaded only when this method is called.
   */
  Reader getReader() throws IOException;
}
