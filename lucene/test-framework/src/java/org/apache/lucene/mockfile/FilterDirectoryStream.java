package org.apache.lucene.mockfile;

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
import java.nio.file.DirectoryStream;
import java.util.Iterator;
import java.util.Objects;

/**  
 * A {@code FilterDirectoryStream} contains another 
 * {@code DirectoryStream}, which it uses as its basic 
 * source of data, possibly transforming the data along the 
 * way or providing additional functionality. 
 */
public class FilterDirectoryStream<T> implements DirectoryStream<T> {
  
  /** 
   * The underlying {@code DirectoryStream} instance. 
   */
  protected final DirectoryStream<T> delegate;
  
  /**
   * Construct a {@code FilterDirectoryStream} based on 
   * the specified base stream.
   * <p>
   * Note that base stream is closed if this stream is closed.
   * @param delegate specified base stream.
   */
  public FilterDirectoryStream(DirectoryStream<T> delegate) {
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public Iterator<T> iterator() {
    return delegate.iterator();
  }
}
