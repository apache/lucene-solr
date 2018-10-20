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
package org.apache.lucene.mockfile;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;

/**  
 * A {@code FilterDirectoryStream} contains another 
 * {@code DirectoryStream}, which it uses as its basic 
 * source of data, possibly transforming the data along the 
 * way or providing additional functionality. 
 */
public class FilterDirectoryStream implements DirectoryStream<Path> {
  
  /** 
   * The underlying {@code DirectoryStream} instance. 
   */
  protected final DirectoryStream<Path> delegate;
  
  /**
   * The underlying {@code FileSystem} instance.
   */
  protected final FileSystem fileSystem;

  /**
   * Construct a {@code FilterDirectoryStream} based on 
   * the specified base stream.
   * <p>
   * Note that base stream is closed if this stream is closed.
   * @param delegate specified base stream.
   */
  public FilterDirectoryStream(DirectoryStream<Path> delegate, FileSystem fileSystem) {
    this.delegate = Objects.requireNonNull(delegate);
    this.fileSystem = Objects.requireNonNull(fileSystem);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public Iterator<Path> iterator() {
    final Iterator<Path> delegateIterator = delegate.iterator();
    return new Iterator<Path>() {
      @Override
      public boolean hasNext() {
        return delegateIterator.hasNext();
      }
      @Override
      public Path next() {
        return new FilterPath(delegateIterator.next(), fileSystem);
      }
      @Override
      public void remove() {
        delegateIterator.remove();
      }
    };
  }
}
