package org.apache.lucene.store;

/**
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

import java.io.Closeable;
import java.io.IOException;

/** Abstract base class for output to a file in a Directory.  A random-access
 * output stream.  Used for all Lucene index output operations.
 * @see Directory
 * @see IndexInput
 */
public abstract class IndexOutput extends DataOutput implements Closeable {

  /** Forces any buffered output to be written. */
  public abstract void flush() throws IOException;

  /** Closes this stream to further operations. */
  public abstract void close() throws IOException;

  /** Returns the current position in this file, where the next write will
   * occur.
   * @see #seek(long)
   */
  public abstract long getFilePointer();

  /** Sets current position in this file, where the next write will occur.
   * @see #getFilePointer()
   */
  public abstract void seek(long pos) throws IOException;

  /** The number of bytes in the file. */
  public abstract long length() throws IOException;

  /** Set the file length. By default, this method does
   * nothing (it's optional for a Directory to implement
   * it).  But, certain Directory implementations (for
   * example @see FSDirectory) can use this to inform the
   * underlying IO system to pre-allocate the file to the
   * specified size.  If the length is longer than the
   * current file length, the bytes added to the file are
   * undefined.  Otherwise the file is truncated.
   * @param length file length
   */
  public void setLength(long length) throws IOException {}

}
