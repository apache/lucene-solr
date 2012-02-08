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

import java.io.IOException;
import java.io.Closeable;

/** Abstract base class for input from a file in a {@link Directory}.  A
 * random-access input stream.  Used for all Lucene index input operations.
 * @see Directory
 */
public abstract class IndexInput extends DataInput implements Cloneable,Closeable {

  /**
   * Expert
   * 
   * Similar to {@link #readChars(char[], int, int)} but does not do any conversion operations on the bytes it is reading in.  It still
   * has to invoke {@link #readByte()} just as {@link #readChars(char[], int, int)} does, but it does not need a buffer to store anything
   * and it does not have to do any of the bitwise operations, since we don't actually care what is in the byte except to determine
   * how many more bytes to read
   * @param length The number of chars to read
   * @deprecated this method operates on old "modified utf8" encoded
   *             strings
   */
  @Deprecated
  public void skipChars(int length) throws IOException{
    for (int i = 0; i < length; i++) {
      byte b = readByte();
      if ((b & 0x80) == 0){
        //do nothing, we only need one byte
      } else if ((b & 0xE0) != 0xE0) {
        readByte();//read an additional byte
      } else {      
        //read two additional bytes.
        readByte();
        readByte();
      }
    }
  }

  private final String resourceDescription;

  /** @deprecated please pass resourceDescription */
  @Deprecated
  protected IndexInput() {
    this("anonymous IndexInput");
  }

  /** resourceDescription should be a non-null, opaque string
   *  describing this resource; it's returned from
   *  {@link #toString}. */
  protected IndexInput(String resourceDescription) {
    if (resourceDescription == null) {
      throw new IllegalArgumentException("resourceDescription must not be null");
    }
    this.resourceDescription = resourceDescription;
  }

  /** Closes the stream to further operations. */
  public abstract void close() throws IOException;

  /** Returns the current position in this file, where the next read will
   * occur.
   * @see #seek(long)
   */
  public abstract long getFilePointer();

  /** Sets current position in this file, where the next read will occur.
   * @see #getFilePointer()
   */
  public abstract void seek(long pos) throws IOException;

  /** The number of bytes in the file. */
  public abstract long length();

  /**
   * Copies <code>numBytes</code> bytes to the given {@link IndexOutput}.
   * <p>
   * <b>NOTE:</b> this method uses an intermediate buffer to copy the bytes.
   * Consider overriding it in your implementation, if you can make a better,
   * optimized copy.
   * <p>
   * <b>NOTE</b> ensure that there are enough bytes in the input to copy to
   * output. Otherwise, different exceptions may be thrown, depending on the
   * implementation.
   */
  public void copyBytes(IndexOutput out, long numBytes) throws IOException {
    assert numBytes >= 0: "numBytes=" + numBytes;

    byte copyBuf[] = new byte[BufferedIndexInput.BUFFER_SIZE];

    while (numBytes > 0) {
      final int toCopy = (int) (numBytes > copyBuf.length ? copyBuf.length : numBytes);
      readBytes(copyBuf, 0, toCopy);
      out.writeBytes(copyBuf, 0, toCopy);
      numBytes -= toCopy;
    }
  }

  @Override
  public String toString() {
    return resourceDescription;
  }
}
