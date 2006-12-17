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

/** Base implementation class for buffered {@link IndexInput}. */
public abstract class BufferedIndexInput extends IndexInput {
  static final int BUFFER_SIZE = BufferedIndexOutput.BUFFER_SIZE;

  private byte[] buffer;

  private long bufferStart = 0;			  // position in file of buffer
  private int bufferLength = 0;			  // end of valid bytes
  private int bufferPosition = 0;		  // next byte to read

  public byte readByte() throws IOException {
    if (bufferPosition >= bufferLength)
      refill();
    return buffer[bufferPosition++];
  }

  public void readBytes(byte[] b, int offset, int len) throws IOException {
    if(len <= (bufferLength-bufferPosition)){
      // the buffer contains enough data to satistfy this request
      if(len>0) // to allow b to be null if len is 0...
        System.arraycopy(buffer, bufferPosition, b, offset, len);
      bufferPosition+=len;
    } else {
      // the buffer does not have enough data. First serve all we've got.
      int available = bufferLength - bufferPosition;
      if(available > 0){
        System.arraycopy(buffer, bufferPosition, b, offset, available);
        offset += available;
        len -= available;
        bufferPosition += available;
      }
      // and now, read the remaining 'len' bytes:
      if(len<BUFFER_SIZE){
        // If the amount left to read is small enough, do it in the usual
        // buffered way: fill the buffer and copy from it:
        refill();
        if(bufferLength<len){
          // Throw an exception when refill() could not read len bytes:
          System.arraycopy(buffer, 0, b, offset, bufferLength);
          throw new IOException("read past EOF");
        } else {
          System.arraycopy(buffer, 0, b, offset, len);
          bufferPosition=len;
        }
      } else {
        // The amount left to read is larger than the buffer - there's no
        // performance reason not to read it all at once. Note that unlike
        // the previous code of this function, there is no need to do a seek
        // here, because there's no need to reread what we had in the buffer.
        long after = bufferStart+bufferPosition+len;
        if(after > length())
          throw new IOException("read past EOF");
        readInternal(b, offset, len);
        bufferStart = after;
        bufferPosition = 0;
        bufferLength = 0;                    // trigger refill() on read
      }
    }
  }

  private void refill() throws IOException {
    long start = bufferStart + bufferPosition;
    long end = start + BUFFER_SIZE;
    if (end > length())				  // don't read past EOF
      end = length();
    bufferLength = (int)(end - start);
    if (bufferLength <= 0)
      throw new IOException("read past EOF");

    if (buffer == null)
      buffer = new byte[BUFFER_SIZE];		  // allocate buffer lazily
    readInternal(buffer, 0, bufferLength);

    bufferStart = start;
    bufferPosition = 0;
  }

  /** Expert: implements buffer refill.  Reads bytes from the current position
   * in the input.
   * @param b the array to read bytes into
   * @param offset the offset in the array to start storing bytes
   * @param length the number of bytes to read
   */
  protected abstract void readInternal(byte[] b, int offset, int length)
          throws IOException;

  public long getFilePointer() { return bufferStart + bufferPosition; }

  public void seek(long pos) throws IOException {
    if (pos >= bufferStart && pos < (bufferStart + bufferLength))
      bufferPosition = (int)(pos - bufferStart);  // seek within buffer
    else {
      bufferStart = pos;
      bufferPosition = 0;
      bufferLength = 0;				  // trigger refill() on read()
      seekInternal(pos);
    }
  }

  /** Expert: implements seek.  Sets current position in this file, where the
   * next {@link #readInternal(byte[],int,int)} will occur.
   * @see #readInternal(byte[],int,int)
   */
  protected abstract void seekInternal(long pos) throws IOException;

  public Object clone() {
    BufferedIndexInput clone = (BufferedIndexInput)super.clone();

    if (buffer != null) {
      clone.buffer = new byte[BUFFER_SIZE];
      System.arraycopy(buffer, 0, clone.buffer, 0, bufferLength);
    }

    return clone;
  }

}
