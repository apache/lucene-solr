package org.apache.lucene.server;

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
import java.io.Reader;
import java.io.Writer;
import java.util.LinkedList;

class Pipe {

  private final LinkedList<char[]> pending = new LinkedList<char[]>();
  volatile boolean closed;
  private int readFrom;
  private long pendingBytes;
  //private long readCount;

  private final long maxBytesBuffered;;

  public Pipe(float maxMBBuffer) {
    this.maxBytesBuffered = (long) (maxMBBuffer*1024*1024);
  }

  public Reader getReader() {
    return new Reader() {
      char[] buffer;
      int nextRead;

      @Override
      public int read() throws IOException {
        //System.out.println("pipe: read");
        if (buffer == null || readFrom == buffer.length) {
          buffer = get();
          if (buffer == null) {
            return -1;
          }
          readFrom = 0;
        }
        //readCount++;
        //if (readCount % 10 == 0) {
        //System.out.println("r: " + readCount + " " + buffer[readFrom]);
        //}
        //System.out.print(buffer[readFrom]);
        return buffer[readFrom++];
      }

      @Override
      public int read(char[] ret, int offset, int length) throws IOException {
        //System.out.println("RB: offset=" + offset + " len=" + length);
        if (buffer == null) {
          buffer = get();
          if (buffer == null) {
            return 0;
          }
          nextRead = 0;
        }

        int left = buffer.length - nextRead;
        //System.out.println("r: buffer.length=" + buffer.length + " left=" + left);
        if (left <= length) {
          System.arraycopy(buffer, nextRead, ret, offset, left);
          buffer = null;
          return left;
        } else {
          System.arraycopy(buffer, nextRead, ret, offset, length);
          nextRead += length;
          return length;
        }
      }

      @Override
      public void close() {
      }
    };
  }

  synchronized void add(char[] buffer) throws IOException {
    //System.out.println("w: add " + buffer.length);
    pendingBytes += buffer.length;
    boolean didWait = false;
    while (pendingBytes > maxBytesBuffered && !closed) {
      //System.out.println("w: wait: " + pendingBytes + " closed=" + closed);
      didWait = true;
      try {
        wait();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IOException(ie);
      }
    }
    if (didWait) {
      //System.out.println("w: resume: " + pendingBytes);
    }
    if (closed) {
      throw new IllegalStateException("already closed");
    }
    pending.add(buffer);
    notifyAll();
  }

  synchronized void close() {
    closed = true;
    notifyAll();
  }

  synchronized char[] get() throws IOException {
    //System.out.println("r: get");
    if (pending.isEmpty()) {
      if (closed) {
        return null;
      }
      try {
        //System.out.println("r: wait");
        wait();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IOException(ie);
      }
    }
    char[] buffer = pending.removeFirst();
    pendingBytes -= buffer.length;
    notifyAll();
    //System.out.println("r: ret " + buffer.length);
    return buffer;
  }

  public Writer getWriter() {
    return new Writer() {
      @Override
      public void write(char[] buffer, int offset, int length) throws IOException {
        if (length <= 0) {
          throw new IllegalArgumentException();
        }
        assert offset == 0 && length == buffer.length;
        /*
        if (offset != 0 || length != buffer.length) {
          System.out.println("w: change buffer: offset=" + offset);
          char[] newBuffer = new char[length];
          System.arraycopy(buffer, offset, newBuffer, 0, length);
          buffer = newBuffer;
        }
        */
        add(buffer);
      }

      @Override
      public void close() {
        Pipe.this.closed = true;
      }

      @Override
      public void flush() {
      }
    };
  }
}
