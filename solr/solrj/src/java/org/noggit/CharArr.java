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

package org.noggit;


import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.CharBuffer;

public class CharArr implements CharSequence, Appendable {
  protected char[] buf;
  protected int start;
  protected int end;

  public CharArr() {
    this(32);
  }

  public CharArr(int size) {
    buf = new char[size];
  }

  public CharArr(char[] arr, int start, int end) {
    set(arr, start, end);
  }

  public void setStart(int start) {
    this.start = start;
  }

  public void setEnd(int end) {
    this.end = end;
  }

  public void set(char[] arr, int start, int end) {
    this.buf = arr;
    this.start = start;
    this.end = end;
  }

  public char[] getArray() {
    return buf;
  }

  public int getStart() {
    return start;
  }

  public int getEnd() {
    return end;
  }

  public int size() {
    return end - start;
  }

  @Override
  public int length() {
    return size();
  }

  /**
   * The capacity of the buffer when empty (getArray().size())
   */
  public int capacity() {
    return buf.length;
  }


  @Override
  public char charAt(int index) {
    return buf[start + index];
  }

  @Override
  public CharArr subSequence(int start, int end) {
    return new CharArr(buf, this.start + start, this.start + end);
  }

  public int read() throws IOException {
    if (start >= end) return -1;
    return buf[start++];
  }

  public int read(char cbuf[], int off, int len) {
    //TODO
    return 0;
  }

  public void unsafeWrite(char b) {
    buf[end++] = b;
  }

  public void unsafeWrite(int b) {
    unsafeWrite((char) b);
  }

  public void unsafeWrite(char b[], int off, int len) {
    System.arraycopy(b, off, buf, end, len);
    end += len;
  }

  protected void resize(int len) {
    char newbuf[] = new char[Math.max(buf.length << 1, len)];
    System.arraycopy(buf, start, newbuf, 0, size());
    buf = newbuf;
  }

  public void reserve(int num) {
    if (end + num > buf.length) resize(end + num);
  }

  public void write(char b) {
    if (end >= buf.length) {
      resize(end + 1);
    }
    unsafeWrite(b);
  }

  public final void write(int b) {
    write((char) b);
  }

  public final void write(char[] b) {
    write(b, 0, b.length);
  }

  public void write(char b[], int off, int len) {
    reserve(len);
    unsafeWrite(b, off, len);
  }

  public final void write(CharArr arr) {
    write(arr.buf, arr.start, arr.end - arr.start);
  }

  public final void write(String s) {
    write(s, 0, s.length());
  }

  public void write(String s, int stringOffset, int len) {
    reserve(len);
    s.getChars(stringOffset, len, buf, end);
    end += len;
  }

  public void flush() {
  }

  public final void reset() {
    start = end = 0;
  }

  public void close() {
  }

  public char[] toCharArray() {
    char newbuf[] = new char[size()];
    System.arraycopy(buf, start, newbuf, 0, size());
    return newbuf;
  }


  @Override
  public String toString() {
    return new String(buf, start, size());
  }

  public int read(CharBuffer cb) throws IOException {

    /***
     int sz = size();
     if (sz<=0) return -1;
     if (sz>0) cb.put(buf, start, sz);
     return -1;
     ***/

    int sz = size();
    if (sz > 0) cb.put(buf, start, sz);
    start = end;
    while (true) {
      fill();
      int s = size();
      if (s == 0) return sz == 0 ? -1 : sz;
      sz += s;
      cb.put(buf, start, s);
    }
  }


  public int fill() throws IOException {
    return 0;  // or -1?
  }

  //////////////// Appendable methods /////////////
  @Override
  public final Appendable append(CharSequence csq) throws IOException {
    return append(csq, 0, csq.length());
  }

  @Override
  public Appendable append(CharSequence csq, int start, int end) throws IOException {
    write(csq.subSequence(start, end).toString());
    return null;
  }

  @Override
  public final Appendable append(char c) throws IOException {
    write(c);
    return this;
  }


  static class NullCharArr extends CharArr {
    public NullCharArr() {
      super(new char[1], 0, 0);
    }

    @Override
    public void unsafeWrite(char b) {
    }

    @Override
    public void unsafeWrite(char b[], int off, int len) {
    }

    @Override
    public void unsafeWrite(int b) {
    }

    @Override
    public void write(char b) {
    }

    @Override
    public void write(char b[], int off, int len) {
    }

    @Override
    public void reserve(int num) {
    }

    @Override
    protected void resize(int len) {
    }

    @Override
    public Appendable append(CharSequence csq, int start, int end) throws IOException {
      return this;
    }

    @Override
    public char charAt(int index) {
      return 0;
    }

    @Override
    public void write(String s, int stringOffset, int len) {
    }
  }


  // IDEA: a subclass that refills the array from a reader?
  class CharArrReader extends CharArr {
    protected final Reader in;

    public CharArrReader(Reader in, int size) {
      super(size);
      this.in = in;
    }

    @Override
    public int read() throws IOException {
      if (start >= end) fill();
      return start >= end ? -1 : buf[start++];
    }

    @Override
    public int read(CharBuffer cb) throws IOException {
      // empty the buffer and then read direct
      int sz = size();
      if (sz > 0) cb.put(buf, start, end);
      int sz2 = in.read(cb);
      if (sz2 >= 0) return sz + sz2;
      return sz > 0 ? sz : -1;
    }

    @Override
    public int fill() throws IOException {
      if (start >= end) {
        reset();
      } else if (start > 0) {
        System.arraycopy(buf, start, buf, 0, size());
        end = size();
        start = 0;
      }
      /***
       // fill fully or not???
       do {
       int sz = in.read(buf,end,buf.length-end);
       if (sz==-1) return;
       end+=sz;
       } while (end < buf.length);
       ***/

      int sz = in.read(buf, end, buf.length - end);
      if (sz > 0) end += sz;
      return sz;
    }

  }


  class CharArrWriter extends CharArr {
    protected Writer sink;

    @Override
    public void flush() {
      try {
        sink.write(buf, start, end - start);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      start = end = 0;
    }

    @Override
    public void write(char b) {
      if (end >= buf.length) {
        flush();
      }
      unsafeWrite(b);
    }

    @Override
    public void write(char b[], int off, int len) {
      int space = buf.length - end;
      if (len < space) {
        unsafeWrite(b, off, len);
      } else if (len < buf.length) {
        unsafeWrite(b, off, space);
        flush();
        unsafeWrite(b, off + space, len - space);
      } else {
        flush();
        try {
          sink.write(b, off, len);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public void write(String s, int stringOffset, int len) {
      int space = buf.length - end;
      if (len < space) {
        s.getChars(stringOffset, stringOffset + len, buf, end);
        end += len;
      } else if (len < buf.length) {
        // if the data to write is small enough, buffer it.
        s.getChars(stringOffset, stringOffset + space, buf, end);
        flush();
        s.getChars(stringOffset + space, stringOffset + len, buf, 0);
        end = len - space;
      } else {
        flush();
        // don't buffer, just write to sink
        try {
          sink.write(s, stringOffset, len);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

      }
    }
  }
}
