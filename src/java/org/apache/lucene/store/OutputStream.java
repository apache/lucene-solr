package org.apache.lucene.store;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.IOException;

/**
  Abstract class for output from a file in a Directory.
  @author Doug Cutting
*/

/** A random-access output stream */
abstract public class OutputStream {
  final static int BUFFER_SIZE = 1024;

  private final byte[] buffer = new byte[BUFFER_SIZE];
  private long bufferStart = 0;			  // position in file of buffer
  private int bufferPosition = 0;		  // position in buffer

  /** OutputStream-like methods @see java.io.InputStream */
  public final void writeByte(byte b) throws IOException {
    if (bufferPosition >= BUFFER_SIZE)
      flush();
    buffer[bufferPosition++] = b;
  }

  public final void writeBytes(byte[] b, int length) throws IOException {
    for (int i = 0; i < length; i++)
      writeByte(b[i]);
  }

  public final void writeInt(int i) throws IOException {
    writeByte((byte)(i >> 24));
    writeByte((byte)(i >> 16));
    writeByte((byte)(i >>  8));
    writeByte((byte) i);
  }

  public final void writeVInt(int i) throws IOException {
    while ((i & ~0x7F) != 0) {
      writeByte((byte)((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeByte((byte)i);
  }

  public final void writeLong(long i) throws IOException {
    writeInt((int) (i >> 32));
    writeInt((int) i);
  }

  public final void writeVLong(long i) throws IOException {
    while ((i & ~0x7F) != 0) {
      writeByte((byte)((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeByte((byte)i);
  }

  public final void writeString(String s) throws IOException {
    int length = s.length();
    writeVInt(length);
    writeChars(s, 0, length);
  }

  public final void writeChars(String s, int start, int length)
       throws IOException {
    final int end = start + length;
    for (int i = start; i < end; i++) {
      final int code = (int)s.charAt(i);
      if (code >= 0x01 && code <= 0x7F)
	writeByte((byte)code);
      else if (((code >= 0x80) && (code <= 0x7FF)) || code == 0) {
	writeByte((byte)(0xC0 | (code >> 6)));
	writeByte((byte)(0x80 | (code & 0x3F)));
      } else {
	writeByte((byte)(0xE0 | (code >>> 12)));
	writeByte((byte)(0x80 | ((code >> 6) & 0x3F)));
	writeByte((byte)(0x80 | (code & 0x3F)));
      }
    }
  }

  protected final void flush() throws IOException {
    flushBuffer(buffer, bufferPosition);
    bufferStart += bufferPosition;
    bufferPosition = 0;
  }

  abstract protected void flushBuffer(byte[] b, int len) throws IOException;

  public void close() throws IOException {
    flush();
  }

  /** RandomAccessFile-like methods @see java.io.RandomAccessFile */
  public final long getFilePointer() throws IOException {
    return bufferStart + bufferPosition;
  }

  public void seek(long pos) throws IOException {
    flush();
    bufferStart = pos;
  }

  abstract public long length() throws IOException;


}
