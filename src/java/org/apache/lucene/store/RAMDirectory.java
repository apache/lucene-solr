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
import java.io.File;
import java.util.Vector;
import java.util.Hashtable;
import java.util.Enumeration;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.InputStream;
import org.apache.lucene.store.OutputStream;

/**
 * A memory-resident {@link Directory} implementation.
 *
 * @version $Id$
 */
public final class RAMDirectory extends Directory {
  Hashtable files = new Hashtable();

  /** Constructs an empty {@link Directory}. */
  public RAMDirectory() {
  }

  /**
   * Creates a new <code>RAMDirectory</code> instance from a different
   * <code>Directory</code> implementation.  This can be used to load
   * a disk-based index into memory.
   * <P>
   * This should be used only with indices that can fit into memory.
   *
   * @param dir a <code>Directory</code> value
   * @exception IOException if an error occurs
   */
  public RAMDirectory(Directory dir) throws IOException {
    final String[] ar = dir.list();
    for (int i = 0; i < ar.length; i++) {
      // make place on ram disk
      OutputStream os = createFile(ar[i]);
      // read current file
      InputStream is = dir.openFile(ar[i]);
      // and copy to ram disk
      int len = (int) is.length();
      byte[] buf = new byte[len];
      is.readBytes(buf, 0, len);
      os.writeBytes(buf, len);
      // graceful cleanup
      is.close();
      os.close();
    }
  }

  /**
   * Creates a new <code>RAMDirectory</code> instance from the {@link FSDirectory}.
   *
   * @param dir a <code>File</code> specifying the index directory
   */
  public RAMDirectory(File dir) throws IOException {
    this(FSDirectory.getDirectory(dir, false));
  }

  /**
   * Creates a new <code>RAMDirectory</code> instance from the {@link FSDirectory}.
   *
   * @param dir a <code>String</code> specifying the full index directory path
   */
  public RAMDirectory(String dir) throws IOException {
    this(FSDirectory.getDirectory(dir, false));
  }

  /** Returns an array of strings, one for each file in the directory. */
  public final String[] list() {
    String[] result = new String[files.size()];
    int i = 0;
    Enumeration names = files.keys();
    while (names.hasMoreElements())
      result[i++] = (String)names.nextElement();
    return result;
  }

  /** Returns true iff the named file exists in this directory. */
  public final boolean fileExists(String name) {
    RAMFile file = (RAMFile)files.get(name);
    return file != null;
  }

  /** Returns the time the named file was last modified. */
  public final long fileModified(String name) throws IOException {
    RAMFile file = (RAMFile)files.get(name);
    return file.lastModified;
  }

  /** Set the modified time of an existing file to now. */
  public void touchFile(String name) throws IOException {
    final boolean MONITOR = false;
    int count = 0;
    
    RAMFile file = (RAMFile)files.get(name);
    long ts2, ts1 = System.currentTimeMillis();
    do {
        try {
            Thread.sleep(0, 1);
        } catch (InterruptedException e) {}
        ts2 = System.currentTimeMillis();
        if (MONITOR) count ++;
    } while(ts1 == ts2);
    
    file.lastModified = ts2;

    if (MONITOR)
        System.out.println("SLEEP COUNT: " + count);        
  }

  /** Returns the length in bytes of a file in the directory. */
  public final long fileLength(String name) {
    RAMFile file = (RAMFile)files.get(name);
    return file.length;
  }

  /** Removes an existing file in the directory. */
  public final void deleteFile(String name) {
    files.remove(name);
  }

  /** Removes an existing file in the directory. */
  public final void renameFile(String from, String to) {
    RAMFile file = (RAMFile)files.get(from);
    files.remove(from);
    files.put(to, file);
  }

  /** Creates a new, empty file in the directory with the given name.
      Returns a stream writing this file. */
  public final OutputStream createFile(String name) {
    RAMFile file = new RAMFile();
    files.put(name, file);
    return new RAMOutputStream(file);
  }

  /** Returns a stream reading an existing file. */
  public final InputStream openFile(String name) {
    RAMFile file = (RAMFile)files.get(name);
    return new RAMInputStream(file);
  }

  /** Construct a {@link Lock}.
   * @param name the name of the lock file
   */
  public final Lock makeLock(final String name) {
    return new Lock() {
        public boolean obtain() throws IOException {
          synchronized (files) {
            if (!fileExists(name)) {
              createFile(name).close();
              return true;
            }
            return false;
          }
        }
        public void release() {
          deleteFile(name);
        }
        public boolean isLocked() {
          return fileExists(name);
        }
      };
  }

  /** Closes the store to future operations. */
  public final void close() {
  }
}


final class RAMInputStream extends InputStream implements Cloneable {
  RAMFile file;
  int pointer = 0;

  public RAMInputStream(RAMFile f) {
    file = f;
    length = file.length;
  }

  /** InputStream methods */
  public final void readInternal(byte[] dest, int destOffset, int len) {
    int remainder = len;
    int start = pointer;
    while (remainder != 0) {
      int bufferNumber = start/InputStream.BUFFER_SIZE;
      int bufferOffset = start%InputStream.BUFFER_SIZE;
      int bytesInBuffer = InputStream.BUFFER_SIZE - bufferOffset;
      int bytesToCopy = bytesInBuffer >= remainder ? remainder : bytesInBuffer;
      byte[] buffer = (byte[])file.buffers.elementAt(bufferNumber);
      System.arraycopy(buffer, bufferOffset, dest, destOffset, bytesToCopy);
      destOffset += bytesToCopy;
      start += bytesToCopy;
      remainder -= bytesToCopy;
    }
    pointer += len;
  }

  public final void close() {
  }

  /** Random-access methods */
  public final void seekInternal(long pos) {
    pointer = (int)pos;
  }
}


final class RAMOutputStream extends OutputStream {
  RAMFile file;
  int pointer = 0;

  public RAMOutputStream(RAMFile f) {
    file = f;
  }

  /** output methods: */
  public final void flushBuffer(byte[] src, int len) {
    int bufferNumber = pointer/OutputStream.BUFFER_SIZE;
    int bufferOffset = pointer%OutputStream.BUFFER_SIZE;
    int bytesInBuffer = OutputStream.BUFFER_SIZE - bufferOffset;
    int bytesToCopy = bytesInBuffer >= len ? len : bytesInBuffer;

    if (bufferNumber == file.buffers.size())
      file.buffers.addElement(new byte[OutputStream.BUFFER_SIZE]);

    byte[] buffer = (byte[])file.buffers.elementAt(bufferNumber);
    System.arraycopy(src, 0, buffer, bufferOffset, bytesToCopy);

    if (bytesToCopy < len) {			  // not all in one buffer
      int srcOffset = bytesToCopy;
      bytesToCopy = len - bytesToCopy;		  // remaining bytes
      bufferNumber++;
      if (bufferNumber == file.buffers.size())
        file.buffers.addElement(new byte[OutputStream.BUFFER_SIZE]);
      buffer = (byte[])file.buffers.elementAt(bufferNumber);
      System.arraycopy(src, srcOffset, buffer, 0, bytesToCopy);
    }
    pointer += len;
    if (pointer > file.length)
      file.length = pointer;

    file.lastModified = System.currentTimeMillis();
  }

  public final void close() throws IOException {
    super.close();
  }

  /** Random-access methods */
  public final void seek(long pos) throws IOException {
    super.seek(pos);
    pointer = (int)pos;
  }
  public final long length() throws IOException {
    return file.length;
  }
}

final class RAMFile {
  Vector buffers = new Vector();
  long length;
  long lastModified = System.currentTimeMillis();
}
