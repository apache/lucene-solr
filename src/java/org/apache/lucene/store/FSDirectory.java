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
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;
import java.util.Hashtable;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.InputStream;
import org.apache.lucene.store.OutputStream;

/**
  Straightforward implementation of Directory as a directory of files.
    @see Directory
    @author Doug Cutting
*/

final public class FSDirectory extends Directory {
  /** This cache of directories ensures that there is a unique Directory
   * instance per path, so that synchronization on the Directory can be used to
   * synchronize access between readers and writers.
   *
   * This should be a WeakHashMap, so that entries can be GC'd, but that would
   * require Java 1.2.  Instead we use refcounts...  */
  private static final Hashtable DIRECTORIES = new Hashtable();

  /** Returns the directory instance for the named location.
   * 
   * <p>Directories are cached, so that, for a given canonical path, the same
   * FSDirectory instance will always be returned.  This permits
   * synchronization on directories.
   * 
   * @param path the path to the directory.
   * @param create if true, create, or erase any existing contents.
   * @returns the FSDirectory for the named file.  */
  public static FSDirectory getDirectory(String path, boolean create)
      throws IOException {
    return getDirectory(new File(path), create);
  }

  /** Returns the directory instance for the named location.
   * 
   * <p>Directories are cached, so that, for a given canonical path, the same
   * FSDirectory instance will always be returned.  This permits
   * synchronization on directories.
   * 
   * @param file the path to the directory.
   * @param create if true, create, or erase any existing contents.
   * @returns the FSDirectory for the named file.  */
  public static FSDirectory getDirectory(File file, boolean create)
    throws IOException {
    file = new File(file.getCanonicalPath());
    FSDirectory dir;
    synchronized (DIRECTORIES) {
      dir = (FSDirectory)DIRECTORIES.get(file);
      if (dir == null) {
	dir = new FSDirectory(file, create);
	DIRECTORIES.put(file, dir);
      }
    }
    synchronized (dir) {
      dir.refCount++;
    }
    return dir;
  }

  private File directory = null;
  private int refCount;

  private FSDirectory(File path, boolean create) throws IOException {
    directory = path;
    if (!directory.exists() && create)
      directory.mkdir();
    if (!directory.isDirectory())
      throw new IOException(path + " not a directory");

    if (create) {				  // clear old files
      String[] files = directory.list();
      for (int i = 0; i < files.length; i++) {
	File file = new File(directory, files[i]);
	if (!file.delete())
	  throw new IOException("couldn't delete " + files[i]);
      }
    }

  }

  /** Returns an array of strings, one for each file in the directory. */
  public final String[] list() throws IOException {
    return directory.list();
  }
       
  /** Returns true iff a file with the given name exists. */
  public final boolean fileExists(String name) throws IOException {
    File file = new File(directory, name);
    return file.exists();
  }
       
  /** Returns the time the named file was last modified. */
  public final long fileModified(String name) throws IOException {
    File file = new File(directory, name);
    return file.lastModified();
  }
       
  /** Returns the time the named file was last modified. */
  public static final long fileModified(File directory, String name)
       throws IOException {
    File file = new File(directory, name);
    return file.lastModified();
  }

  /** Returns the length in bytes of a file in the directory. */
  public final long fileLength(String name) throws IOException {
    File file = new File(directory, name);
    return file.length();
  }

  /** Removes an existing file in the directory. */
  public final void deleteFile(String name) throws IOException {
    File file = new File(directory, name);
    if (!file.delete())
      throw new IOException("couldn't delete " + name);
  }

  /** Renames an existing file in the directory. */
  public final synchronized void renameFile(String from, String to)
      throws IOException {
    File old = new File(directory, from);
    File nu = new File(directory, to);

    /* This is not atomic.  If the program crashes between the call to
       delete() and the call to renameTo() then we're screwed, but I've
       been unable to figure out how else to do this... */

    if (nu.exists())
      if (!nu.delete())
	throw new IOException("couldn't delete " + to);

    if (!old.renameTo(nu))
      throw new IOException("couldn't rename " + from + " to " + to);
  }

  /** Creates a new, empty file in the directory with the given name.
      Returns a stream writing this file. */
  public final OutputStream createFile(String name) throws IOException {
    return new FSOutputStream(new File(directory, name));
  }

  /** Returns a stream reading an existing file. */
  public final InputStream openFile(String name) throws IOException {
    return new FSInputStream(new File(directory, name));
  }

  /** Closes the store to future operations. */
  public final synchronized void close() throws IOException {
    if (--refCount <= 0) {
      synchronized (DIRECTORIES) {
	DIRECTORIES.remove(directory);
      }
    }
  }
}


final class FSInputStream extends InputStream {
  private class Descriptor extends RandomAccessFile {
    public long position;
    public Descriptor(File file, String mode) throws IOException {
      super(file, mode);
    }
  }

  Descriptor file = null;
  boolean isClone;

  public FSInputStream(File path) throws IOException {
    file = new Descriptor(path, "r");
    length = file.length();
  }

  /** InputStream methods */
  protected final void readInternal(byte[] b, int offset, int len)
       throws IOException {
    synchronized (file) {
      long position = getFilePointer();
      if (position != file.position) {
	file.seek(position);
	file.position = position;
      }
      int total = 0;
      do {
	int i = file.read(b, offset+total, len-total);
	if (i == -1)
	  throw new IOException("read past EOF");
	file.position += i;
	total += i;
      } while (total < len);
    }
  }

  public final void close() throws IOException {
    if (!isClone)
      file.close();
  }

  /** Random-access methods */
  protected final void seekInternal(long position) throws IOException {
  }

  protected final void finalize() throws IOException {
    close();					  // close the file 
  }

  public Object clone() {
    FSInputStream clone = (FSInputStream)super.clone();
    clone.isClone = true;
    return clone;
  }
}


final class FSOutputStream extends OutputStream {
  RandomAccessFile file = null;

  public FSOutputStream(File path) throws IOException {
    if (path.isFile())
      throw new IOException(path + " already exists");
    file = new RandomAccessFile(path, "rw");
  }

  /** output methods: */
  public final void flushBuffer(byte[] b, int size) throws IOException {
    file.write(b, 0, size);
  }
  public final void close() throws IOException {
    super.close();
    file.close();
  }

  /** Random-access methods */
  public final void seek(long pos) throws IOException {
    super.seek(pos);
    file.seek(pos);
  }
  public final long length() throws IOException {
    return file.length();
  }

  protected final void finalize() throws IOException {
    file.close();				  // close the file 
  }

}
