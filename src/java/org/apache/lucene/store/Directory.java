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

/** A Directory is a flat list of files.  Files may be written once, when they
 * are created.  Once a file is created it may only be opened for read, or
 * deleted.  Random access is permitted both when reading and writing.
 *
 * <p> Java's i/o APIs not used directly, but rather all i/o is
 * through this API.  This permits things such as: <ul>
 * <li> implementation of RAM-based indices;
 * <li> implementation indices stored in a database, via JDBC;
 * <li> implementation of an index as a single file;
 * </ul>
 *
 * @author Doug Cutting
 */
public abstract class Directory {
  /** Returns an array of strings, one for each file in the directory. */
  public abstract String[] list()
       throws IOException;

  /** Returns true iff a file with the given name exists. */
  public abstract boolean fileExists(String name)
       throws IOException;

  /** Returns the time the named file was last modified. */
  public abstract long fileModified(String name)
       throws IOException;

  /** Set the modified time of an existing file to now. */
  public abstract void touchFile(String name)
       throws IOException;

  /** Removes an existing file in the directory. */
  public abstract void deleteFile(String name)
       throws IOException;

  /** Renames an existing file in the directory.
    If a file already exists with the new name, then it is replaced.
    This replacement should be atomic. */
  public abstract void renameFile(String from, String to)
       throws IOException;

  /** Returns the length of a file in the directory. */
  public abstract long fileLength(String name)
       throws IOException;

  /** Creates a new, empty file in the directory with the given name.
      Returns a stream writing this file. */
  public abstract OutputStream createFile(String name)
       throws IOException;

  /** Returns a stream reading an existing file. */
  public abstract InputStream openFile(String name)
       throws IOException;

  /** Construct a {@link Lock}.
   * @param name the name of the lock file
   */
  public abstract Lock makeLock(String name);

  /** Closes the store. */
  public abstract void close()
       throws IOException;
}
