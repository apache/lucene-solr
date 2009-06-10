package org.apache.lucene.index;

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

/** 
 * This class keeps track of closing the underlying directory. It is used to wrap
 * DirectoryReaders, that are created using a String/File parameter
 * in IndexReader.open() with FSDirectory.getDirectory().
 * @deprecated This helper class is removed with all String/File
 * IndexReader.open() methods in Lucene 3.0
 */
final class DirectoryOwningReader extends FilterIndexReader implements Cloneable {

  DirectoryOwningReader(final IndexReader in) {
    super(in);
    this.ref = new SegmentReader.Ref();
    assert this.ref.refCount() == 1;
  }

  private DirectoryOwningReader(final IndexReader in, final SegmentReader.Ref ref) {
    super(in);
    this.ref = ref;
    ref.incRef();
  }

  public IndexReader reopen() throws CorruptIndexException, IOException {
    ensureOpen();
    final IndexReader r = in.reopen();
    if (r != in)
      return new DirectoryOwningReader(r, ref);
    return this;
  }

  public IndexReader reopen(boolean openReadOnly) throws CorruptIndexException, IOException {
    ensureOpen();
    final IndexReader r = in.reopen(openReadOnly);
    if (r != in)
      return new DirectoryOwningReader(r, ref);
    return this;
  }

  public IndexReader reopen(final IndexCommit commit) throws CorruptIndexException, IOException {
    ensureOpen();
    final IndexReader r = in.reopen(commit);
    if (r != in)
      return new DirectoryOwningReader(r, ref);
    return this;
  }

  public Object clone() {
    ensureOpen();
    return new DirectoryOwningReader((IndexReader) in.clone(), ref);
  }

  public IndexReader clone(boolean openReadOnly) throws CorruptIndexException, IOException {
    ensureOpen();
    return new DirectoryOwningReader(in.clone(openReadOnly), ref);
  }

  protected void doClose() throws IOException {
    IOException ioe = null;
    // close the reader, record exception
    try {
      super.doClose();
    } catch (IOException e) {
      ioe = e;
    }
    // close the directory, record exception
    if (ref.decRef() == 0) {
      try {
        in.directory().close();
      } catch (IOException e) {
        if (ioe == null) ioe = e;
      }
    }
    // throw the first exception
    if (ioe != null) throw ioe;
  }

  /**
   * This member contains the ref counter, that is passed to each instance after cloning/reopening,
   * and is global to all DirectoryOwningReader derived from the original one.
   * This reuses the class {@link SegmentReader.Ref}
   */
  private final SegmentReader.Ref ref;

}

