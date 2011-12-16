package org.apache.lucene.codecs.sep;

/**
 * LICENSED to the Apache Software Foundation (ASF) under one or more
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

// TODO: we may want tighter integration w/ IndexOutput --
// may give better perf:

import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.io.Closeable;

/** Defines basic API for writing ints to an IndexOutput.
 *  IntBlockCodec interacts with this API. @see
 *  IntBlockReader.
 *
 * <p>NOTE: block sizes could be variable
 *
 * @lucene.experimental */
public abstract class IntIndexOutput implements Closeable {

  /** Write an int to the primary file.  The value must be
   * >= 0.  */
  public abstract void write(int v) throws IOException;

  public abstract static class Index {

    /** Internally records the current location */
    public abstract void mark() throws IOException;

    /** Copies index from other */
    public abstract void copyFrom(Index other, boolean copyLast) throws IOException;

    /** Writes "location" of current output pointer of primary
     *  output to different output (out) */
    public abstract void write(IndexOutput indexOut, boolean absolute) throws IOException;
  }

  /** If you are indexing the primary output file, call
   *  this and interact with the returned IndexWriter. */
  public abstract Index index() throws IOException;

  public abstract void close() throws IOException;
}
