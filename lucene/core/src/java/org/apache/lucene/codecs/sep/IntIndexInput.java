package org.apache.lucene.codecs.sep;

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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.IntsRef;

/** Defines basic API for writing ints to an IndexOutput.
 *  IntBlockCodec interacts with this API. @see
 *  IntBlockReader
 *
 * @lucene.experimental */
public abstract class IntIndexInput implements Closeable {

  public abstract Reader reader() throws IOException;

  public abstract void close() throws IOException;

  public abstract Index index() throws IOException;
  
  // TODO: -- can we simplify this?
  public abstract static class Index {

    public abstract void read(DataInput indexIn, boolean absolute) throws IOException;

    /** Seeks primary stream to the last read offset */
    public abstract void seek(IntIndexInput.Reader stream) throws IOException;

    public abstract void set(Index other);
    
    @Override
    public abstract Index clone();
  }

  public abstract static class Reader {

    /** Reads next single int */
    public abstract int next() throws IOException;

    /** Reads next chunk of ints */
    private IntsRef bulkResult;

    /** Read up to count ints. */
    public IntsRef read(int count) throws IOException {
      if (bulkResult == null) {
        bulkResult = new IntsRef();
        bulkResult.ints = new int[count];
      } else {
        bulkResult.grow(count);
      }
      for(int i=0;i<count;i++) {
        bulkResult.ints[i] = next();
      }
      bulkResult.length = count;
      return bulkResult;
    }
  }
}
