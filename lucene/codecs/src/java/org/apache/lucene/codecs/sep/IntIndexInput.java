package org.apache.lucene.codecs.sep;

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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.store.DataInput;

/** Defines basic API for writing ints to an IndexOutput.
 *  IntBlockCodec interacts with this API. @see
 *  IntBlockReader
 *
 * @lucene.experimental */
public abstract class IntIndexInput implements Closeable {

  public abstract Reader reader() throws IOException;

  @Override
  public abstract void close() throws IOException;

  public abstract Index index() throws IOException;
  
  /** Records a single skip-point in the {@link IntIndexInput.Reader}. */
  public abstract static class Index {

    public abstract void read(DataInput indexIn, boolean absolute) throws IOException;

    /** Seeks primary stream to the last read offset */
    public abstract void seek(IntIndexInput.Reader stream) throws IOException;

    public abstract void copyFrom(Index other);
    
    @Override
    public abstract Index clone();
  }

  /** Reads int values. */
  public abstract static class Reader {

    /** Reads next single int */
    public abstract int next() throws IOException;
  }
}
