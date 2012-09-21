package org.apache.lucene.codecs.mocksep;

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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.sep.IntIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

/** Reads IndexInputs written with {@link
 *  MockSingleIntIndexOutput}.  NOTE: this class is just for
 *  demonstration purposes (it is a very slow way to read a
 *  block of ints).
 *
 * @lucene.experimental
 */
public class MockSingleIntIndexInput extends IntIndexInput {
  private final IndexInput in;

  public MockSingleIntIndexInput(Directory dir, String fileName, IOContext context)
    throws IOException {
    in = dir.openInput(fileName, context);
    CodecUtil.checkHeader(in, MockSingleIntIndexOutput.CODEC,
                          MockSingleIntIndexOutput.VERSION_START,
                          MockSingleIntIndexOutput.VERSION_START);
  }

  @Override
  public Reader reader() throws IOException {
    return new Reader(in.clone());
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  /**
   * Just reads a vInt directly from the file.
   */
  public static class Reader extends IntIndexInput.Reader {
    // clone:
    private final IndexInput in;

    public Reader(IndexInput in) {
      this.in = in;
    }

    /** Reads next single int */
    @Override
    public int next() throws IOException {
      //System.out.println("msii.next() fp=" + in.getFilePointer() + " vs " + in.length());
      return in.readVInt();
    }
  }
  
  class MockSingleIntIndexInputIndex extends IntIndexInput.Index {
    private long fp;

    @Override
    public void read(DataInput indexIn, boolean absolute)
      throws IOException {
      if (absolute) {
        fp = indexIn.readVLong();
      } else {
        fp += indexIn.readVLong();
      }
    }

    @Override
    public void copyFrom(IntIndexInput.Index other) {
      fp = ((MockSingleIntIndexInputIndex) other).fp;
    }

    @Override
    public void seek(IntIndexInput.Reader other) throws IOException {
      ((Reader) other).in.seek(fp);
    }

    @Override
    public String toString() {
      return Long.toString(fp);
    }

    @Override
    public Index clone() {
      MockSingleIntIndexInputIndex other = new MockSingleIntIndexInputIndex();
      other.fp = fp;
      return other;
    }
  }

  @Override
  public Index index() {
    return new MockSingleIntIndexInputIndex();
  }
}

