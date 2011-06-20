package org.apache.lucene.index.codecs.mocksep;

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

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.index.IOContext;
import org.apache.lucene.index.codecs.sep.IntIndexOutput;
import java.io.IOException;

/** Writes ints directly to the file (not in blocks) as
 *  vInt.
 * 
 * @lucene.experimental
*/
public class MockSingleIntIndexOutput extends IntIndexOutput {
  private final IndexOutput out;
  final static String CODEC = "SINGLE_INTS";
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;

  public MockSingleIntIndexOutput(Directory dir, String fileName) throws IOException {
    //nocommit pass IOContext in via ctor!
    out = dir.createOutput(fileName, IOContext.DEFAULT);
    boolean success = false;
    try {
      CodecUtil.writeHeader(out, CODEC, VERSION_CURRENT);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeSafely(true, out);
      }
    }
  }

  /** Write an int to the primary file */
  @Override
  public void write(int v) throws IOException {
    assert v >= 0;
    out.writeVInt(v);
  }

  @Override
  public Index index() {
    return new Index();
  }

  @Override
  public void close() throws IOException {
    out.close();
  }

  private class Index extends IntIndexOutput.Index {
    long fp;
    long lastFP;
    @Override
    public void mark() {
      fp = out.getFilePointer();
    }
    @Override
    public void set(IntIndexOutput.Index other) {
      lastFP = fp = ((Index) other).fp;
    }
    @Override
    public void write(IndexOutput indexOut, boolean absolute)
      throws IOException {
      if (absolute) {
        indexOut.writeVLong(fp);
      } else {
        indexOut.writeVLong(fp - lastFP);
      }
      lastFP = fp;
    }
      
    @Override
    public String toString() {
      return Long.toString(fp);
    }
  }
}
