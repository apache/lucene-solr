package org.apache.lucene.index.memory;
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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.util.BytesRef;

/**
 * 
 * @lucene.internal
 */
class MemoryIndexNormDocValues extends DocValues {

  private final Source source;

  MemoryIndexNormDocValues(Source source) {
    this.source = source;
  }
  @Override
  public Source load() throws IOException {
    return source;
  }

  @Override
  public Source getDirectSource() throws IOException {
    return source;
  }

  @Override
  public Type type() {
    return source.type();
  }
  
  public static class SingleByteSource extends Source {

    private final byte[] bytes;

    protected SingleByteSource(byte[] bytes) {
      super(Type.BYTES_FIXED_STRAIGHT);
      this.bytes = bytes;
    }

    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      ref.bytes = bytes;
      ref.offset = docID;
      ref.length = 1;
      return ref;
    }

    @Override
    public boolean hasArray() {
      return true;
    }

    @Override
    public Object getArray() {
      return bytes;
    }
    
  }

}
