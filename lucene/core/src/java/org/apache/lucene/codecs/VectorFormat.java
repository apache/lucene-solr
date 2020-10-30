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

package org.apache.lucene.codecs;

import java.io.IOException;

import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorValues;

/**
 * Encodes/decodes per-document vector and any associated indexing structures required to support nearest-neighbor search
 */
public abstract class VectorFormat {

  /** Sole constructor */
  protected VectorFormat() {}

  /**
   * Returns a {@link VectorWriter} to write the vectors to the index.
   */
  public abstract VectorWriter fieldsWriter(SegmentWriteState state) throws IOException;

  /**
   * Returns a {@link VectorReader} to read the vectors from the index.
   */
  public abstract VectorReader fieldsReader(SegmentReadState state) throws IOException;

  /**
   * EMPTY throws an exception when written. It acts as a sentinel indicating a Codec that does not support vectors.
   */
  public static final VectorFormat EMPTY = new VectorFormat() {
    @Override
    public VectorWriter fieldsWriter(SegmentWriteState state) {
      throw new UnsupportedOperationException("Attempt to write EMPTY VectorValues: maybe you forgot to use codec=Lucene90");
    }

    @Override
    public VectorReader fieldsReader(SegmentReadState state) {
      return new VectorReader() {
        @Override
        public void checkIntegrity() {
        }

        @Override
        public VectorValues getVectorValues(String field)  {
          return VectorValues.EMPTY;
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public long ramBytesUsed() {
          return 0;
        }
      };
    }
  };
}
