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

import org.apache.lucene.index.KnnGraphValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorValues;

/**
 * Encodes/decodes per-document vector and indexed knn-graph for approximate nearest neighbor search.
 */
public abstract class KnnGraphFormat {

  /** Sole constructor */
  protected KnnGraphFormat() {}

  /**
   * Returns a {@link KnnGraphWriter} to write the vectors and knn-graph to the index.
   */
  public abstract KnnGraphWriter fieldsWriter(SegmentWriteState state) throws IOException;

  /**
   * Returns a {@link KnnGraphReader} to read the vectors and knn-graph from the index.
   */
  public abstract KnnGraphReader fieldsReader(SegmentReadState state) throws IOException;

  public static KnnGraphFormat EMPTY = new KnnGraphFormat() {
    @Override
    public KnnGraphWriter fieldsWriter(SegmentWriteState state) throws IOException {
      throw new UnsupportedOperationException("Attempt to write EMPTY KnnGraphValues: maybe you forgot to use codec=Lucene90");
    }

    @Override
    public KnnGraphReader fieldsReader(SegmentReadState state) throws IOException {
      return new KnnGraphReader() {
        @Override
        public void checkIntegrity() throws IOException {
        }

        @Override
        public VectorValues getVectorValues(String field) throws IOException {
          return VectorValues.EMPTY;
        }

        @Override
        public KnnGraphValues getGraphValues(String field) throws IOException {
          return KnnGraphValues.EMPTY;
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
