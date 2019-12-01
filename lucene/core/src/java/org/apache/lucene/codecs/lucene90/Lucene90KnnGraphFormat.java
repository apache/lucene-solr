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

package org.apache.lucene.codecs.lucene90;

import java.io.IOException;

import org.apache.lucene.codecs.KnnGraphFormat;
import org.apache.lucene.codecs.KnnGraphReader;
import org.apache.lucene.codecs.KnnGraphWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Lucene 9.0 vector and knn graph format, which encodes the vector values and the knn graphs
 * in a Hierarchical NSW graph for fast approximate nearest neighbor searching.
 * See <a href="https://arxiv.org/abs/1603.09320">this paper</a> for details.
 *
 */
public final class Lucene90KnnGraphFormat extends KnnGraphFormat {

  static final String META_CODEC_NAME = "Lucene90KnnGraphFormatMeta";
  static final String VECTOR_DATA_CODEC_NAME = "Lucene90VectorFormatData";
  static final String GRAPH_DATA_CODEC_NAME = "Lucene90KnnGraphFormatData";

  static final String META_EXTENSION = "gri";
  static final String VECTOR_DATA_EXTENSION = "vec";
  static final String GRAPH_DATA_EXTENSION = "grh";

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  /** Sole constructor */
  public Lucene90KnnGraphFormat() {
  }

  @Override
  public KnnGraphWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Lucene90KnnGraphWriter(state);
  }

  @Override
  public KnnGraphReader fieldsReader(SegmentReadState state) throws IOException {
    return new Lucene90KnnGraphReader(state);
  }

}
