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
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.search.Similarity;

/* Stores norms, buffered in RAM, until they are flushed
 * to a partial segment. */
final class BufferedNorms {

  RAMOutputStream out;
  int upto;

  private static final byte defaultNorm = Similarity.encodeNorm(1.0f);

  BufferedNorms() {
    out = new RAMOutputStream();
  }

  void add(float norm) throws IOException {
    byte b = Similarity.encodeNorm(norm);
    out.writeByte(b);
    upto++;
  }

  void reset() {
    out.reset();
    upto = 0;
  }

  void fill(int docID) throws IOException {
    // Must now fill in docs that didn't have this
    // field.  Note that this is how norms can consume
    // tremendous storage when the docs have widely
    // varying different fields, because we are not
    // storing the norms sparsely (see LUCENE-830)
    if (upto < docID) {
      DocumentsWriter.fillBytes(out, defaultNorm, docID-upto);
      upto = docID;
    }
  }
}

