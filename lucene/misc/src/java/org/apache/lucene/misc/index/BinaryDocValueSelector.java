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

package org.apache.lucene.misc.index;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/** Select documents using binary doc values */
public class BinaryDocValueSelector implements IndexRearranger.DocumentSelector, Serializable {

  private final String field;
  private final HashSet<String> keySet;

  public BinaryDocValueSelector(String field, HashSet<String> keySet) {
    this.field = field;
    this.keySet = keySet;
  }

  @Override
  public BitSet getFilteredLiveDocs(CodecReader reader) throws IOException {
    BinaryDocValues binaryDocValues = reader.getBinaryDocValues(field);
    Bits oldLiveDocs = reader.getLiveDocs();
    FixedBitSet bits = new FixedBitSet(reader.maxDoc());
    for (int i = 0; i < reader.maxDoc(); i++) {
      if (oldLiveDocs != null && oldLiveDocs.get(i) == false) {
        continue;
      }
      if (binaryDocValues.advanceExact(i)
          && keySet.contains(binaryDocValues.binaryValue().utf8ToString())) {
        bits.set(i);
      }
    }
    return bits;
  }

  public static List<IndexRearranger.DocumentSelector> createFromExistingIndex(
      String field, Directory directory) throws IOException {
    List<IndexRearranger.DocumentSelector> selectors = new ArrayList<>();
    try (IndexReader reader = DirectoryReader.open(directory)) {
      for (LeafReaderContext context : reader.leaves()) {
        HashSet<String> keySet = new HashSet<>();
        Bits liveDocs = context.reader().getLiveDocs();
        BinaryDocValues binaryDocValues = context.reader().getBinaryDocValues(field);
        for (int i = 0; i < context.reader().maxDoc(); i++) {
          if (liveDocs != null && liveDocs.get(i) == false) {
            continue;
          }
          if (binaryDocValues.advanceExact(i)) {
            keySet.add(binaryDocValues.binaryValue().utf8ToString());
          } else {
            throw new AssertionError("Document don't have selected key");
          }
        }
        selectors.add(new BinaryDocValueSelector(field, keySet));
      }
    }
    return selectors;
  }
}
