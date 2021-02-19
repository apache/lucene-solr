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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntBlockPool;

/**
 * This class is passed each token produced by the analyzer on each field during indexing, and it
 * stores these tokens in a hash table, and allocates separate byte streams per token. Consumers of
 * this class, eg {@link FreqProxTermsWriter} and {@link TermVectorsConsumer}, write their own byte
 * streams under each term.
 */
abstract class TermsHash {

  final TermsHash nextTermsHash;

  final IntBlockPool intPool;
  final ByteBlockPool bytePool;
  ByteBlockPool termBytePool;
  final Counter bytesUsed;

  TermsHash(
      final IntBlockPool.Allocator intBlockAllocator,
      final ByteBlockPool.Allocator byteBlockAllocator,
      Counter bytesUsed,
      TermsHash nextTermsHash) {
    this.nextTermsHash = nextTermsHash;
    this.bytesUsed = bytesUsed;
    intPool = new IntBlockPool(intBlockAllocator);
    bytePool = new ByteBlockPool(byteBlockAllocator);

    if (nextTermsHash != null) {
      // We are primary
      termBytePool = bytePool;
      nextTermsHash.termBytePool = bytePool;
    }
  }

  public void abort() {
    try {
      reset();
    } finally {
      if (nextTermsHash != null) {
        nextTermsHash.abort();
      }
    }
  }

  // Clear all state
  void reset() {
    // we don't reuse so we drop everything and don't fill with 0
    intPool.reset(false, false);
    bytePool.reset(false, false);
  }

  void flush(
      Map<String, TermsHashPerField> fieldsToFlush,
      final SegmentWriteState state,
      Sorter.DocMap sortMap,
      NormsProducer norms)
      throws IOException {
    if (nextTermsHash != null) {
      Map<String, TermsHashPerField> nextChildFields = new HashMap<>();
      for (final Map.Entry<String, TermsHashPerField> entry : fieldsToFlush.entrySet()) {
        nextChildFields.put(entry.getKey(), entry.getValue().getNextPerField());
      }
      nextTermsHash.flush(nextChildFields, state, sortMap, norms);
    }
  }

  abstract TermsHashPerField addField(FieldInvertState fieldInvertState, FieldInfo fieldInfo);

  void finishDocument(int docID) throws IOException {
    if (nextTermsHash != null) {
      nextTermsHash.finishDocument(docID);
    }
  }

  void startDocument() throws IOException {
    if (nextTermsHash != null) {
      nextTermsHash.startDocument();
    }
  }
}
