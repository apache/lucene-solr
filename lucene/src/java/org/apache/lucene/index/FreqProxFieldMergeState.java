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
import java.util.Comparator;

import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;

import org.apache.lucene.index.FreqProxTermsWriterPerField.FreqProxPostingsArray;

// TODO FI: some of this is "generic" to TermsHash* so we
// should factor it out so other consumers don't have to
// duplicate this code

/** Used by DocumentsWriter to merge the postings from
 *  multiple ThreadStates when creating a segment */
final class FreqProxFieldMergeState {

  final FreqProxTermsWriterPerField field;
  final int numPostings;
  private final ByteBlockPool bytePool;
  final int[] termIDs;
  final FreqProxPostingsArray postings;
  int currentTermID;
  
  final BytesRef text = new BytesRef();

  private int postingUpto = -1;

  final ByteSliceReader freq = new ByteSliceReader();
  final ByteSliceReader prox = new ByteSliceReader();

  int docID;
  int termFreq;

  public FreqProxFieldMergeState(FreqProxTermsWriterPerField field, Comparator<BytesRef> termComp) {
    this.field = field;
    this.numPostings = field.termsHashPerField.bytesHash.size();
    this.bytePool = field.perThread.termsHashPerThread.bytePool;
    this.termIDs = field.termsHashPerField.sortPostings(termComp);
    this.postings = (FreqProxPostingsArray) field.termsHashPerField.postingsArray;
  }

  boolean nextTerm() throws IOException {
    postingUpto++;
    if (postingUpto == numPostings) {
      return false;
    }

    currentTermID = termIDs[postingUpto];
    docID = 0;

    // Get BytesRef
    final int textStart = postings.textStarts[currentTermID];
    bytePool.setBytesRef(text, textStart);

    field.termsHashPerField.initReader(freq, currentTermID, 0);
    if (!field.fieldInfo.omitTermFreqAndPositions) {
      field.termsHashPerField.initReader(prox, currentTermID, 1);
    }

    // Should always be true
    boolean result = nextDoc();
    assert result;

    return true;
  }

  public boolean nextDoc() throws IOException {
    if (freq.eof()) {
      if (postings.lastDocCodes[currentTermID] != -1) {
        // Return last doc
        docID = postings.lastDocIDs[currentTermID];
        if (!field.omitTermFreqAndPositions)
          termFreq = postings.docFreqs[currentTermID];
        postings.lastDocCodes[currentTermID] = -1;
        return true;
      } else
        // EOF
        return false;
    }

    final int code = freq.readVInt();
    if (field.omitTermFreqAndPositions)
      docID += code;
    else {
      docID += code >>> 1;
      if ((code & 1) != 0)
        termFreq = 1;
      else
        termFreq = freq.readVInt();
    }

    assert docID != postings.lastDocIDs[currentTermID];

    return true;
  }
}
