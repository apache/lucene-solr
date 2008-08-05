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

// TODO FI: some of this is "generic" to TermsHash* so we
// should factor it out so other consumers don't have to
// duplicate this code

/** Used by DocumentsWriter to merge the postings from
 *  multiple ThreadStates when creating a segment */
final class FreqProxFieldMergeState {

  final FreqProxTermsWriterPerField field;
  final int numPostings;
  final CharBlockPool charPool;
  final RawPostingList[] postings;

  private FreqProxTermsWriter.PostingList p;
  char[] text;
  int textOffset;

  private int postingUpto = -1;

  final ByteSliceReader freq = new ByteSliceReader();
  final ByteSliceReader prox = new ByteSliceReader();

  int docID;
  int termFreq;

  public FreqProxFieldMergeState(FreqProxTermsWriterPerField field) {
    this.field = field;
    this.charPool = field.perThread.termsHashPerThread.charPool;
    this.numPostings = field.termsHashPerField.numPostings;
    this.postings = field.termsHashPerField.sortPostings();
  }

  boolean nextTerm() throws IOException {
    postingUpto++;
    if (postingUpto == numPostings)
      return false;

    p = (FreqProxTermsWriter.PostingList) postings[postingUpto];
    docID = 0;

    text = charPool.buffers[p.textStart >> DocumentsWriter.CHAR_BLOCK_SHIFT];
    textOffset = p.textStart & DocumentsWriter.CHAR_BLOCK_MASK;

    field.termsHashPerField.initReader(freq, p, 0);
    if (!field.fieldInfo.omitTf)
      field.termsHashPerField.initReader(prox, p, 1);

    // Should always be true
    boolean result = nextDoc();
    assert result;

    return true;
  }

  public boolean nextDoc() throws IOException {
    if (freq.eof()) {
      if (p.lastDocCode != -1) {
        // Return last doc
        docID = p.lastDocID;
        if (!field.omitTf)
          termFreq = p.docFreq;
        p.lastDocCode = -1;
        return true;
      } else
        // EOF
        return false;
    }

    final int code = freq.readVInt();
    if (field.omitTf)
      docID += code;
    else {
      docID += code >>> 1;
      if ((code & 1) != 0)
        termFreq = 1;
      else
        termFreq = freq.readVInt();
    }

    assert docID != p.lastDocID;

    return true;
  }
}
