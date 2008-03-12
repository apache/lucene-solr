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

/** Used by DocumentsWriter to merge the postings from
 *  multiple ThreadStates when creating a segment */
final class DocumentsWriterFieldMergeState {

  DocumentsWriterFieldData field;

  Posting[] postings;

  private Posting p;
  char[] text;
  int textOffset;

  private int postingUpto = -1;

  ByteSliceReader freq = new ByteSliceReader();
  ByteSliceReader prox = new ByteSliceReader();

  int docID;
  int termFreq;

  boolean nextTerm() throws IOException {
    postingUpto++;
    if (postingUpto == field.numPostings)
      return false;

    p = postings[postingUpto];
    docID = 0;

    text = field.threadState.charPool.buffers[p.textStart >> DocumentsWriter.CHAR_BLOCK_SHIFT];
    textOffset = p.textStart & DocumentsWriter.CHAR_BLOCK_MASK;

    if (p.freqUpto > p.freqStart)
      freq.init(field.threadState.postingsPool, p.freqStart, p.freqUpto);
    else
      freq.bufferOffset = freq.upto = freq.endIndex = 0;

    prox.init(field.threadState.postingsPool, p.proxStart, p.proxUpto);

    // Should always be true
    boolean result = nextDoc();
    assert result;

    return true;
  }

  public boolean nextDoc() throws IOException {
    if (freq.bufferOffset + freq.upto == freq.endIndex) {
      if (p.lastDocCode != -1) {
        // Return last doc
        docID = p.lastDocID;
        termFreq = p.docFreq;
        p.lastDocCode = -1;
        return true;
      } else 
        // EOF
        return false;
    }

    final int code = freq.readVInt();
    docID += code >>> 1;
    if ((code & 1) != 0)
      termFreq = 1;
    else
      termFreq = freq.readVInt();

    return true;
  }
}
