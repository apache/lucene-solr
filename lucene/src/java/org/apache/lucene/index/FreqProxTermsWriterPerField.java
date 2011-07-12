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

import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.util.RamUsageEstimator;

// TODO: break into separate freq and prox writers as
// codecs; make separate container (tii/tis/skip/*) that can
// be configured as any number of files 1..N
final class FreqProxTermsWriterPerField extends TermsHashConsumerPerField implements Comparable<FreqProxTermsWriterPerField> {

  final FreqProxTermsWriterPerThread perThread;
  final TermsHashPerField termsHashPerField;
  final FieldInfo fieldInfo;
  final DocumentsWriter.DocState docState;
  final FieldInvertState fieldState;
  IndexOptions indexOptions;
  PayloadAttribute payloadAttribute;

  public FreqProxTermsWriterPerField(TermsHashPerField termsHashPerField, FreqProxTermsWriterPerThread perThread, FieldInfo fieldInfo) {
    this.termsHashPerField = termsHashPerField;
    this.perThread = perThread;
    this.fieldInfo = fieldInfo;
    docState = termsHashPerField.docState;
    fieldState = termsHashPerField.fieldState;
    indexOptions = fieldInfo.indexOptions;
  }

  @Override
  int getStreamCount() {
    if (fieldInfo.indexOptions != IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)
      return 1;
    else
      return 2;
  }

  @Override
  void finish() {}

  boolean hasPayloads;

  @Override
  void skippingLongTerm() throws IOException {}

  public int compareTo(FreqProxTermsWriterPerField other) {
    return fieldInfo.name.compareTo(other.fieldInfo.name);
  }

  void reset() {
    // Record, up front, whether our in-RAM format will be
    // with or without term freqs:
    indexOptions = fieldInfo.indexOptions;
    payloadAttribute = null;
  }

  @Override
  boolean start(Fieldable[] fields, int count) {
    for(int i=0;i<count;i++)
      if (fields[i].isIndexed())
        return true;
    return false;
  }     
  
  @Override
  void start(Fieldable f) {
    if (fieldState.attributeSource.hasAttribute(PayloadAttribute.class)) {
      payloadAttribute = fieldState.attributeSource.getAttribute(PayloadAttribute.class);
    } else {
      payloadAttribute = null;
    }
  }

  void writeProx(final int termID, int proxCode) {
    final Payload payload;
    if (payloadAttribute == null) {
      payload = null;
    } else {
      payload = payloadAttribute.getPayload();
    }
    
    if (payload != null && payload.length > 0) {
      termsHashPerField.writeVInt(1, (proxCode<<1)|1);
      termsHashPerField.writeVInt(1, payload.length);
      termsHashPerField.writeBytes(1, payload.data, payload.offset, payload.length);
      hasPayloads = true;      
    } else
      termsHashPerField.writeVInt(1, proxCode<<1);
    
    FreqProxPostingsArray postings = (FreqProxPostingsArray) termsHashPerField.postingsArray;
    postings.lastPositions[termID] = fieldState.position;
    
  }

  @Override
  void newTerm(final int termID) {
    // First time we're seeing this term since the last
    // flush
    assert docState.testPoint("FreqProxTermsWriterPerField.newTerm start");
    
    FreqProxPostingsArray postings = (FreqProxPostingsArray) termsHashPerField.postingsArray;
    postings.lastDocIDs[termID] = docState.docID;
    if (indexOptions == IndexOptions.DOCS_ONLY) {
      postings.lastDocCodes[termID] = docState.docID;
    } else {
      postings.lastDocCodes[termID] = docState.docID << 1;
      postings.docFreqs[termID] = 1;
      if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
        writeProx(termID, fieldState.position);
      }
    }
    fieldState.maxTermFrequency = Math.max(1, fieldState.maxTermFrequency);
    fieldState.uniqueTermCount++;
  }

  @Override
  void addTerm(final int termID) {

    assert docState.testPoint("FreqProxTermsWriterPerField.addTerm start");
    
    FreqProxPostingsArray postings = (FreqProxPostingsArray) termsHashPerField.postingsArray;
    
    assert indexOptions == IndexOptions.DOCS_ONLY || postings.docFreqs[termID] > 0;

    if (indexOptions == IndexOptions.DOCS_ONLY) {
      if (docState.docID != postings.lastDocIDs[termID]) {
        assert docState.docID > postings.lastDocIDs[termID];
        termsHashPerField.writeVInt(0, postings.lastDocCodes[termID]);
        postings.lastDocCodes[termID] = docState.docID - postings.lastDocIDs[termID];
        postings.lastDocIDs[termID] = docState.docID;
        fieldState.uniqueTermCount++;
      }
    } else {
      if (docState.docID != postings.lastDocIDs[termID]) {
        assert docState.docID > postings.lastDocIDs[termID];
        // Term not yet seen in the current doc but previously
        // seen in other doc(s) since the last flush

        // Now that we know doc freq for previous doc,
        // write it & lastDocCode
        if (1 == postings.docFreqs[termID])
          termsHashPerField.writeVInt(0, postings.lastDocCodes[termID]|1);
        else {
          termsHashPerField.writeVInt(0, postings.lastDocCodes[termID]);
          termsHashPerField.writeVInt(0, postings.docFreqs[termID]);
        }
        postings.docFreqs[termID] = 1;
        fieldState.maxTermFrequency = Math.max(1, fieldState.maxTermFrequency);
        postings.lastDocCodes[termID] = (docState.docID - postings.lastDocIDs[termID]) << 1;
        postings.lastDocIDs[termID] = docState.docID;
        if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
          writeProx(termID, fieldState.position);
        }
        fieldState.uniqueTermCount++;
      } else {
        fieldState.maxTermFrequency = Math.max(fieldState.maxTermFrequency, ++postings.docFreqs[termID]);
        if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
          writeProx(termID, fieldState.position-postings.lastPositions[termID]);
        }
      }
    }
  }
  
  @Override
  ParallelPostingsArray createPostingsArray(int size) {
    return new FreqProxPostingsArray(size);
  }

  static final class FreqProxPostingsArray extends ParallelPostingsArray {
    public FreqProxPostingsArray(int size) {
      super(size);
      docFreqs = new int[size];
      lastDocIDs = new int[size];
      lastDocCodes = new int[size];
      lastPositions = new int[size];
    }

    int docFreqs[];                                    // # times this term occurs in the current doc
    int lastDocIDs[];                                  // Last docID where this term occurred
    int lastDocCodes[];                                // Code for prior doc
    int lastPositions[];                               // Last position where this term occurred

    @Override
    ParallelPostingsArray newInstance(int size) {
      return new FreqProxPostingsArray(size);
    }

    @Override
    void copyTo(ParallelPostingsArray toArray, int numToCopy) {
      assert toArray instanceof FreqProxPostingsArray;
      FreqProxPostingsArray to = (FreqProxPostingsArray) toArray;

      super.copyTo(toArray, numToCopy);

      System.arraycopy(docFreqs, 0, to.docFreqs, 0, numToCopy);
      System.arraycopy(lastDocIDs, 0, to.lastDocIDs, 0, numToCopy);
      System.arraycopy(lastDocCodes, 0, to.lastDocCodes, 0, numToCopy);
      System.arraycopy(lastPositions, 0, to.lastPositions, 0, numToCopy);
    }

    @Override
    int bytesPerPosting() {
      return ParallelPostingsArray.BYTES_PER_POSTING + 4 * RamUsageEstimator.NUM_BYTES_INT;
    }
  }
  
  public void abort() {}
}

