package org.apache.lucene.index;

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

import java.io.IOException;

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

final class TermVectorsConsumerPerField extends TermsHashConsumerPerField {

  final TermsHashPerField termsHashPerField;
  final TermVectorsConsumer termsWriter;
  final FieldInfo fieldInfo;
  final DocumentsWriterPerThread.DocState docState;
  final FieldInvertState fieldState;

  boolean doVectors;
  boolean doVectorPositions;
  boolean doVectorOffsets;
  boolean doVectorPayloads;

  int maxNumPostings;
  OffsetAttribute offsetAttribute;
  PayloadAttribute payloadAttribute;
  boolean hasPayloads; // if enabled, and we actually saw any for this field

  public TermVectorsConsumerPerField(TermsHashPerField termsHashPerField, TermVectorsConsumer termsWriter, FieldInfo fieldInfo) {
    this.termsHashPerField = termsHashPerField;
    this.termsWriter = termsWriter;
    this.fieldInfo = fieldInfo;
    docState = termsHashPerField.docState;
    fieldState = termsHashPerField.fieldState;
  }

  @Override
  int getStreamCount() {
    return 2;
  }

  @Override
  boolean start(IndexableField[] fields, int count) {
    doVectors = false;
    doVectorPositions = false;
    doVectorOffsets = false;
    doVectorPayloads = false;
    hasPayloads = false;

    for(int i=0;i<count;i++) {
      IndexableField field = fields[i];
      if (field.fieldType().indexed()) {
        if (field.fieldType().storeTermVectors()) {
          doVectors = true;
          doVectorPositions |= field.fieldType().storeTermVectorPositions();
          doVectorOffsets |= field.fieldType().storeTermVectorOffsets();
          if (doVectorPositions) {
            doVectorPayloads |= field.fieldType().storeTermVectorPayloads();
          } else if (field.fieldType().storeTermVectorPayloads()) {
            // TODO: move this check somewhere else, and impl the other missing ones
            throw new IllegalArgumentException("cannot index term vector payloads for field: " + field + " without term vector positions");
          }
        } else {
          if (field.fieldType().storeTermVectorOffsets()) {
            throw new IllegalArgumentException("cannot index term vector offsets when term vectors are not indexed (field=\"" + field.name());
          }
          if (field.fieldType().storeTermVectorPositions()) {
            throw new IllegalArgumentException("cannot index term vector positions when term vectors are not indexed (field=\"" + field.name());
          }
          if (field.fieldType().storeTermVectorPayloads()) {
            throw new IllegalArgumentException("cannot index term vector payloads when term vectors are not indexed (field=\"" + field.name());
          }
        }
      } else {
        if (field.fieldType().storeTermVectors()) {
          throw new IllegalArgumentException("cannot index term vectors when field is not indexed (field=\"" + field.name());
        }
        if (field.fieldType().storeTermVectorOffsets()) {
          throw new IllegalArgumentException("cannot index term vector offsets when field is not indexed (field=\"" + field.name());
        }
        if (field.fieldType().storeTermVectorPositions()) {
          throw new IllegalArgumentException("cannot index term vector positions when field is not indexed (field=\"" + field.name());
        }
        if (field.fieldType().storeTermVectorPayloads()) {
          throw new IllegalArgumentException("cannot index term vector payloads when field is not indexed (field=\"" + field.name());
        }
      }
    }

    if (doVectors) {
      termsWriter.hasVectors = true;
      if (termsHashPerField.bytesHash.size() != 0) {
        // Only necessary if previous doc hit a
        // non-aborting exception while writing vectors in
        // this field:
        termsHashPerField.reset();
      }
    }

    // TODO: only if needed for performance
    //perThread.postingsCount = 0;

    return doVectors;
  }

  public void abort() {}

  /** Called once per field per document if term vectors
   *  are enabled, to write the vectors to
   *  RAMOutputStream, which is then quickly flushed to
   *  the real term vectors files in the Directory. */  @Override
  void finish() {
    if (!doVectors || termsHashPerField.bytesHash.size() == 0) {
      return;
    }

    termsWriter.addFieldToFlush(this);
  }

  void finishDocument() throws IOException {
    assert docState.testPoint("TermVectorsTermsWriterPerField.finish start");

    final int numPostings = termsHashPerField.bytesHash.size();

    final BytesRef flushTerm = termsWriter.flushTerm;

    assert numPostings >= 0;

    if (numPostings > maxNumPostings)
      maxNumPostings = numPostings;

    // This is called once, after inverting all occurrences
    // of a given field in the doc.  At this point we flush
    // our hash into the DocWriter.

    assert termsWriter.vectorFieldsInOrder(fieldInfo);

    TermVectorsPostingsArray postings = (TermVectorsPostingsArray) termsHashPerField.postingsArray;
    final TermVectorsWriter tv = termsWriter.writer;

    final int[] termIDs = termsHashPerField.sortPostings(tv.getComparator());

    tv.startField(fieldInfo, numPostings, doVectorPositions, doVectorOffsets, hasPayloads);
    
    final ByteSliceReader posReader = doVectorPositions ? termsWriter.vectorSliceReaderPos : null;
    final ByteSliceReader offReader = doVectorOffsets ? termsWriter.vectorSliceReaderOff : null;
    
    final ByteBlockPool termBytePool = termsHashPerField.termBytePool;

    for(int j=0;j<numPostings;j++) {
      final int termID = termIDs[j];
      final int freq = postings.freqs[termID];

      // Get BytesRef
      termBytePool.setBytesRef(flushTerm, postings.textStarts[termID]);
      tv.startTerm(flushTerm, freq);
      
      if (doVectorPositions || doVectorOffsets) {
        if (posReader != null) {
          termsHashPerField.initReader(posReader, termID, 0);
        }
        if (offReader != null) {
          termsHashPerField.initReader(offReader, termID, 1);
        }
        tv.addProx(freq, posReader, offReader);
      }
      tv.finishTerm();
    }
    tv.finishField();

    termsHashPerField.reset();

    fieldInfo.setStoreTermVectors();
  }

  void shrinkHash() {
    termsHashPerField.shrinkHash(maxNumPostings);
    maxNumPostings = 0;
  }

  @Override
  void start(IndexableField f) {
    if (doVectorOffsets) {
      offsetAttribute = fieldState.attributeSource.addAttribute(OffsetAttribute.class);
    } else {
      offsetAttribute = null;
    }
    if (doVectorPayloads && fieldState.attributeSource.hasAttribute(PayloadAttribute.class)) {
      payloadAttribute = fieldState.attributeSource.getAttribute(PayloadAttribute.class);
    } else {
      payloadAttribute = null;
    }
  }
  
  void writeProx(TermVectorsPostingsArray postings, int termID) {    
    if (doVectorOffsets) {
      int startOffset = fieldState.offset + offsetAttribute.startOffset();
      int endOffset = fieldState.offset + offsetAttribute.endOffset();

      termsHashPerField.writeVInt(1, startOffset - postings.lastOffsets[termID]);
      termsHashPerField.writeVInt(1, endOffset - startOffset);
      postings.lastOffsets[termID] = endOffset;
    }

    if (doVectorPositions) {
      final BytesRef payload;
      if (payloadAttribute == null) {
        payload = null;
      } else {
        payload = payloadAttribute.getPayload();
      }
      
      final int pos = fieldState.position - postings.lastPositions[termID];
      if (payload != null && payload.length > 0) {
        termsHashPerField.writeVInt(0, (pos<<1)|1);
        termsHashPerField.writeVInt(0, payload.length);
        termsHashPerField.writeBytes(0, payload.bytes, payload.offset, payload.length);
        hasPayloads = true;
      } else {
        termsHashPerField.writeVInt(0, pos<<1);
      }
      postings.lastPositions[termID] = fieldState.position;
    }
  }

  @Override
  void newTerm(final int termID) {
    assert docState.testPoint("TermVectorsTermsWriterPerField.newTerm start");
    TermVectorsPostingsArray postings = (TermVectorsPostingsArray) termsHashPerField.postingsArray;

    postings.freqs[termID] = 1;
    postings.lastOffsets[termID] = 0;
    postings.lastPositions[termID] = 0;
    
    writeProx(postings, termID);
  }

  @Override
  void addTerm(final int termID) {
    assert docState.testPoint("TermVectorsTermsWriterPerField.addTerm start");
    TermVectorsPostingsArray postings = (TermVectorsPostingsArray) termsHashPerField.postingsArray;

    postings.freqs[termID]++;

    writeProx(postings, termID);
  }

  @Override
  void skippingLongTerm() {}

  @Override
  ParallelPostingsArray createPostingsArray(int size) {
    return new TermVectorsPostingsArray(size);
  }

  static final class TermVectorsPostingsArray extends ParallelPostingsArray {
    public TermVectorsPostingsArray(int size) {
      super(size);
      freqs = new int[size];
      lastOffsets = new int[size];
      lastPositions = new int[size];
    }

    int[] freqs;                                       // How many times this term occurred in the current doc
    int[] lastOffsets;                                 // Last offset we saw
    int[] lastPositions;                               // Last position where this term occurred

    @Override
    ParallelPostingsArray newInstance(int size) {
      return new TermVectorsPostingsArray(size);
    }

    @Override
    void copyTo(ParallelPostingsArray toArray, int numToCopy) {
      assert toArray instanceof TermVectorsPostingsArray;
      TermVectorsPostingsArray to = (TermVectorsPostingsArray) toArray;

      super.copyTo(toArray, numToCopy);

      System.arraycopy(freqs, 0, to.freqs, 0, size);
      System.arraycopy(lastOffsets, 0, to.lastOffsets, 0, size);
      System.arraycopy(lastPositions, 0, to.lastPositions, 0, size);
    }

    @Override
    int bytesPerPosting() {
      return super.bytesPerPosting() + 3 * RamUsageEstimator.NUM_BYTES_INT;
    }
  }
}
