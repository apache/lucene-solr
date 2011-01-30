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

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

final class TermVectorsTermsWriterPerField extends TermsHashConsumerPerField {

  final TermVectorsTermsWriterPerThread perThread;
  final TermsHashPerField termsHashPerField;
  final TermVectorsTermsWriter termsWriter;
  final FieldInfo fieldInfo;
  final DocumentsWriter.DocState docState;
  final FieldInvertState fieldState;

  boolean doVectors;
  boolean doVectorPositions;
  boolean doVectorOffsets;

  int maxNumPostings;
  OffsetAttribute offsetAttribute = null;
  
  public TermVectorsTermsWriterPerField(TermsHashPerField termsHashPerField, TermVectorsTermsWriterPerThread perThread, FieldInfo fieldInfo) {
    this.termsHashPerField = termsHashPerField;
    this.perThread = perThread;
    this.termsWriter = perThread.termsWriter;
    this.fieldInfo = fieldInfo;
    docState = termsHashPerField.docState;
    fieldState = termsHashPerField.fieldState;
  }

  @Override
  int getStreamCount() {
    return 2;
  }

  @Override
  boolean start(Fieldable[] fields, int count) {
    doVectors = false;
    doVectorPositions = false;
    doVectorOffsets = false;

    for(int i=0;i<count;i++) {
      Fieldable field = fields[i];
      if (field.isIndexed() && field.isTermVectorStored()) {
        doVectors = true;
        doVectorPositions |= field.isStorePositionWithTermVector();
        doVectorOffsets |= field.isStoreOffsetWithTermVector();
      }
    }

    if (doVectors) {
      if (perThread.doc == null) {
        perThread.doc = termsWriter.getPerDoc();
        perThread.doc.docID = docState.docID;
        assert perThread.doc.numVectorFields == 0;
        assert 0 == perThread.doc.perDocTvf.length();
        assert 0 == perThread.doc.perDocTvf.getFilePointer();
      }

      assert perThread.doc.docID == docState.docID;

      if (termsHashPerField.bytesHash.size() != 0) {
        // Only necessary if previous doc hit a
        // non-aborting exception while writing vectors in
        // this field:
        termsHashPerField.reset();
        perThread.termsHashPerThread.reset(false);
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
   *  the real term vectors files in the Directory. */
  @Override
  void finish() throws IOException {

    assert docState.testPoint("TermVectorsTermsWriterPerField.finish start");

    final int numPostings = termsHashPerField.bytesHash.size();

    final BytesRef flushTerm = perThread.flushTerm;

    assert numPostings >= 0;

    if (!doVectors || numPostings == 0)
      return;

    if (numPostings > maxNumPostings)
      maxNumPostings = numPostings;

    final IndexOutput tvf = perThread.doc.perDocTvf;

    // This is called once, after inverting all occurrences
    // of a given field in the doc.  At this point we flush
    // our hash into the DocWriter.

    assert fieldInfo.storeTermVector;
    assert perThread.vectorFieldsInOrder(fieldInfo);

    perThread.doc.addField(termsHashPerField.fieldInfo.number);
    TermVectorsPostingsArray postings = (TermVectorsPostingsArray) termsHashPerField.postingsArray;

    // TODO: we may want to make this sort in same order
    // as Codec's terms dict?
    final int[] termIDs = termsHashPerField.sortPostings(BytesRef.getUTF8SortedAsUnicodeComparator());

    tvf.writeVInt(numPostings);
    byte bits = 0x0;
    if (doVectorPositions)
      bits |= TermVectorsReader.STORE_POSITIONS_WITH_TERMVECTOR;
    if (doVectorOffsets) 
      bits |= TermVectorsReader.STORE_OFFSET_WITH_TERMVECTOR;
    tvf.writeByte(bits);

    int lastLen = 0;
    byte[] lastBytes = null;
    int lastStart = 0;
      
    final ByteSliceReader reader = perThread.vectorSliceReader;
    final ByteBlockPool termBytePool = perThread.termsHashPerThread.termBytePool;

    for(int j=0;j<numPostings;j++) {
      final int termID = termIDs[j];
      final int freq = postings.freqs[termID];
          
      // Get BytesRef
      termBytePool.setBytesRef(flushTerm, postings.textStarts[termID]);

      // Compute common byte prefix between last term and
      // this term
      int prefix = 0;
      if (j > 0) {
        while(prefix < lastLen && prefix < flushTerm.length) {
          if (lastBytes[lastStart+prefix] != flushTerm.bytes[flushTerm.offset+prefix]) {
            break;
          }
          prefix++;
        }
      }

      lastLen = flushTerm.length;
      lastBytes = flushTerm.bytes;
      lastStart = flushTerm.offset;

      final int suffix = flushTerm.length - prefix;
      tvf.writeVInt(prefix);
      tvf.writeVInt(suffix);
      tvf.writeBytes(flushTerm.bytes, lastStart+prefix, suffix);
      tvf.writeVInt(freq);

      if (doVectorPositions) {
        termsHashPerField.initReader(reader, termID, 0);
        reader.writeTo(tvf);
      }

      if (doVectorOffsets) {
        termsHashPerField.initReader(reader, termID, 1);
        reader.writeTo(tvf);
      }
    }

    termsHashPerField.reset();

    // NOTE: we clear, per-field, at the thread level,
    // because term vectors fully write themselves on each
    // field; this saves RAM (eg if large doc has two large
    // fields w/ term vectors on) because we recycle/reuse
    // all RAM after each field:
    perThread.termsHashPerThread.reset(false);
  }

  void shrinkHash() {
    termsHashPerField.shrinkHash(maxNumPostings);
    maxNumPostings = 0;
  }
  
  @Override
  void start(Fieldable f) {
    if (doVectorOffsets) {
      offsetAttribute = fieldState.attributeSource.addAttribute(OffsetAttribute.class);
    } else {
      offsetAttribute = null;
    }
  }

  @Override
  void newTerm(final int termID) {
    assert docState.testPoint("TermVectorsTermsWriterPerField.newTerm start");
    TermVectorsPostingsArray postings = (TermVectorsPostingsArray) termsHashPerField.postingsArray;

    postings.freqs[termID] = 1;

    if (doVectorOffsets) {
      int startOffset = fieldState.offset + offsetAttribute.startOffset();
      int endOffset = fieldState.offset + offsetAttribute.endOffset();
      
      termsHashPerField.writeVInt(1, startOffset);
      termsHashPerField.writeVInt(1, endOffset - startOffset);
      postings.lastOffsets[termID] = endOffset;
    }

    if (doVectorPositions) {
      termsHashPerField.writeVInt(0, fieldState.position);
      postings.lastPositions[termID] = fieldState.position;
    }
  }

  @Override
  void addTerm(final int termID) {

    assert docState.testPoint("TermVectorsTermsWriterPerField.addTerm start");

    TermVectorsPostingsArray postings = (TermVectorsPostingsArray) termsHashPerField.postingsArray;
    
    postings.freqs[termID]++;

    if (doVectorOffsets) {
      int startOffset = fieldState.offset + offsetAttribute.startOffset();
      int endOffset = fieldState.offset + offsetAttribute.endOffset();
      
      termsHashPerField.writeVInt(1, startOffset - postings.lastOffsets[termID]);
      termsHashPerField.writeVInt(1, endOffset - startOffset);
      postings.lastOffsets[termID] = endOffset;
    }

    if (doVectorPositions) {
      termsHashPerField.writeVInt(0, fieldState.position - postings.lastPositions[termID]);
      postings.lastPositions[termID] = fieldState.position;
    }
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
