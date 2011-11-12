package org.apache.lucene.index.codecs;

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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Codec API for writing term vectors:
 * <p>
 * <ol>
 *   <li>For every document, {@link #startDocument(int)} is called,
 *       informing the Codec how many fields will be written.
 *   <li>{@link #startField(FieldInfo, int, boolean, boolean)} is called for 
 *       each field in the document, informing the codec how many terms
 *       will be written for that field, and whether or not positions
 *       or offsets are enabled.
 *   <li>Within each field, {@link #startTerm(BytesRef, int)} is called
 *       for each term.
 *   <li>If offsets and/or positions are enabled, then {@link #addPosition(int)}
 *       and {@link #addOffset(int, int)} will be called for each term
 *       occurrence.
 *   <li>After all documents have been written, {@link #finish(int)} 
 *       is called for verification/sanity-checks.
 *   <li>Finally the writer is closed ({@link #close()})
 * </ol>
 * 
 * @lucene.experimental
 */
public abstract class TermVectorsWriter implements Closeable {
  
  /** Called before writing the term vectors of the document.
   *  {@link #startField(FieldInfo, int, boolean, boolean)} will 
   *  be called <code>numVectorFields</code> times. Note that if term 
   *  vectors are enabled, this is called even if the document 
   *  has no vector fields, in this case <code>numVectorFields</code> 
   *  will be zero. */
  // nocommit must we pass numVectorFields...
  public abstract void startDocument(int numVectorFields) throws IOException;
  
  /** Called before writing the terms of the field.
   *  {@link #startTerm(BytesRef, int)} will be called <code>numTerms</code> times. */
  // nocommit must we pass numTerms...
  public abstract void startField(FieldInfo info, int numTerms, boolean positions, boolean offsets) throws IOException;
  
  /** Adds a term and its term frequency <code>freq</code>.
   * If this field has positions and/or offsets enabled, then
   * {@link #addPosition(int)} and/or {@link #addOffset(int, int)}
   * will be called <code>freq</code> times respectively.
   */
  public abstract void startTerm(BytesRef term, int freq) throws IOException;
  
  /** Adds a term position */
  // nocommit can we do pos & offsets together...?  ie push
  // buffering into 3x/40 impl
  public abstract void addPosition(int position) throws IOException;
  
  /** Adds term start and end offsets */
  public abstract void addOffset(int startOffset, int endOffset) throws IOException;
  
  /** Aborts writing entirely, implementation should remove
   *  any partially-written files, etc. */
  public abstract void abort();

  /** Called before {@link #close()}, passing in the number
   *  of documents that were written. Note that this is 
   *  intentionally redundant (equivalent to the number of
   *  calls to {@link #startDocument(int)}, but a Codec should
   *  check that this is the case to detect the JRE bug described 
   *  in LUCENE-1282. */
  public abstract void finish(int numDocs) throws IOException;
  
  /** Merges in the stored fields from the readers in 
   *  <code>mergeState</code>. The default implementation skips
   *  over deleted documents, and uses {@link #startDocument(int)},
   *  {@link #startField(FieldInfo, int, boolean, boolean)}, 
   *  {@link #startTerm(BytesRef, int)}, {@link #addPosition(int)},
   *  {@link #addOffset(int, int)}, and {@link #finish(int)},
   *  returning the number of documents that were written.
   *  Implementations can override this method for more sophisticated
   *  merging (bulk-byte copying, etc). */
  public int merge(MergeState mergeState) throws IOException {
    int docCount = 0;
    for (MergeState.IndexReaderAndLiveDocs reader : mergeState.readers) {
      final int maxDoc = reader.reader.maxDoc();
      final Bits liveDocs = reader.liveDocs;
      for (int docID = 0; docID < maxDoc; docID++) {
        if (liveDocs != null && !liveDocs.get(docID)) {
          // skip deleted docs
          continue;
        }
        // NOTE: it's very important to first assign to vectors then pass it to
        // termVectorsWriter.addAllDocVectors; see LUCENE-1282
        Fields vectors = reader.reader.getTermVectors(docID);
        addAllDocVectors(vectors, mergeState.fieldInfos);
        docCount++;
        mergeState.checkAbort.work(300);
      }
    }
    finish(docCount);
    return docCount;
  }
  
  /** Safe (but, slowish) default method to write every vector field in the document */
  protected final void addAllDocVectors(Fields vectors, FieldInfos fieldInfos) throws IOException {
    if (vectors == null) {
      startDocument(0);
      return;
    }

    final int numFields = vectors.getUniqueFieldCount();
    if (numFields == -1) {
      throw new IllegalStateException("vectors.getUniqueFieldCount() returned -1");
    }
    startDocument(numFields);
    
    final FieldsEnum fieldsEnum = vectors.iterator();
    String fieldName;

    while((fieldName = fieldsEnum.next()) != null) {
      
      final FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldName);

      // nocommit O(N^2) under here:
      // nocommit just cast to int right off....?  single
      // doc w/ > 2.1 B terms is surely crazy...?
      // nocommit -- must null check here
      final long numTerms = vectors.terms(fieldName).getUniqueTermCount();

      final boolean positions;
      final boolean offsets;

      final TermsEnum termsEnum = fieldsEnum.terms();
      final OffsetAttribute offsetAtt;

      DocsAndPositionsEnum docsAndPositionsEnum = null;

      if (termsEnum.next() != null) {
        assert numTerms > 0;
        docsAndPositionsEnum = termsEnum.docsAndPositions(null, null);
        if (docsAndPositionsEnum != null) {
          // has positions
          positions = true;
          if (docsAndPositionsEnum.attributes().hasAttribute(OffsetAttribute.class)) {
            offsetAtt = docsAndPositionsEnum.attributes().getAttribute(OffsetAttribute.class);
          } else {
            offsetAtt = null;
          }
        } else {
          positions = false;
          offsetAtt = null;
        }
      } else {
        // no terms in this field (hmm why is field present
        // then...?)
        assert numTerms == 0;
        positions = false;
        offsetAtt = null;
      }
      
      startField(fieldInfo, (int) numTerms, positions, offsetAtt != null);

      int[] startOffsets;
      int[] endOffsets;

      if (offsetAtt != null) {
        startOffsets = new int[4];
        endOffsets = new int[4];
      } else {
        startOffsets = null;
        endOffsets = null;
      }

      long termCount = 1;

      // NOTE: we already .next()'d the TermsEnum above, to
      // peek @ first term to see if positions/offsets are
      // present
      while(true) {
        final int freq = (int) termsEnum.totalTermFreq();
        startTerm(termsEnum.term(), freq);

        if (positions || offsetAtt != null) {
          docsAndPositionsEnum = termsEnum.docsAndPositions(null, docsAndPositionsEnum);
          final int docID = docsAndPositionsEnum.nextDoc();
          assert docID != DocsEnum.NO_MORE_DOCS;
          assert docsAndPositionsEnum.freq() == freq;
          if (positions && offsetAtt != null && startOffsets.length < freq) {
            startOffsets = new int[ArrayUtil.oversize(freq, RamUsageEstimator.NUM_BYTES_INT)];
            endOffsets = new int[ArrayUtil.oversize(freq, RamUsageEstimator.NUM_BYTES_INT)];
          }

          for(int posUpto=0; posUpto<freq; posUpto++) {
            final int pos = docsAndPositionsEnum.nextPosition();
            if (positions) {
              addPosition(pos);
              if (offsetAtt != null) {
                // Must buffer up offsets and write in 2nd
                // pass, until we fix file format:
                startOffsets[posUpto] = offsetAtt.startOffset();
                endOffsets[posUpto] = offsetAtt.endOffset();
              }
            } else {
              // Don't buffer, just add live:
              addOffset(offsetAtt.startOffset(),
                        offsetAtt.endOffset());
            }
          }

          if (positions && offsetAtt != null) {
            // 2nd pass to write offsets:
            for(int posUpto=0; posUpto<freq; posUpto++) {
              addOffset(startOffsets[posUpto],
                        endOffsets[posUpto]);
            }
          }
        }
        
        if (termsEnum.next() == null) {
          assert termCount == numTerms;
          break;
        }
        termCount++;
      }
    }
  }
}
