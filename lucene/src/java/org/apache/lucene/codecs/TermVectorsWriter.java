package org.apache.lucene.codecs;

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
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

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
 *   <li>If offsets and/or positions are enabled, then 
 *       {@link #addPosition(int, int, int)} will be called for each term
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
  public abstract void startDocument(int numVectorFields) throws IOException;
  
  /** Called before writing the terms of the field.
   *  {@link #startTerm(BytesRef, int)} will be called <code>numTerms</code> times. */
  public abstract void startField(FieldInfo info, int numTerms, boolean positions, boolean offsets) throws IOException;
  
  /** Adds a term and its term frequency <code>freq</code>.
   * If this field has positions and/or offsets enabled, then
   * {@link #addPosition(int, int, int)} will be called 
   * <code>freq</code> times respectively.
   */
  public abstract void startTerm(BytesRef term, int freq) throws IOException;
  
  /** Adds a term position and offsets */
  public abstract void addPosition(int position, int startOffset, int endOffset) throws IOException;
  
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
  
  /** 
   * Called by IndexWriter when writing new segments.
   * <p>
   * This is an expert API that allows the codec to consume 
   * positions and offsets directly from the indexer.
   * <p>
   * The default implementation calls {@link #addPosition(int, int, int)},
   * but subclasses can override this if they want to efficiently write 
   * all the positions, then all the offsets, for example.
   * <p>
   * NOTE: This API is extremely expert and subject to change or removal!!!
   * @lucene.internal
   */
  // TODO: we should probably nuke this and make a more efficient 4.x format
  // PreFlex-RW could then be slow and buffer (its only used in tests...)
  public void addProx(int numProx, DataInput positions, DataInput offsets) throws IOException {
    int position = 0;
    int lastOffset = 0;

    for (int i = 0; i < numProx; i++) {
      final int startOffset;
      final int endOffset;
      
      if (positions == null) {
        position = -1;
      } else {
        position += positions.readVInt();
      }
      
      if (offsets == null) {
        startOffset = endOffset = -1;
      } else {
        startOffset = lastOffset + offsets.readVInt();
        endOffset = startOffset + offsets.readVInt();
        lastOffset = endOffset;
      }
      addPosition(position, startOffset, endOffset);
    }
  }
  
  /** Merges in the term vectors from the readers in 
   *  <code>mergeState</code>. The default implementation skips
   *  over deleted documents, and uses {@link #startDocument(int)},
   *  {@link #startField(FieldInfo, int, boolean, boolean)}, 
   *  {@link #startTerm(BytesRef, int)}, {@link #addPosition(int, int, int)},
   *  and {@link #finish(int)},
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
  
  /** Safe (but, slowish) default method to write every
   *  vector field in the document.  This default
   *  implementation requires that the vectors implement
   *  both Fields.getUniqueFieldCount and
   *  Terms.getUniqueTermCount. */
  protected final void addAllDocVectors(Fields vectors, FieldInfos fieldInfos) throws IOException {
    if (vectors == null) {
      startDocument(0);
      return;
    }

    final int numFields = vectors.getUniqueFieldCount();
    if (numFields == -1) {
      throw new IllegalStateException("vectors.getUniqueFieldCount() must be implemented (it returned -1)");
    }
    startDocument(numFields);
    
    final FieldsEnum fieldsEnum = vectors.iterator();
    String fieldName;
    String lastFieldName = null;

    while((fieldName = fieldsEnum.next()) != null) {
      
      final FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldName);

      assert lastFieldName == null || fieldName.compareTo(lastFieldName) > 0: "lastFieldName=" + lastFieldName + " fieldName=" + fieldName;
      lastFieldName = fieldName;

      final Terms terms = fieldsEnum.terms();
      if (terms == null) {
        // FieldsEnum shouldn't lie...
        continue;
      }
      final int numTerms = (int) terms.getUniqueTermCount();
      if (numTerms == -1) {
        throw new IllegalStateException("vector.getUniqueTermCount() must be implemented (it returned -1)");
      }

      final boolean positions;

      OffsetAttribute offsetAtt;

      final TermsEnum termsEnum = terms.iterator(null);

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
      
      startField(fieldInfo, numTerms, positions, offsetAtt != null);

      int termCount = 1;

      // NOTE: we already .next()'d the TermsEnum above, to
      // peek @ first term to see if positions/offsets are
      // present
      while(true) {
        final int freq = (int) termsEnum.totalTermFreq();
        startTerm(termsEnum.term(), freq);

        if (positions || offsetAtt != null) {
          DocsAndPositionsEnum dp = termsEnum.docsAndPositions(null, docsAndPositionsEnum);
          // TODO: add startOffset()/endOffset() to d&pEnum... this is insanity
          if (dp != docsAndPositionsEnum) {
            // producer didnt reuse, must re-pull attributes
            if (offsetAtt != null) {
              assert dp.attributes().hasAttribute(OffsetAttribute.class);
              offsetAtt = dp.attributes().getAttribute(OffsetAttribute.class);
            }
          }
          docsAndPositionsEnum = dp;
          final int docID = docsAndPositionsEnum.nextDoc();
          assert docID != DocsEnum.NO_MORE_DOCS;
          assert docsAndPositionsEnum.freq() == freq;

          for(int posUpto=0; posUpto<freq; posUpto++) {
            final int pos = docsAndPositionsEnum.nextPosition();
            final int startOffset = offsetAtt == null ? -1 : offsetAtt.startOffset();
            final int endOffset = offsetAtt == null ? -1 : offsetAtt.endOffset();
            
            addPosition(pos, startOffset, endOffset);
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
