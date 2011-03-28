package org.apache.lucene.search;

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

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.Explanation.IDFExplanation;
import org.apache.lucene.util.TermContext;


/** 
 * Expert: Scoring API.
 *
 * <p>Similarity defines the components of Lucene scoring.
 * Overriding computation of these components is a convenient
 * way to alter Lucene scoring.
 *
 * @see org.apache.lucene.index.IndexWriterConfig#setSimilarityProvider(SimilarityProvider)
 * @see IndexSearcher#setSimilarityProvider(SimilarityProvider)
 */
public abstract class Similarity {
  
  public static final int NO_DOC_ID_PROVIDED = -1;

  /** Decodes a normalization factor stored in an index.
   * @see #encodeNormValue(float)
   */
  public abstract float decodeNormValue(byte b);

  /**
   * Computes the normalization value for a field, given the accumulated
   * state of term processing for this field (see {@link FieldInvertState}).
   * 
   * <p>Implementations should calculate a float value based on the field
   * state and then return that value.
   *
   * <p>Matches in longer fields are less precise, so implementations of this
   * method usually return smaller values when <code>state.getLength()</code> is large,
   * and larger values when <code>state.getLength()</code> is small.
   * 
   * <p>Note that the return values are computed under 
   * {@link org.apache.lucene.index.IndexWriter#addDocument(org.apache.lucene.document.Document)} 
   * and then stored using
   * {@link #encodeNormValue(float)}.  
   * Thus they have limited precision, and documents
   * must be re-indexed if this method is altered.
   *
   * @lucene.experimental
   * 
   * @param state current processing state for this field
   * @return the calculated float norm
   */
  public abstract float computeNorm(FieldInvertState state);
  
  /** Encodes a normalization factor for storage in an index.
   * 
   * @see org.apache.lucene.document.Field#setBoost(float)
   * @see org.apache.lucene.util.SmallFloat
   */
  public abstract byte encodeNormValue(float f);

  /** Computes the amount of a sloppy phrase match, based on an edit distance.
   * This value is summed for each sloppy phrase match in a document to form
   * the frequency that is passed to {@link #tf(float)}.
   *
   * <p>A phrase match with a small edit distance to a document passage more
   * closely matches the document, so implementations of this method usually
   * return larger values when the edit distance is small and smaller values
   * when it is large.
   *
   * @see PhraseQuery#setSlop(int)
   * @param distance the edit distance of this sloppy phrase match
   * @return the frequency increment for this match
   */
  public abstract float sloppyFreq(int distance);

  /**
   * Calculate a scoring factor based on the data in the payload.  Overriding implementations
   * are responsible for interpreting what is in the payload.  Lucene makes no assumptions about
   * what is in the byte array.
   * <p>
   * The default implementation returns 1.
   *
   * @param docId The docId currently being scored.  If this value is {@link #NO_DOC_ID_PROVIDED}, then it should be assumed that the PayloadQuery implementation does not provide document information
   * @param start The start position of the payload
   * @param end The end position of the payload
   * @param payload The payload byte array to be scored
   * @param offset The offset into the payload array
   * @param length The length in the array
   * @return An implementation dependent float to be used as a scoring factor
   *
   */
  // TODO: maybe switch this API to BytesRef?
  public float scorePayload(int docId, int start, int end, byte [] payload, int offset, int length)
  {
    return 1;
  }
  
  public abstract IDFExplanation computeWeight(IndexSearcher searcher, String fieldName, TermContext... termStats) throws IOException;
  
  public abstract ExactDocScorer exactDocScorer(Weight weight, String fieldName, AtomicReaderContext context) throws IOException;
  public abstract SloppyDocScorer sloppyDocScorer(Weight weight, String fieldName, AtomicReaderContext context) throws IOException;
  
  public abstract class ExactDocScorer {
    public abstract float score(int doc, int freq);
  }
  
  public abstract class SloppyDocScorer {
    public abstract float score(int doc, float freq);
  }
}
