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
import java.util.ArrayList;
import java.util.logging.Logger;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FilterIndexReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentTermPositionVector;
import org.apache.lucene.index.SegmentTermVector;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositionVector;
import org.apache.lucene.index.TermVectorOffsetInfo;
import org.apache.lucene.index.pruning.StorePruningPolicy;
import org.apache.lucene.index.pruning.TermPruningPolicy;

/**
 * This class produces a subset of the input index, by removing some postings
 * data according to rules implemented in a {@link TermPruningPolicy}, and
 * optionally it can also remove stored fields of documents according to rules
 * implemented in a {@link StorePruningPolicy}.
 */
public class PruningReader extends FilterIndexReader {
  private static final Logger LOG = Logger.getLogger(PruningReader.class.getName());
  
  protected int docCount;
  protected int vecCount;
  protected int termCount, delTermCount;
  protected int prunedVecCount, delVecCount;
  
  protected TermPruningPolicy termPolicy;
  protected StorePruningPolicy storePolicy;
  
  /**
   * Constructor.
   * @param in input reader
   * @param storePolicy implementation of {@link StorePruningPolicy} - if null
   *          then stored values will be retained as is.
   * @param termPolicy implementation of {@link TermPruningPolicy}, must not
   * be null.
   */
  public PruningReader(IndexReader in, StorePruningPolicy storePolicy,
      TermPruningPolicy termPolicy) {
    super(in);
    this.termPolicy = termPolicy;
    assert termPolicy != null;
    this.storePolicy = storePolicy;
  }
  
  @Override
  public IndexReader[] getSequentialSubReaders() {
    IndexReader[] orig = super.getSequentialSubReaders();
    if (orig == null) {
      return null;
    }
    IndexReader[] res = new IndexReader[orig.length];
    for (int i = 0; i < res.length; i++) {
      res[i] = new PruningReader(orig[i], storePolicy, termPolicy);
    }
    return res;
  }

  /**
   * Applies a {@link StorePruningPolicy} to stored fields of a document.
   */
  @Override
  public Document document(final int n, FieldSelector fieldSelector)
      throws CorruptIndexException, IOException {
    docCount++;
    if ((docCount % 10000) == 0) {
      LOG.info(" - stored fields: " + docCount + " docs.");
    }
    if (storePolicy != null) {
      return storePolicy.pruneDocument(n, fieldSelector);
    } else {
      return in.document(n, fieldSelector);
    }
  }
  
  /**
   * Applies a {@link StorePruningPolicy} to the list of available field infos.
   */
  @Override
  public FieldInfos getFieldInfos() {
    FieldInfos res = super.getFieldInfos();
    if (storePolicy == null) {
      return res;
    }
    return storePolicy.getFieldInfos(res);
  }
  
  /**
   * Applies {@link TermPruningPolicy} to terms inside term vectors.
   */
  @Override
  public TermFreqVector[] getTermFreqVectors(int docNumber) throws IOException {
    TermFreqVector[] vectors = super.getTermFreqVectors(docNumber);
    if (vectors == null) {
      return null;
    }
    ArrayList<TermFreqVector> newVectors = new ArrayList<TermFreqVector>();
    for (TermFreqVector v : vectors) {
      if (v == null) {
        continue;
      }
      if (termPolicy.pruneWholeTermVector(docNumber, v.getField())) {
        delVecCount++;
        if ((delVecCount % 10000) == 0) {
          LOG.info(" - deleted vectors: " + delVecCount);
        }
        continue;
      }
      if (v.size() == 0) {
        continue;
      }
      String[] terms = v.getTerms();
      int[] freqs = v.getTermFrequencies();
      
      int removed = termPolicy.pruneTermVectorTerms(docNumber, v.getField(), terms, freqs, v);
      if (removed > 0 && removed < terms.length) {
        String[] newTerms = new String[terms.length - removed];
        int[] newFreqs = new int[terms.length - removed];
        int j = 0;
        for (int i = 0; i < terms.length; i++) {
          if (terms[i] != null) {
            newTerms[j] = terms[i];
            newFreqs[j] = freqs[i];
            j++;
          }
        }
        // create a modified vector
        if (v instanceof TermPositionVector) {
          TermVectorOffsetInfo[][] offsets = new TermVectorOffsetInfo[terms.length - removed][];
          boolean withOffsets = false;
          j = 0;
          for (int i = 0; i < terms.length; i++) {
            if (terms[i] == null) {
              continue;
            }
            offsets[j] = ((TermPositionVector) v).getOffsets(i);
            if (offsets[j] != null && offsets[j] != TermVectorOffsetInfo.EMPTY_OFFSET_INFO) {
              withOffsets = true;
            }
            j++;
          }
          j = 0;
          int[][] positions = new int[terms.length - removed][];
          boolean withPositions = false;
          for (int i = 0; i < terms.length; i++) {
            if (terms[i] == null) {
              continue;
            }
            positions[j] = ((TermPositionVector) v).getTermPositions(i);
            if (positions[j] != null && positions[j].length > 0) {
              withPositions = true;
            }
            j++;
          }
          v = new SegmentTermPositionVector(v.getField(), newTerms, newFreqs,
              withPositions ? positions : null,
              withOffsets ? offsets : null);
        } else {
          v = new SegmentTermVector(v.getField(), newTerms, newFreqs);
        }
        newVectors.add(v);
      }
    }
    vecCount++;
    if ((vecCount % 10000) == 0) {
      LOG.info(" - vectors: " + vecCount + " docs.");
    }
    if (newVectors.size() == 0) {
      prunedVecCount++;
      if ((prunedVecCount % 1000) == 0) {
        LOG.info(" - deleted pruned vectors: " + prunedVecCount);
      }
      return null;
    }
    return newVectors.toArray(new TermFreqVector[newVectors.size()]);
  }
  
  /**
   * Applies {@link TermPruningPolicy} to term positions.
   */
  @Override
  public TermPositions termPositions() throws IOException {
    return new PruningTermPositions(in.termPositions());
  }
  
  /**
   * Applies {@link TermPruningPolicy} to term enum.
   */
  @Override
  public TermEnum terms() throws IOException {
    return new PruningTermEnum(in.terms());
  }
  
  private class PruningTermEnum extends FilterTermEnum {
    
    private PruningTermEnum(TermEnum in) {
      super(in);
    }
    
    @Override
    public boolean next() throws IOException {
      for (;;) {
        if (!super.next()) {
          // System.out.println("TE: end");
          return false;
        }
        termCount++;
        if ((termCount % 50000) == 0) {
          LOG.info(" - terms: " + termCount + " (" + term() + "), deleted: " + delTermCount);
        }
        if (termPolicy.pruneAllFieldPostings(term().field())
            || termPolicy.pruneTermEnum(in)) {
          delTermCount++;
          // System.out.println("TE: remove " + term());
          continue;
        }
        // System.out.println("TE: pass " + term());
        return true;
      }
    }
    
  }
  
  private class PruningTermPositions extends FilterTermPositions {
    
    protected Term curTerm = null;
    protected int[] positions;
    protected TermPositions tp;
    protected int curFreq;
    protected int posPos;
    
    private PruningTermPositions(TermPositions in) {
      super(in);
      tp = in;
    }
    
    @Override
    public void seek(Term t) throws IOException {
      super.seek(t);
      informPolicy(t);
    }
    
    @Override
    public void seek(TermEnum termEnum) throws IOException {
      super.seek(termEnum);
      informPolicy(termEnum.term());
    }
    
    private void informPolicy(Term t) throws IOException {
      termPolicy.initPositionsTerm(tp, t);
      curTerm = new Term(t.field(), t.text());
    }
    
    @Override
    public boolean next() throws IOException {
      for (;;) {
        positions = null;
        if (!super.next()) {
          return false;
        }
        if (termPolicy.pruneAllPositions(tp, curTerm)) {
          continue;
        }
        break;
      }
      // prepare the positions
      positions = new int[tp.freq()];
      for (int i = 0; i < positions.length; i++) {
        positions[i] = tp.nextPosition();
      }
      int pruned = termPolicy.pruneSomePositions(tp.doc(), positions, curTerm);
      if (pruned > 0) {
        int[] newPositions = new int[positions.length - pruned];
        int j = 0;
        for (int i = 0; i < positions.length; i++) {
          if (positions[i] < 0) {
            continue;
          } else {
            newPositions[j++] = positions[i];
          }
        }
        positions = newPositions;
      }
      curFreq = positions.length;
      posPos = 0;
      return true;
    }
    
    @Override
    public int nextPosition() throws IOException {
      return positions[posPos++];
    }
    
    @Override
    public int freq() {
      return curFreq;
    }
    
    @Override
    public boolean isPayloadAvailable() {
      if (!super.isPayloadAvailable()) {
        return false;
      }
      if (termPolicy.prunePayload((TermPositions) in, curTerm)) {
        return false;
      }
      return true;
    }
  }
}