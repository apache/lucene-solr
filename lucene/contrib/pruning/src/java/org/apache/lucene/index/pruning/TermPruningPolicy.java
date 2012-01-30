package org.apache.lucene.index.pruning;
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
import java.util.Collections;
import java.util.Map;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;

/**
 * Policy for producing smaller index out of an input index, by examining its terms
 * and removing from the index some or all of their data as follows:
 * <ul>
 * <li>all terms of a certain field - see {@link #pruneAllFieldPostings(String)}</li>
 * <li>all data of a certain term - see {@link #pruneTermEnum(TermEnum)}</li>
 * <li>all positions of a certain term in a certain document - see #pruneAllPositions(TermPositions, Term)</li>
 * <li>some positions of a certain term in a certain document - see #pruneSomePositions(int, int[], Term)</li>
 * </ul>
 * <p>
 * The pruned, smaller index would, for many types of queries return nearly 
 * identical top-N results as compared with the original index, but with increased performance.
 * <p>
 * Pruning of indexes is handy for producing small first-tier indexes that fit
 * completely in RAM, and store these indexes using {@link IndexWriter#addIndexes(IndexReader...)}
 * <p>
 * Interestingly, if the input index is optimized (i.e. doesn't contain deletions),
 * then the index produced via {@link IndexWriter#addIndexes(IndexReader[])} will preserve internal document
 * id-s so that they are in sync with the original index. This means that
 * all other auxiliary information not necessary for first-tier processing, such
 * as some stored fields, can also be removed, to be quickly retrieved on-demand
 * from the original index using the same internal document id. See
 * {@link StorePruningPolicy} for information about removing stored fields.
 * <p>
 * Please note that while this family of policies method produces good results for term queries it
 * often leads to poor results for phrase queries (because postings are removed
 * without considering whether they belong to an important phrase). 
 * <p>
 * Aggressive pruning policies produce smaller indexes - 
 * search performance increases, and recall decreases (i.e. search quality
 * deteriorates). 
 * <p>
 * See the following papers for a discussion of this problem and the
 * proposed solutions to improve the quality of a pruned index (not implemented
 * here):
 * <small>
 * <ul>
 * <li><a href="http://portal.acm.org/citation.cfm?id=1148235">Pruned query
 * evaluation using pre-computed impacts, V. Anh et al, ACM SIGIR 2006</a></li>
 * <li><a href="http://portal.acm.org/citation.cfm?id=1183614.1183644"> A
 * document-centric approach to static index pruning in text retrieval systems,
 * S. Buettcher et al, ACM SIGIR 2006</a></li>
 * <li><a href=" http://oak.cs.ucla.edu/~cho/papers/ntoulas-sigir07.pdf">
 * Pruning Policies for Two-Tiered Inverted Index with Correctness Guarantee, A.
 * Ntoulas et al, ACM SIGIR 2007.</a></li>
 * </ul>
 * </small>
 */
public abstract class TermPruningPolicy extends PruningPolicy {
  /** Pruning operations to be conducted on fields. */
  protected Map<String,Integer> fieldFlags;
  protected IndexReader in;
  
  /**
   * Construct a policy.
   * @param in input reader
   * @param fieldFlags a map, where keys are field names and values
   * are bitwise-OR flags of operations to be performed (see
   * {@link PruningPolicy} for more details).
   */
  protected TermPruningPolicy(IndexReader in, Map<String,Integer> fieldFlags) {
    this.in = in;
    if (fieldFlags != null) {
      this.fieldFlags = fieldFlags;
    } else {
      this.fieldFlags = Collections.emptyMap();
    }
  }
  
  /**
   * Term vector pruning.
   * @param docNumber document number
   * @param field field name
   * @return true if the complete term vector for this field should be
   * removed (as specified by {@link PruningPolicy#DEL_VECTOR} flag).
   * @throws IOException
   */
  public boolean pruneWholeTermVector(int docNumber, String field)
      throws IOException {
    if (fieldFlags.containsKey(field) && 
            (fieldFlags.get(field) & DEL_VECTOR) != 0) {
      return true;
    } else {
      return false;
    }
  }
  
  /**
   * Pruning of all postings for a field
   * @param field field name
   * @return true if all postings for all terms in this field should be
   * removed (as specified by {@link PruningPolicy#DEL_POSTINGS}).
   * @throws IOException
   */
  public boolean pruneAllFieldPostings(String field) throws IOException {
    if (fieldFlags.containsKey(field) && 
            (fieldFlags.get(field) & DEL_POSTINGS) != 0) {
      return true;
    } else {
      return false;
    }
  }
  
  /**
   * Called when moving {@link TermPositions} to a new {@link Term}.
   * @param in input term positions
   * @param t current term
   * @throws IOException
   */
  public abstract void initPositionsTerm(TermPositions in, Term t)
    throws IOException;

  /**
   * Called when checking for the presence of payload for the current
   * term at a current position
   * @param in positioned term positions
   * @param curTerm current term associated with these positions
   * @return true if the payload should be removed, false otherwise.
   */
  public boolean prunePayload(TermPositions in, Term curTerm) {
    if (fieldFlags.containsKey(curTerm.field()) &&
            (fieldFlags.get(curTerm.field()) & DEL_PAYLOADS) != 0) {
      return true;
    }
    return false;
  }

  /**
   * Pruning of individual terms in term vectors.
   * @param docNumber document number
   * @param field field name
   * @param terms array of terms
   * @param freqs array of term frequencies
   * @param v the original term frequency vector
   * @return 0 if no terms are to be removed, positive number to indicate
   * how many terms need to be removed. The same number of entries in the terms
   * array must be set to null to indicate which terms to remove.
   * @throws IOException
   */
  public abstract int pruneTermVectorTerms(int docNumber, String field,
          String[] terms, int[] freqs, TermFreqVector v) throws IOException;

  /**
   * Pruning of all postings for a term (invoked once per term).
   * @param te positioned term enum.
   * @return true if all postings for this term should be removed, false
   * otherwise.
   * @throws IOException
   */
  public abstract boolean pruneTermEnum(TermEnum te) throws IOException;

  /**
   * Prune <b>all</b> postings per term (invoked once per term per doc)
   * @param termPositions positioned term positions. Implementations MUST NOT
   * advance this by calling {@link TermPositions} methods that advance either
   * the position pointer (next, skipTo) or term pointer (seek).
   * @param t current term
   * @return true if the current posting should be removed, false otherwise.
   * @throws IOException
   */
  public abstract boolean pruneAllPositions(TermPositions termPositions, Term t)
      throws IOException;

  /**
   * Prune <b>some</b> postings per term (invoked once per term per doc).
   * @param docNum current document number
   * @param positions original term positions in the document (and indirectly
   * term frequency)
   * @param curTerm current term
   * @return 0 if no postings are to be removed, or positive number to indicate
   * how many postings need to be removed. The same number of entries in the
   * positions array must be set to -1 to indicate which positions to remove.
   */
  public abstract int pruneSomePositions(int docNum, int[] positions,
          Term curTerm);
  
}
