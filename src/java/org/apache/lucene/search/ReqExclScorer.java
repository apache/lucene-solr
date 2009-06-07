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


/** A Scorer for queries with a required subscorer
 * and an excluding (prohibited) sub DocIdSetIterator.
 * <br>
 * This <code>Scorer</code> implements {@link Scorer#skipTo(int)},
 * and it uses the skipTo() on the given scorers.
 */
class ReqExclScorer extends Scorer {
  private Scorer reqScorer;
  private DocIdSetIterator exclDisi;
  private int doc = -1;

  /** Construct a <code>ReqExclScorer</code>.
   * @param reqScorer The scorer that must match, except where
   * @param exclDisi indicates exclusion.
   */
  public ReqExclScorer(Scorer reqScorer, DocIdSetIterator exclDisi) {
    super(null); // No similarity used.
    this.reqScorer = reqScorer;
    this.exclDisi = exclDisi;
  }

  /** @deprecated use {@link #nextDoc()} instead. */
  public boolean next() throws IOException {
    return nextDoc() != NO_MORE_DOCS;
  }

  public int nextDoc() throws IOException {
    if (reqScorer == null) {
      return doc;
    }
    doc = reqScorer.nextDoc();
    if (doc == NO_MORE_DOCS) {
      reqScorer = null; // exhausted, nothing left
      return doc;
    }
    if (exclDisi == null) {
      return doc;
    }
    return doc = toNonExcluded();
  }
  
  /** Advance to non excluded doc.
   * <br>On entry:
   * <ul>
   * <li>reqScorer != null,
   * <li>exclScorer != null,
   * <li>reqScorer was advanced once via next() or skipTo()
   *      and reqScorer.doc() may still be excluded.
   * </ul>
   * Advances reqScorer a non excluded required doc, if any.
   * @return true iff there is a non excluded required doc.
   */
  private int toNonExcluded() throws IOException {
    int exclDoc = exclDisi.docID();
    int reqDoc = reqScorer.docID(); // may be excluded
    do {  
      if (reqDoc < exclDoc) {
        return reqDoc; // reqScorer advanced to before exclScorer, ie. not excluded
      } else if (reqDoc > exclDoc) {
        exclDoc = exclDisi.advance(reqDoc);
        if (exclDoc == NO_MORE_DOCS) {
          exclDisi = null; // exhausted, no more exclusions
          return reqDoc;
        }
        if (exclDoc > reqDoc) {
          return reqDoc; // not excluded
        }
      }
    } while ((reqDoc = reqScorer.nextDoc()) != NO_MORE_DOCS);
    reqScorer = null; // exhausted, nothing left
    return NO_MORE_DOCS;
  }

  /** @deprecated use {@link #docID()} instead. */
  public int doc() {
    return reqScorer.doc(); // reqScorer may be null when next() or skipTo() already return false
  }
  
  public int docID() {
    return doc;
  }

  /** Returns the score of the current document matching the query.
   * Initially invalid, until {@link #next()} is called the first time.
   * @return The score of the required scorer.
   */
  public float score() throws IOException {
    return reqScorer.score(); // reqScorer may be null when next() or skipTo() already return false
  }
  
  /** @deprecated use {@link #advance(int)} instead. */
  public boolean skipTo(int target) throws IOException {
    return advance(target) != NO_MORE_DOCS;
  }

  public int advance(int target) throws IOException {
    if (reqScorer == null) {
      return doc = NO_MORE_DOCS;
    }
    if (exclDisi == null) {
      return doc = reqScorer.advance(target);
    }
    if (reqScorer.advance(target) == NO_MORE_DOCS) {
      reqScorer = null;
      return doc = NO_MORE_DOCS;
    }
    return doc = toNonExcluded();
  }
  
  public Explanation explain(int doc) throws IOException {
    Explanation res = new Explanation();
    if (exclDisi.advance(doc) == doc) {
      res.setDescription("excluded");
    } else {
      res.setDescription("not excluded");
      res.addDetail(reqScorer.explain(doc));
    }
    return res;
  }
}
