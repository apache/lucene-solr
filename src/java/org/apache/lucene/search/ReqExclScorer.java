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


/** A Scorer for queries with a required subscorer and an excluding (prohibited) subscorer.
 * <br>
 * This <code>Scorer</code> implements {@link Scorer#skipTo(int)},
 * and it uses the skipTo() on the given scorers.
 */
public class ReqExclScorer extends Scorer {
  private Scorer reqScorer, exclScorer;

  /** Construct a <code>ReqExclScorer</code>.
   * @param reqScorer The scorer that must match, except where
   * @param exclScorer indicates exclusion.
   */
  public ReqExclScorer(
      Scorer reqScorer,
      Scorer exclScorer) {
    super(null); // No similarity used.
    this.reqScorer = reqScorer;
    this.exclScorer = exclScorer;
  }

  private boolean firstTime = true;
  
  public boolean next() throws IOException {
    if (firstTime) {
      if (! exclScorer.next()) {
        exclScorer = null; // exhausted at start
      }
      firstTime = false;
    }
    if (reqScorer == null) {
      return false;
    }
    if (! reqScorer.next()) {
      reqScorer = null; // exhausted, nothing left
      return false;
    }
    if (exclScorer == null) {
      return true; // reqScorer.next() already returned true
    }
    return toNonExcluded();
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
  private boolean toNonExcluded() throws IOException {
    int exclDoc = exclScorer.doc();
    do {  
      int reqDoc = reqScorer.doc(); // may be excluded
      if (reqDoc < exclDoc) {
        return true; // reqScorer advanced to before exclScorer, ie. not excluded
      } else if (reqDoc > exclDoc) {
        if (! exclScorer.skipTo(reqDoc)) {
          exclScorer = null; // exhausted, no more exclusions
          return true;
        }
        exclDoc = exclScorer.doc();
        if (exclDoc > reqDoc) {
          return true; // not excluded
        }
      }
    } while (reqScorer.next());
    reqScorer = null; // exhausted, nothing left
    return false;
  }

  public int doc() {
    return reqScorer.doc(); // reqScorer may be null when next() or skipTo() already return false
  }

  /** Returns the score of the current document matching the query.
   * Initially invalid, until {@link #next()} is called the first time.
   * @return The score of the required scorer.
   */
  public float score() throws IOException {
    return reqScorer.score(); // reqScorer may be null when next() or skipTo() already return false
  }
  
  /** Skips to the first match beyond the current whose document number is
   * greater than or equal to a given target.
   * <br>When this method is used the {@link #explain(int)} method should not be used.
   * @param target The target document number.
   * @return true iff there is such a match.
   */
  public boolean skipTo(int target) throws IOException {
    if (firstTime) {
      firstTime = false;
      if (! exclScorer.skipTo(target)) {
        exclScorer = null; // exhausted
      }
    }
    if (reqScorer == null) {
      return false;
    }
    if (exclScorer == null) {
      return reqScorer.skipTo(target);
    }
    if (! reqScorer.skipTo(target)) {
      reqScorer = null;
      return false;
    }
    return toNonExcluded();
  }

  public Explanation explain(int doc) throws IOException {
    Explanation res = new Explanation();
    if (exclScorer.skipTo(doc) && (exclScorer.doc() == doc)) {
      res.setDescription("excluded");
    } else {
      res.setDescription("not excluded");
      res.addDetail(reqScorer.explain(doc));
    }
    return res;
  }
}
