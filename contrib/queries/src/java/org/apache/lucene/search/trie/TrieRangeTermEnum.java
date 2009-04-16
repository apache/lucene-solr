package org.apache.lucene.search.trie;

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
import java.util.LinkedList;

import org.apache.lucene.search.FilteredTermEnum;
import org.apache.lucene.search.MultiTermQuery; // for javadocs
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

/**
 * Subclass of FilteredTermEnum for enumerating all terms that match the
 * sub-ranges for trie range queries.
 * <p>
 * WARNING: Term enumerations is not guaranteed to be always ordered by
 * {@link Term#compareTo}.
 * The ordering depends on how {@link TrieUtils#splitLongRange} and
 * {@link TrieUtils#splitIntRange} generates the sub-ranges. For
 * the {@link MultiTermQuery} ordering is not relevant.
 */
final class TrieRangeTermEnum extends FilteredTermEnum {

  private final AbstractTrieRangeQuery query;
  private final IndexReader reader;
  private final LinkedList/*<String>*/ rangeBounds = new LinkedList/*<String>*/();
  private String currentUpperBound = null;

  TrieRangeTermEnum(AbstractTrieRangeQuery query, IndexReader reader) {
    this.query = query;
    this.reader = reader;
  }

  /** Returns a range builder that must be used to feed in the sub-ranges. */
  TrieUtils.IntRangeBuilder getIntRangeBuilder() {
    return new TrieUtils.IntRangeBuilder() {
      //@Override
      public final void addRange(String minPrefixCoded, String maxPrefixCoded) {
        rangeBounds.add(minPrefixCoded);
        rangeBounds.add(maxPrefixCoded);
      }
    };
  }

  /** Returns a range builder that must be used to feed in the sub-ranges. */
  TrieUtils.LongRangeBuilder getLongRangeBuilder() {
    return new TrieUtils.LongRangeBuilder() {
      //@Override
      public final void addRange(String minPrefixCoded, String maxPrefixCoded) {
        rangeBounds.add(minPrefixCoded);
        rangeBounds.add(maxPrefixCoded);
      }
    };
  }
  
  /** After feeding the range builder call this method to initialize the enum. */
  void init() throws IOException {
    next();
  }

  //@Override
  public float difference() {
    return 1.0f;
  }
  
  /** this is a dummy, it is not used by this class. */
  //@Override
  protected boolean endEnum() {
    assert false; // should never be called
    return (currentTerm != null);
  }

  /**
   * Compares if current upper bound is reached,
   * this also updates the term count for statistics.
   * In contrast to {@link FilteredTermEnum}, a return value
   * of <code>false</code> ends iterating the current enum
   * and forwards to the next sub-range.
   */
  //@Override
  protected boolean termCompare(Term term) {
    return (term.field() == query.field && term.text().compareTo(currentUpperBound) <= 0);
  }
  
  /** Increments the enumeration to the next element.  True if one exists. */
  //@Override
  public boolean next() throws IOException {
    // if a current term exists, the actual enum is initialized:
    // try change to next term, if no such term exists, fall-through
    if (currentTerm != null) {
      assert actualEnum!=null;
      if (actualEnum.next()) {
        currentTerm = actualEnum.term();
        if (termCompare(currentTerm)) return true;
      }
    }
    // if all above fails, we go forward to the next enum,
    // if one is available
    currentTerm = null;
    if (rangeBounds.size() < 2) return false;
    // close the current enum and read next bounds
    if (actualEnum != null) {
      actualEnum.close();
      actualEnum = null;
    }
    final String lowerBound = (String)rangeBounds.removeFirst();
    this.currentUpperBound = (String)rangeBounds.removeFirst();
    // this call recursively uses next(), if no valid term in
    // next enum found.
    // if this behavior is changed/modified in the superclass,
    // this enum will not work anymore!
    setEnum(reader.terms(new Term(query.field, lowerBound)));
    return (currentTerm != null);
  }

  /** Closes the enumeration to further activity, freeing resources.  */
  //@Override
  public void close() throws IOException {
    rangeBounds.clear();
    currentUpperBound = null;
    super.close();
  }

}
