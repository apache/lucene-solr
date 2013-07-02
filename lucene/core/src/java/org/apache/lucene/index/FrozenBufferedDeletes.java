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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.lucene.index.BufferedDeletesStream.QueryAndLimit;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/** Holds buffered deletes by term or query, once pushed.
 *  Pushed deletes are write-once, so we shift to more
 *  memory efficient data structure to hold them.  We don't
 *  hold docIDs because these are applied on flush. */

class FrozenBufferedDeletes {

  /* Query we often undercount (say 24 bytes), plus int. */
  final static int BYTES_PER_DEL_QUERY = RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_INT + 24;

  final static List<Term> EMPTY_LIST = new ArrayList<Term>(0);
  // Terms, in sorted order:
  final PrefixCodedTerms terms;
  int termCount; // just for debugging

  // Parallel array of deleted query, and the docIDUpto for
  // each
  final Query[] queries;
  final int[] queryLimits;
  final int bytesUsed;
  final int numTermDeletes;
  private long gen = -1; // assigned by BufferedDeletesStream once pushed
  final boolean isSegmentPrivate;  // set to true iff this frozen packet represents 
                                   // a segment private deletes. in that case is should
                                   // only have Queries 
  
  // A sorted set of updates
  final SortedSet<FieldsUpdate> allUpdates;

  public FrozenBufferedDeletes(BufferedDeletes deletes,
      BufferedUpdates updates, boolean isSegmentPrivate) {
    this.isSegmentPrivate = isSegmentPrivate;
    int localBytesUsed = 0;

    // freeze deletes
    if (deletes != null) {
      // arrange terms and queries in arrays
      assert !isSegmentPrivate || deletes.terms.size() == 0 : "segment private package should only have del queries";
      Term termsArray[] = deletes.terms.keySet().toArray(
          new Term[deletes.terms.size()]);
      termCount = termsArray.length;
      ArrayUtil.timSort(termsArray);
      PrefixCodedTerms.Builder builder = new PrefixCodedTerms.Builder();
      for (Term term : termsArray) {
        builder.add(term);
      }
      terms = builder.finish();
      localBytesUsed += (int) terms.getSizeInBytes();
      
      queries = new Query[deletes.queries.size()];
      queryLimits = new int[deletes.queries.size()];
      int upto = 0;
      for (Map.Entry<Query,Integer> ent : deletes.queries.entrySet()) {
        queries[upto] = ent.getKey();
        queryLimits[upto] = ent.getValue();
        upto++;
      }
      
      localBytesUsed += queries.length * BYTES_PER_DEL_QUERY;
      numTermDeletes = deletes.numTermDeletes.get();
    } else { 
      terms = null;
      numTermDeletes = 0;
      queries = null;
      queryLimits = null;
    }
    
    // freeze updates
    if (updates == null || updates.terms.isEmpty()) {
      allUpdates = null;
    } else {
      allUpdates = new TreeSet<>();
      for (List<FieldsUpdate> list : updates.terms.values()) {
        allUpdates.addAll(list);
      }
      localBytesUsed += updates.bytesUsed.get();
    }
    
    bytesUsed = localBytesUsed;
  }
  
  public void setDelGen(long gen) {
    assert this.gen == -1;
    this.gen = gen;
  }
  
  public long delGen() {
    assert gen != -1;
    return gen;
  }

  public Iterable<Term> termsIterable() {
    return new Iterable<Term>() {
      @Override
      public Iterator<Term> iterator() {
        if (terms == null) {
          return EMPTY_LIST.iterator();
        }
        return terms.iterator();
      }
    };
  }

  public Iterable<QueryAndLimit> queriesIterable() {
    return new Iterable<QueryAndLimit>() {
      @Override
      public Iterator<QueryAndLimit> iterator() {
        return new Iterator<QueryAndLimit>() {
          private int upto;

          @Override
          public boolean hasNext() {
            return upto < queries.length;
          }

          @Override
          public QueryAndLimit next() {
            QueryAndLimit ret = new QueryAndLimit(queries[upto], queryLimits[upto]);
            upto++;
            return ret;
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  @Override
  public String toString() {
    String s = "";
    if (numTermDeletes != 0) {
      s += " " + numTermDeletes + " deleted terms (unique count=" + termCount + ")";
    }
    if (queries != null && queries.length != 0) {
      s += " " + queries.length + " deleted queries";
    }
    if (allUpdates != null && !allUpdates.isEmpty()) {
      s += " " + allUpdates.size() + " updates";
    }
    if (bytesUsed != 0) {
      s += " bytesUsed=" + bytesUsed;
    }

    return s;
  }
  
  boolean anyDeletes() {
    return termCount > 0 || (queries != null && queries.length > 0);
  }
  
  boolean anyUpdates() {
    return allUpdates != null;
  }
}
