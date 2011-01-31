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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.index.BufferedDeletesStream.QueryAndLimit;

/* Holds buffered deletes, by docID, term or query for a
 * single segment. This is used to hold buffered pending
 * deletes against the to-be-flushed segment.  Once the
 * deletes are pushed (on flush in DocumentsWriter), these
 * deletes are converted to a FrozenDeletes instance. */

// NOTE: we are sync'd by BufferedDeletes, ie, all access to
// instances of this class is via sync'd methods on
// BufferedDeletes

class BufferedDeletes {

  /* Rough logic: HashMap has an array[Entry] w/ varying
     load factor (say 2 * POINTER).  Entry is object w/ Term
     key, Integer val, int hash, Entry next
     (OBJ_HEADER + 3*POINTER + INT).  Term is object w/
     String field and String text (OBJ_HEADER + 2*POINTER).
     We don't count Term's field since it's interned.
     Term's text is String (OBJ_HEADER + 4*INT + POINTER +
     OBJ_HEADER + string.length*CHAR).  Integer is
     OBJ_HEADER + INT. */
  final static int BYTES_PER_DEL_TERM = 8*RamUsageEstimator.NUM_BYTES_OBJECT_REF + 5*RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 6*RamUsageEstimator.NUM_BYTES_INT;

  /* Rough logic: del docIDs are List<Integer>.  Say list
     allocates ~2X size (2*POINTER).  Integer is OBJ_HEADER
     + int */
  final static int BYTES_PER_DEL_DOCID = 2*RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + RamUsageEstimator.NUM_BYTES_INT;

  /* Rough logic: HashMap has an array[Entry] w/ varying
     load factor (say 2 * POINTER).  Entry is object w/
     Query key, Integer val, int hash, Entry next
     (OBJ_HEADER + 3*POINTER + INT).  Query we often
     undercount (say 24 bytes).  Integer is OBJ_HEADER + INT. */
  final static int BYTES_PER_DEL_QUERY = 5*RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2*RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 2*RamUsageEstimator.NUM_BYTES_INT + 24;

  final AtomicInteger numTermDeletes = new AtomicInteger();
  final Map<Term,Integer> terms;
  final Map<Query,Integer> queries = new HashMap<Query,Integer>();
  final List<Integer> docIDs = new ArrayList<Integer>();

  public static final Integer MAX_INT = Integer.valueOf(Integer.MAX_VALUE);

  final AtomicLong bytesUsed = new AtomicLong();

  private final static boolean VERBOSE_DELETES = false;

  long gen;

  public BufferedDeletes(boolean sortTerms) {
    if (sortTerms) {
      terms = new TreeMap<Term,Integer>();
    } else {
      terms = new HashMap<Term,Integer>();
    }
  }

  @Override
  public String toString() {
    if (VERBOSE_DELETES) {
      return "gen=" + gen + " numTerms=" + numTermDeletes + ", terms=" + terms
        + ", queries=" + queries + ", docIDs=" + docIDs + ", bytesUsed="
        + bytesUsed;
    } else {
      String s = "gen=" + gen;
      if (numTermDeletes.get() != 0) {
        s += " " + numTermDeletes.get() + " deleted terms (unique count=" + terms.size() + ")";
      }
      if (queries.size() != 0) {
        s += " " + queries.size() + " deleted queries";
      }
      if (docIDs.size() != 0) {
        s += " " + docIDs.size() + " deleted docIDs";
      }
      if (bytesUsed.get() != 0) {
        s += " bytesUsed=" + bytesUsed.get();
      }

      return s;
    }
  }

  void update(BufferedDeletes in) {
    numTermDeletes.addAndGet(in.numTermDeletes.get());
    for (Map.Entry<Term,Integer> ent : in.terms.entrySet()) {
      final Term term = ent.getKey();
      if (!terms.containsKey(term)) {
        // only incr bytesUsed if this term wasn't already buffered:
        bytesUsed.addAndGet(BYTES_PER_DEL_TERM);
      }
      terms.put(term, MAX_INT);
    }

    for (Map.Entry<Query,Integer> ent : in.queries.entrySet()) {
      final Query query = ent.getKey();
      if (!queries.containsKey(query)) {
        // only incr bytesUsed if this query wasn't already buffered:
        bytesUsed.addAndGet(BYTES_PER_DEL_QUERY);
      }
      queries.put(query, MAX_INT);
    }

    // docIDs never move across segments and the docIDs
    // should already be cleared
  }

  void update(FrozenBufferedDeletes in) {
    numTermDeletes.addAndGet(in.numTermDeletes);
    for(Term term : in.terms) {
      if (!terms.containsKey(term)) {
        // only incr bytesUsed if this term wasn't already buffered:
        bytesUsed.addAndGet(BYTES_PER_DEL_TERM);
      }
      terms.put(term, MAX_INT);
    }

    for(int queryIdx=0;queryIdx<in.queries.length;queryIdx++) {
      final Query query = in.queries[queryIdx];
      if (!queries.containsKey(query)) {
        // only incr bytesUsed if this query wasn't already buffered:
        bytesUsed.addAndGet(BYTES_PER_DEL_QUERY);
      }
      queries.put(query, MAX_INT);
    }
  }

  public void addQuery(Query query, int docIDUpto) {
    Integer current = queries.put(query, docIDUpto);
    // increment bytes used only if the query wasn't added so far.
    if (current == null) {
      bytesUsed.addAndGet(BYTES_PER_DEL_QUERY);
    }
  }

  public void addDocID(int docID) {
    docIDs.add(Integer.valueOf(docID));
    bytesUsed.addAndGet(BYTES_PER_DEL_DOCID);
  }

  public void addTerm(Term term, int docIDUpto) {
    Integer current = terms.get(term);
    if (current != null && docIDUpto < current) {
      // Only record the new number if it's greater than the
      // current one.  This is important because if multiple
      // threads are replacing the same doc at nearly the
      // same time, it's possible that one thread that got a
      // higher docID is scheduled before the other
      // threads.  If we blindly replace than we can
      // incorrectly get both docs indexed.
      return;
    }

    terms.put(term, Integer.valueOf(docIDUpto));
    numTermDeletes.incrementAndGet();
    if (current == null) {
      bytesUsed.addAndGet(BYTES_PER_DEL_TERM + term.bytes.length);
    }
  }

  public Iterable<Term> termsIterable() {
    return new Iterable<Term>() {
      // @Override -- not until Java 1.6
      public Iterator<Term> iterator() {
        return terms.keySet().iterator();
      }
    };
  }

  public Iterable<QueryAndLimit> queriesIterable() {
    return new Iterable<QueryAndLimit>() {
      
      // @Override -- not until Java 1.6
      public Iterator<QueryAndLimit> iterator() {
        return new Iterator<QueryAndLimit>() {
          private final Iterator<Map.Entry<Query,Integer>> iter = queries.entrySet().iterator();

          // @Override -- not until Java 1.6
          public boolean hasNext() {
            return iter.hasNext();
          }

          // @Override -- not until Java 1.6
          public QueryAndLimit next() {
            final Map.Entry<Query,Integer> ent = iter.next();
            return new QueryAndLimit(ent.getKey(), ent.getValue());
          }

          // @Override -- not until Java 1.6
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }    
    
  void clear() {
    terms.clear();
    queries.clear();
    docIDs.clear();
    numTermDeletes.set(0);
    bytesUsed.set(0);
  }
  
  void clearDocIDs() {
    bytesUsed.addAndGet(-docIDs.size()*BYTES_PER_DEL_DOCID);
    docIDs.clear();
  }
  
  boolean any() {
    return terms.size() > 0 || docIDs.size() > 0 || queries.size() > 0;
  }
}
