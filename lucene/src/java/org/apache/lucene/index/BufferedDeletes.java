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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.lucene.search.Query;

/** Holds buffered deletes, by docID, term or query.  We
 *  hold two instances of this class: one for the deletes
 *  prior to the last flush, the other for deletes after
 *  the last flush.  This is so if we need to abort
 *  (discard all buffered docs) we can also discard the
 *  buffered deletes yet keep the deletes done during
 *  previously flushed segments. */
class BufferedDeletes {
  int numTerms;
  Map<Term,Num> terms;
  Map<Query,Integer> queries = new HashMap<Query,Integer>();
  List<Integer> docIDs = new ArrayList<Integer>();
  long bytesUsed;
  private final boolean doTermSort;

  public BufferedDeletes(boolean doTermSort) {
    this.doTermSort = doTermSort;
    if (doTermSort) {
      terms = new TreeMap<Term,Num>();
    } else {
      terms = new HashMap<Term,Num>();
    }
  }

  // Number of documents a delete term applies to.
  final static class Num {
    private int num;

    Num(int num) {
      this.num = num;
    }

    int getNum() {
      return num;
    }

    void setNum(int num) {
      // Only record the new number if it's greater than the
      // current one.  This is important because if multiple
      // threads are replacing the same doc at nearly the
      // same time, it's possible that one thread that got a
      // higher docID is scheduled before the other
      // threads.
      if (num > this.num)
        this.num = num;
    }
  }

  int size() {
    // We use numTerms not terms.size() intentionally, so
    // that deletes by the same term multiple times "count",
    // ie if you ask to flush every 1000 deletes then even
    // dup'd terms are counted towards that 1000
    return numTerms + queries.size() + docIDs.size();
  }

  void update(BufferedDeletes in) {
    numTerms += in.numTerms;
    bytesUsed += in.bytesUsed;
    terms.putAll(in.terms);
    queries.putAll(in.queries);
    docIDs.addAll(in.docIDs);
    in.clear();
  }
    
  void clear() {
    terms.clear();
    queries.clear();
    docIDs.clear();
    numTerms = 0;
    bytesUsed = 0;
  }

  void addBytesUsed(long b) {
    bytesUsed += b;
  }

  boolean any() {
    return terms.size() > 0 || docIDs.size() > 0 || queries.size() > 0;
  }

  // Remaps all buffered deletes based on a completed
  // merge
  synchronized void remap(MergeDocIDRemapper mapper,
                          SegmentInfos infos,
                          int[][] docMaps,
                          int[] delCounts,
                          MergePolicy.OneMerge merge,
                          int mergeDocCount) {

    final Map<Term,Num> newDeleteTerms;

    // Remap delete-by-term
    if (terms.size() > 0) {
      if (doTermSort) {
        newDeleteTerms = new TreeMap<Term,Num>();
      } else {
        newDeleteTerms = new HashMap<Term,Num>();
      }
      for(Entry<Term,Num> entry : terms.entrySet()) {
        Num num = entry.getValue();
        newDeleteTerms.put(entry.getKey(),
                           new Num(mapper.remap(num.getNum())));
      }
    } else 
      newDeleteTerms = null;
    

    // Remap delete-by-docID
    final List<Integer> newDeleteDocIDs;

    if (docIDs.size() > 0) {
      newDeleteDocIDs = new ArrayList<Integer>(docIDs.size());
      for (Integer num : docIDs) {
        newDeleteDocIDs.add(Integer.valueOf(mapper.remap(num.intValue())));
      }
    } else 
      newDeleteDocIDs = null;
    

    // Remap delete-by-query
    final HashMap<Query,Integer> newDeleteQueries;
    
    if (queries.size() > 0) {
      newDeleteQueries = new HashMap<Query, Integer>(queries.size());
      for(Entry<Query,Integer> entry: queries.entrySet()) {
        Integer num = entry.getValue();
        newDeleteQueries.put(entry.getKey(),
                             Integer.valueOf(mapper.remap(num.intValue())));
      }
    } else
      newDeleteQueries = null;

    if (newDeleteTerms != null)
      terms = newDeleteTerms;
    if (newDeleteDocIDs != null)
      docIDs = newDeleteDocIDs;
    if (newDeleteQueries != null)
      queries = newDeleteQueries;
  }
}