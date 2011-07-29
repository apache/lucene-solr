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

import org.apache.lucene.search.Query;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.index.BufferedDeletesStream.QueryAndLimit;

class CoalescedDeletes {
  final Map<Query,Integer> queries = new HashMap<Query,Integer>();
  final List<Iterable<Term>> iterables = new ArrayList<Iterable<Term>>();

  @Override
  public String toString() {
    // note: we could add/collect more debugging information
    return "CoalescedDeletes(termSets=" + iterables.size() + ",queries=" + queries.size() + ")";
  }

  void update(FrozenBufferedDeletes in) {
    iterables.add(in.termsIterable());

    for(int queryIdx=0;queryIdx<in.queries.length;queryIdx++) {
      final Query query = in.queries[queryIdx];
      queries.put(query, BufferedDeletes.MAX_INT);
    }
  }

 public Iterable<Term> termsIterable() {
   return new Iterable<Term>() {
     @Override
     public Iterator<Term> iterator() {
       ArrayList<Iterator<Term>> subs = new ArrayList<Iterator<Term>>(iterables.size());
       for (Iterable<Term> iterable : iterables) {
         subs.add(iterable.iterator());
       }
       return mergedIterator(subs);
     }
   };
  }

  public Iterable<QueryAndLimit> queriesIterable() {
    return new Iterable<QueryAndLimit>() {
      
      @Override
      public Iterator<QueryAndLimit> iterator() {
        return new Iterator<QueryAndLimit>() {
          private final Iterator<Map.Entry<Query,Integer>> iter = queries.entrySet().iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public QueryAndLimit next() {
            final Map.Entry<Query,Integer> ent = iter.next();
            return new QueryAndLimit(ent.getKey(), ent.getValue());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }
  
  /** provides a merged view across multiple iterators */
  static Iterator<Term> mergedIterator(final List<Iterator<Term>> iterators) {
    return new Iterator<Term>() {
      Term current;
      TermMergeQueue queue = new TermMergeQueue(iterators.size());
      SubIterator[] top = new SubIterator[iterators.size()];
      int numTop;
      
      {
        int index = 0;
        for (Iterator<Term> iterator : iterators) {
          if (iterator.hasNext()) {
            SubIterator sub = new SubIterator();
            sub.current = iterator.next();
            sub.iterator = iterator;
            sub.index = index++;
            queue.add(sub);
          }
        }
      }
      
      public boolean hasNext() {
        if (queue.size() > 0) {
          return true;
        }
        
        for (int i = 0; i < numTop; i++) {
          if (top[i].iterator.hasNext()) {
            return true;
          }
        }
        return false;
      }
      
      public Term next() {
        // restore queue
        pushTop();
        
        // gather equal top fields
        if (queue.size() > 0) {
          pullTop();
        } else {
          current = null;
        }
        return current;
      }
      
      public void remove() {
        throw new UnsupportedOperationException();
      }
      
      private void pullTop() {
        // extract all subs from the queue that have the same top term
        assert numTop == 0;
        while (true) {
          top[numTop++] = queue.pop();
          if (queue.size() == 0
              || !(queue.top()).current.equals(top[0].current)) {
            break;
          }
        }
        current = top[0].current;
      }
      
      private void pushTop() {
        // call next() on each top, and put back into queue
        for (int i = 0; i < numTop; i++) {
          if (top[i].iterator.hasNext()) {
            top[i].current = top[i].iterator.next();
            queue.add(top[i]);
          } else {
            // no more terms
            top[i].current = null;
          }
        }
        numTop = 0;
      }
    };
  }
  
  private static class SubIterator {
    Iterator<Term> iterator;
    Term current;
    int index;
  }
  
  private static class TermMergeQueue extends PriorityQueue<SubIterator> {
    TermMergeQueue(int size) {
      super(size);
    }

    @Override
    protected boolean lessThan(SubIterator a, SubIterator b) {
      final int cmp = a.current.compareTo(b.current);
      if (cmp != 0) {
        return cmp < 0;
      } else {
        return a.index < b.index;
      }
    }
  }
}
