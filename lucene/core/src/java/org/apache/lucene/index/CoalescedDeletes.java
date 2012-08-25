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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Query;
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
     @SuppressWarnings({"unchecked","rawtypes"})
     @Override
     public Iterator<Term> iterator() {
       Iterator<Term> subs[] = new Iterator[iterables.size()];
       for (int i = 0; i < iterables.size(); i++) {
         subs[i] = iterables.get(i).iterator();
       }
       return new MergedIterator<Term>(subs);
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
}
