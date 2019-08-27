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
package org.apache.solr.request;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.Filter;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.BoundedTreeSet;

/**
 * A class which performs per-segment field faceting for single-valued string fields.
 */
class PerSegmentSingleValuedFaceting {

  // input params
  SolrIndexSearcher searcher;
  DocSet docs;
  String fieldName;
  int offset;
  int limit;
  int mincount;
  boolean missing;
  String sort;
  String prefix;

  private final Predicate<BytesRef> termFilter;

  Filter baseSet;

  int nThreads;

  public PerSegmentSingleValuedFaceting(SolrIndexSearcher searcher, DocSet docs, String fieldName, int offset, int limit, int mincount, boolean missing, String sort, String prefix, String contains, boolean ignoreCase) {
    this(searcher, docs, fieldName, offset, limit, mincount, missing, sort, prefix, new SubstringBytesRefFilter(contains, ignoreCase));
  }

  public PerSegmentSingleValuedFaceting(SolrIndexSearcher searcher, DocSet docs, String fieldName, int offset, int limit, int mincount, boolean missing, String sort, String prefix, Predicate<BytesRef> filter) {
    this.searcher = searcher;
    this.docs = docs;
    this.fieldName = fieldName;
    this.offset = offset;
    this.limit = limit;
    this.mincount = mincount;
    this.missing = missing;
    this.sort = sort;
    this.prefix = prefix;
    this.termFilter = filter;
  }

  public void setNumThreads(int threads) {
    nThreads = threads;
  }


  NamedList<Integer> getFacetCounts(Executor executor) throws IOException {

    CompletionService<SegFacet> completionService = new ExecutorCompletionService<>(executor);

    // reuse the translation logic to go from top level set to per-segment set
    baseSet = docs.getTopFilter();

    final List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
    // The list of pending tasks that aren't immediately submitted
    // TODO: Is there a completion service, or a delegating executor that can
    // limit the number of concurrent tasks submitted to a bigger executor?
    LinkedList<Callable<SegFacet>> pending = new LinkedList<>();

    int threads = nThreads <= 0 ? Integer.MAX_VALUE : nThreads;

    for (final LeafReaderContext leave : leaves) {
      final SegFacet segFacet = new SegFacet(leave);

      Callable<SegFacet> task = () -> {
        segFacet.countTerms();
        return segFacet;
      };

      // TODO: if limiting threads, submit by largest segment first?

      if (--threads >= 0) {
        completionService.submit(task);
      } else {
        pending.add(task);
      }
    }


    // now merge the per-segment results
    PriorityQueue<SegFacet> queue = new PriorityQueue<SegFacet>(leaves.size()) {
      @Override
      protected boolean lessThan(SegFacet a, SegFacet b) {
        return a.tempBR.compareTo(b.tempBR) < 0;
      }
    };


    boolean hasMissingCount=false;
    int missingCount=0;
    for (int i=0, c=leaves.size(); i<c; i++) {
      SegFacet seg = null;

      try {
        Future<SegFacet> future = completionService.take();        
        seg = future.get();
        if (!pending.isEmpty()) {
          completionService.submit(pending.removeFirst());
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException)cause;
        } else {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error in per-segment faceting on field: " + fieldName, cause);
        }
      }


      if (seg.startTermIndex < seg.endTermIndex) {
        if (seg.startTermIndex==-1) {
          hasMissingCount=true;
          missingCount += seg.counts[0];
          seg.pos = 0;
        } else {
          seg.pos = seg.startTermIndex;
        }
        if (seg.pos < seg.endTermIndex && (mincount < 1 || seg.hasAnyCount)) {  
          seg.tenum = seg.si.termsEnum();
          seg.tenum.seekExact(seg.pos);
          seg.tempBR = seg.tenum.term();
          queue.add(seg);
        }
      }
    }

    if (limit == 0) {
      NamedList<Integer> res = new NamedList<>();
      return finalize(res, missingCount, hasMissingCount);
    }

    FacetCollector collector;
    if (sort.equals(FacetParams.FACET_SORT_COUNT) || sort.equals(FacetParams.FACET_SORT_COUNT_LEGACY)) {
      collector = new CountSortedFacetCollector(offset, limit, mincount);
    } else {
      collector = new IndexSortedFacetCollector(offset, limit, mincount);
    }

    BytesRefBuilder val = new BytesRefBuilder();

    while (queue.size() > 0) {
      SegFacet seg = queue.top();
      
      boolean collect = termFilter == null || termFilter.test(seg.tempBR);
      
      // we will normally end up advancing the term enum for this segment
      // while still using "val", so we need to make a copy since the BytesRef
      // may be shared across calls.
      if (collect) {
        val.copyBytes(seg.tempBR);
      }
      
      int count = 0;

      do {
        if (collect) {
          count += seg.counts[seg.pos - seg.startTermIndex];
        }

        // if mincount>0 then seg.pos++ can skip ahead to the next non-zero entry.
        do{
          ++seg.pos;
        }
        while(  
            (seg.pos < seg.endTermIndex)  //stop incrementing before we run off the end
         && (seg.tenum.next() != null || true) //move term enum forward with position -- dont care about value 
         && (mincount > 0) //only skip ahead if mincount > 0
         && (seg.counts[seg.pos - seg.startTermIndex] == 0) //check zero count
        );
        
        if (seg.pos >= seg.endTermIndex) {
          queue.pop();
          seg = queue.top();
        } else {
          seg.tempBR = seg.tenum.term();
          seg = queue.updateTop();
        }
      } while (seg != null && val.get().compareTo(seg.tempBR) == 0);

      if (collect) {
        boolean stop = collector.collect(val.get(), count);
        if (stop) break;
      }
    }

    NamedList<Integer> res = collector.getFacetCounts();

    // convert labels to readable form    
    FieldType ft = searcher.getSchema().getFieldType(fieldName);
    int sz = res.size();
    for (int i=0; i<sz; i++) {
      res.setName(i, ft.indexedToReadable(res.getName(i)));
    }

    return finalize(res, missingCount, hasMissingCount);
  }

  private NamedList<Integer> finalize(NamedList<Integer> res, int missingCount, boolean hasMissingCount)
      throws IOException {
    if (missing) {
      if (!hasMissingCount) {
        missingCount = SimpleFacets.getFieldMissingCount(searcher,docs,fieldName);
      }
      res.add(null, missingCount);
    }

    return res;
  }

  class SegFacet {
    LeafReaderContext context;
    SegFacet(LeafReaderContext context) {
      this.context = context;
    }
    
    SortedDocValues si;
    int startTermIndex;
    int endTermIndex;
    int[] counts;
    
    //whether this segment has any non-zero term counts
    //used to ignore insignificant segments when mincount>0
    boolean hasAnyCount = false; 

    int pos; // only used when merging
    TermsEnum tenum; // only used when merging

    BytesRef tempBR = new BytesRef();

    void countTerms() throws IOException {
      si = DocValues.getSorted(context.reader(), fieldName);
      // SolrCore.log.info("reader= " + reader + "  FC=" + System.identityHashCode(si));

      if (prefix!=null) {
        BytesRefBuilder prefixRef = new BytesRefBuilder();
        prefixRef.copyChars(prefix);
        startTermIndex = si.lookupTerm(prefixRef.get());
        if (startTermIndex<0) startTermIndex=-startTermIndex-1;
        prefixRef.append(UnicodeUtil.BIG_TERM);
        // TODO: we could constrain the lower endpoint if we had a binarySearch method that allowed passing start/end
        endTermIndex = si.lookupTerm(prefixRef.get());
        assert endTermIndex < 0;
        endTermIndex = -endTermIndex-1;
      } else {
        startTermIndex=-1;
        endTermIndex=si.getValueCount();
      }
      final int nTerms=endTermIndex-startTermIndex;
      if (nTerms == 0) return;

      // count collection array only needs to be as big as the number of terms we are
      // going to collect counts for.
      final int[] counts = this.counts = new int[nTerms];
      DocIdSet idSet = baseSet.getDocIdSet(context, null);  // this set only includes live docs
      DocIdSetIterator iter = idSet.iterator();

      if (prefix==null) {
        // specialized version when collecting counts for all terms
        int doc;
        while ((doc = iter.nextDoc()) < DocIdSetIterator.NO_MORE_DOCS) {
          int t;
          if (si.advanceExact(doc)) {
            t = 1+si.ordValue();
          } else {
            t = 0;
          }
          hasAnyCount = hasAnyCount || t > 0; //counts[0] == missing counts
          counts[t]++;
        }
      } else {
        // version that adjusts term numbers because we aren't collecting the full range
        int doc;
        while ((doc = iter.nextDoc()) < DocIdSetIterator.NO_MORE_DOCS) {
          int term;
          if (si.advanceExact(doc)) {
            term = si.ordValue();
          } else {
            term = -1;
          }
          int arrIdx = term-startTermIndex;
          if (arrIdx>=0 && arrIdx<nTerms){
            counts[arrIdx]++;
            hasAnyCount = true;
          }
        }
      }
    }
  }

}



abstract class FacetCollector {
  /*** return true to stop collection */
  public abstract boolean collect(BytesRef term, int count);
  public abstract NamedList<Integer> getFacetCounts();
}


// This collector expects facets to be collected in index order
class CountSortedFacetCollector extends FacetCollector {
  private final CharsRefBuilder spare = new CharsRefBuilder();

  final int offset;
  final int limit;
  final int maxsize;
  final BoundedTreeSet<SimpleFacets.CountPair<String,Integer>> queue;

  int min;  // the smallest value in the top 'N' values

  public CountSortedFacetCollector(int offset, int limit, int mincount) {
    this.offset = offset;
    this.limit = limit;
    maxsize = limit>0 ? offset+limit : Integer.MAX_VALUE-1;
    queue = new BoundedTreeSet<>(maxsize);
    min=mincount-1;  // the smallest value in the top 'N' values
  }

  @Override
  public boolean collect(BytesRef term, int count) {
    if (count > min) {
      // NOTE: we use c>min rather than c>=min as an optimization because we are going in
      // index order, so we already know that the keys are ordered.  This can be very
      // important if a lot of the counts are repeated (like zero counts would be).
      spare.copyUTF8Bytes(term);
      queue.add(new SimpleFacets.CountPair<>(spare.toString(), count));
      if (queue.size()>=maxsize) min=queue.last().val;
    }
    return false;
  }

  @Override
  public NamedList<Integer> getFacetCounts() {
    NamedList<Integer> res = new NamedList<>();
    int off=offset;
    int lim=limit>=0 ? limit : Integer.MAX_VALUE;
     // now select the right page from the results
     for (SimpleFacets.CountPair<String,Integer> p : queue) {
       if (--off>=0) continue;
       if (--lim<0) break;
       res.add(p.key, p.val);
     }
    return res;
  }
}

// This collector expects facets to be collected in index order
class IndexSortedFacetCollector extends FacetCollector {
  private final CharsRefBuilder spare = new CharsRefBuilder();

  int offset;
  int limit;
  final int mincount;
  final NamedList<Integer> res = new NamedList<>();

  public IndexSortedFacetCollector(int offset, int limit, int mincount) {
    this.offset = offset;
    this.limit = limit>0 ? limit : Integer.MAX_VALUE;
    this.mincount = mincount;
  }

  @Override
  public boolean collect(BytesRef term, int count) {
    if (count < mincount) {
      return false;
    }

    if (offset > 0) {
      offset--;
      return false;
    }

    if (limit > 0) {
      spare.copyUTF8Bytes(term);
      res.add(spare.toString(), count);
      limit--;
    }

    return limit <= 0;
  }

  @Override
  public NamedList<Integer> getFacetCounts() {
    return res;
  }
}
