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

package org.apache.solr.search.facet;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiPostingsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.HashDocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortedIntDocSet;
import org.apache.solr.search.facet.SlotAcc.SlotContext;

/**
 * Enumerates indexed terms in order in a streaming fashion.
 * It's able to stream since no data needs to be accumulated so long as it's index order.
 */
class FacetFieldProcessorByEnumTermsStream extends FacetFieldProcessor implements Closeable {
  long bucketsToSkip;
  long bucketsReturned;

  boolean closed;
  boolean countOnly;
  boolean hasSubFacets;  // true if there are subfacets
  int minDfFilterCache;
  DocSet docs;
  DocSet fastForRandomSet;
  TermsEnum termsEnum = null;
  SolrIndexSearcher.DocsEnumState deState = null;
  PostingsEnum postingsEnum;
  BytesRef startTermBytes;
  BytesRef term;
  AtomicBoolean shardHasMoreBuckets = new AtomicBoolean(false);  // set after streaming as finished

  // at any point in processing where we need a SlotContext, all we care about is the current 'term'
  private IntFunction<SlotContext> slotContext = (slotNum) -> {
    assert null != term;
    return new SlotAcc.SlotContext(new TermQuery(new Term(sf.getName(), term)));
  };
  
  LeafReaderContext[] leaves;

  FacetFieldProcessorByEnumTermsStream(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq, sf);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      // fcontext.base.decref();  // OFF-HEAP
    }
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public void process() throws IOException {
    super.process();

    // We need to keep the fcontext open after processing is done (since we will be streaming in the response writer).
    // But if the connection is broken, we want to clean up.
    // fcontext.base.incref();  // OFF-HEAP
    fcontext.qcontext.addCloseHook(this);

    setup();
    response = new SimpleOrderedMap<>();
    response.add("buckets", new Iterator() {
      boolean retrieveNext = true;
      Object val;

      @Override
      public boolean hasNext() {
        if (retrieveNext) {
          val = nextBucket();
        }
        retrieveNext = false;
        return val != null;
      }

      @Override
      public Object next() {
        if (retrieveNext) {
          val = nextBucket();
        }
        retrieveNext = true;
        if (val == null) {
          // Last value, so clean up.  In the case that we are doing streaming facets within streaming facets,
          // the number of close hooks could grow very large, so we want to remove ourselves.
          boolean removed = fcontext.qcontext.removeCloseHook(FacetFieldProcessorByEnumTermsStream.this);
          assert removed;
          try {
            close();
          } catch (IOException e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error during facet streaming close", e);
          }
        }
        return val;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    });
    if (fcontext.isShard()) {
      response.add("more", shardHasMoreBuckets);  // lazily evaluated
    }
  }

  private void setup() throws IOException {

    countOnly = freq.facetStats.size() == 0 || freq.facetStats.values().iterator().next() instanceof CountAgg;
    hasSubFacets = freq.subFacets.size() > 0;
    bucketsToSkip = freq.offset;

    createAccs(-1, 1);

    // Minimum term docFreq in order to use the filterCache for that term.
    if (freq.cacheDf == -1) { // -1 means never cache
      minDfFilterCache = Integer.MAX_VALUE;
    } else if (freq.cacheDf == 0) { // default; compute as fraction of maxDoc
      minDfFilterCache = Math.max(fcontext.searcher.maxDoc() >> 4, 3);  // (minimum of 3 is for test coverage purposes)
    } else {
      minDfFilterCache = freq.cacheDf;
    }

    docs = fcontext.base;
    fastForRandomSet = null;

    if (freq.prefix != null) {
      String indexedPrefix = sf.getType().toInternal(freq.prefix);
      startTermBytes = new BytesRef(indexedPrefix);
    } else if (sf.getType().getNumberType() != null) {
      String triePrefix = TrieField.getMainValuePrefix(sf.getType());
      if (triePrefix != null) {
        startTermBytes = new BytesRef(triePrefix);
      }
    }

    Terms terms = fcontext.searcher.getSlowAtomicReader().terms(sf.getName());

    termsEnum = null;
    deState = null;
    term = null;


    if (terms != null) {

      termsEnum = terms.iterator();

      // TODO: OPT: if seek(ord) is supported for this termsEnum, then we could use it for
      // facet.offset when sorting by index order.

      if (startTermBytes != null) {
        if (termsEnum.seekCeil(startTermBytes) == TermsEnum.SeekStatus.END) {
          termsEnum = null;
        } else {
          term = termsEnum.term();
        }
      } else {
        // position termsEnum on first term
        term = termsEnum.next();
      }
    }

    List<LeafReaderContext> leafList = fcontext.searcher.getTopReaderContext().leaves();
    leaves = leafList.toArray( new LeafReaderContext[ leafList.size() ]);
  }

  private SimpleOrderedMap<Object> nextBucket() {
    try {
      return _nextBucket();
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error during facet streaming", e);
    }
  }

  private SimpleOrderedMap<Object> _nextBucket() throws IOException {
    DocSet termSet = null;
    
    try {
      while (term != null) {

        if (startTermBytes != null && !StringHelper.startsWith(term, startTermBytes)) {
          break;
        }

        int df = termsEnum.docFreq();
        if (df < effectiveMincount) {
          term = termsEnum.next();
          continue;
        }

        if (termSet != null) {
          // termSet.decref(); // OFF-HEAP
          termSet = null;
        }

        int c = 0;

        if (hasSubFacets || df >= minDfFilterCache) {
          // use the filter cache

          if (deState == null) {
            deState = new SolrIndexSearcher.DocsEnumState();
            deState.fieldName = sf.getName();
            deState.liveDocs = fcontext.searcher.getLiveDocsBits();
            deState.termsEnum = termsEnum;
            deState.postingsEnum = postingsEnum;
            deState.minSetSizeCached = minDfFilterCache;
          }

            if (hasSubFacets || !countOnly) {
              DocSet termsAll = fcontext.searcher.getDocSet(deState);
              termSet = docs.intersection(termsAll);
              // termsAll.decref(); // OFF-HEAP
              c = termSet.size();
            } else {
              c = fcontext.searcher.numDocs(docs, deState);
            }
            postingsEnum = deState.postingsEnum;

            resetStats();

            if (!countOnly) {
              collect(termSet, 0, slotContext);
            }

        } else {
          // We don't need the docset here (meaning no sub-facets).
          // if countOnly, then we are calculating some other stats...
          resetStats();

          // lazy convert to fastForRandomSet
          if (fastForRandomSet == null) {
            fastForRandomSet = docs;
            if (docs instanceof SortedIntDocSet) {  // OFF-HEAP todo: also check for native version
              SortedIntDocSet sset = (SortedIntDocSet) docs;
              fastForRandomSet = new HashDocSet(sset.getDocs(), 0, sset.size());
            }
          }
          // iterate over TermDocs to calculate the intersection
          postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);

          if (postingsEnum instanceof MultiPostingsEnum) {
            MultiPostingsEnum.EnumWithSlice[] subs = ((MultiPostingsEnum) postingsEnum).getSubs();
            int numSubs = ((MultiPostingsEnum) postingsEnum).getNumSubs();
            for (int subindex = 0; subindex < numSubs; subindex++) {
              MultiPostingsEnum.EnumWithSlice sub = subs[subindex];
              if (sub.postingsEnum == null) continue;
              int base = sub.slice.start;
              int docid;

              if (countOnly) {
                while ((docid = sub.postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  if (fastForRandomSet.exists(docid + base)) c++;
                }
              } else {
                setNextReader(leaves[sub.slice.readerIndex]);
                while ((docid = sub.postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  if (fastForRandomSet.exists(docid + base)) {
                    c++;
                    collect(docid, 0, slotContext);
                  }
                }
              }

            }
          } else {
            int docid;
            if (countOnly) {
              while ((docid = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (fastForRandomSet.exists(docid)) c++;
              }
            } else {
              setNextReader(leaves[0]);
              while ((docid = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (fastForRandomSet.exists(docid)) {
                  c++;
                  collect(docid, 0, slotContext);
                }
              }
            }
          }

        }

        if (c < effectiveMincount) {
          term = termsEnum.next();
          continue;
        }

        // handle offset and limit
        if (bucketsToSkip > 0) {
          bucketsToSkip--;
          term = termsEnum.next();
          continue;
        }

        if (freq.limit >= 0 && ++bucketsReturned > freq.limit) {
          shardHasMoreBuckets.set(true);
          return null;
        }

        // set count in case other stats depend on it
        countAcc.incrementCount(0, c);

        // OK, we have a good bucket to return... first get bucket value before moving to next term
        Object bucketVal = sf.getType().toObject(sf, term);
        TermQuery bucketQuery = hasSubFacets ? new TermQuery(new Term(freq.field, term)) : null;
        term = termsEnum.next();

        SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();
        bucket.add("val", bucketVal);
        addStats(bucket, 0);
        if (hasSubFacets) {
          processSubs(bucket, bucketQuery, termSet, false, null);
        }

        // TODO... termSet needs to stick around for streaming sub-facets?

        return bucket;

      }

    } finally {
      if (termSet != null) {
        // termSet.decref();  // OFF-HEAP
        termSet = null;
      }
    }

    // end of the iteration
    return null;
  }

}
