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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.SchemaField;

/**
 * Base class for DV/UIF accumulating counts into an array by ordinal.
 * It can handle terms (strings), not numbers directly but those encoded as terms, and is multi-valued capable.
 */
abstract class FacetFieldProcessorByArray extends FacetFieldProcessor {
  BytesRefBuilder prefixRef;
  int startTermIndex;
  int endTermIndex;
  int nTerms;
  int nDocs;
  int maxSlots;

  int allBucketsSlot = -1;  // slot for the primary Accs (countAcc, collectAcc)

  FacetFieldProcessorByArray(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq, sf);
  }

  abstract protected void findStartAndEndOrds() throws IOException;

  abstract protected void collectDocs() throws IOException;

  /** this BytesRef may be shared across calls and should be deep-cloned if necessary */
  abstract protected BytesRef lookupOrd(int ord) throws IOException;

  @Override
  public void process() throws IOException {
    super.process();
    sf = fcontext.searcher.getSchema().getField(freq.field);
    response = getFieldCacheCounts();
  }

  private SimpleOrderedMap<Object> getFieldCacheCounts() throws IOException {
    String prefix = freq.prefix;
    if (prefix == null || prefix.length() == 0) {
      prefixRef = null;
    } else {
      prefixRef = new BytesRefBuilder();
      prefixRef.copyChars(prefix);
    }

    findStartAndEndOrds();

    maxSlots = nTerms;

    if (freq.allBuckets) {
      allBucketsSlot = maxSlots++;
    }

    createCollectAcc(nDocs, maxSlots);

    if (freq.allBuckets) {
      allBucketsAcc = new SpecialSlotAcc(fcontext, collectAcc, allBucketsSlot, otherAccs, 0);
    }

    collectDocs();

    return findTopSlots();
  }

  private SimpleOrderedMap<Object> findTopSlots() throws IOException {
    SimpleOrderedMap<Object> res = new SimpleOrderedMap<>();

    int numBuckets = 0;
    List<Object> bucketVals = null;
    if (freq.numBuckets && fcontext.isShard()) {
      bucketVals = new ArrayList<>(100);
    }

    int off = fcontext.isShard() ? 0 : (int) freq.offset;
    // add a modest amount of over-request if this is a shard request
    int lim = freq.limit >= 0 ? (fcontext.isShard() ? (int)(freq.limit*1.1+4) : (int)freq.limit) : Integer.MAX_VALUE;

    int maxsize = (int)(freq.limit >= 0 ?  freq.offset + lim : Integer.MAX_VALUE - 1);
    maxsize = Math.min(maxsize, nTerms);

    final int sortMul = freq.sortDirection.getMultiplier();
    final SlotAcc sortAcc = this.sortAcc;

    PriorityQueue<Slot> queue = new PriorityQueue<Slot>(maxsize) {
      @Override
      protected boolean lessThan(Slot a, Slot b) {
        int cmp = sortAcc.compare(a.slot, b.slot) * sortMul;
        return cmp == 0 ? b.slot < a.slot : cmp < 0;
      }
    };

    Slot bottom = null;
    for (int i = 0; i < nTerms; i++) {
      // screen out buckets not matching mincount immediately (i.e. don't even increment numBuckets)
      if (effectiveMincount > 0 && countAcc.getCount(i) < effectiveMincount) {
        continue;
      }

      numBuckets++;
      if (bucketVals != null && bucketVals.size()<100) {
        int ord = startTermIndex + i;
        BytesRef br = lookupOrd(ord);
        Object val = sf.getType().toObject(sf, br);
        bucketVals.add(val);
      }

      if (bottom != null) {
        if (sortAcc.compare(bottom.slot, i) * sortMul < 0) {
          bottom.slot = i;
          bottom = queue.updateTop();
        }
      } else if (lim > 0) {
        // queue not full
        Slot s = new Slot();
        s.slot = i;
        queue.add(s);
        if (queue.size() >= maxsize) {
          bottom = queue.top();
        }
      }
    }

    if (freq.numBuckets) {
      if (!fcontext.isShard()) {
        res.add("numBuckets", numBuckets);
      } else {
        SimpleOrderedMap<Object> map = new SimpleOrderedMap<>(2);
        map.add("numBuckets", numBuckets);
        map.add("vals", bucketVals);
        res.add("numBuckets", map);
      }
    }

    FacetDebugInfo fdebug = fcontext.getDebugInfo();
    if (fdebug != null) fdebug.putInfoItem("numBuckets", (long) numBuckets);

    // if we are deep paging, we don't have to order the highest "offset" counts.
    int collectCount = Math.max(0, queue.size() - off);
    assert collectCount <= lim;
    int[] sortedSlots = new int[collectCount];
    for (int i = collectCount - 1; i >= 0; i--) {
      sortedSlots[i] = queue.pop().slot;
    }

    if (freq.allBuckets) {
      SimpleOrderedMap<Object> allBuckets = new SimpleOrderedMap<>();
      allBuckets.add("count", allBucketsAcc.getSpecialCount());
      if (allBucketsAcc != null) {
        allBucketsAcc.setValues(allBuckets, allBucketsSlot);
      }
      res.add("allBuckets", allBuckets);
    }

    ArrayList<SimpleOrderedMap<Object>> bucketList = new ArrayList<>(collectCount);
    res.add("buckets", bucketList);

    // TODO: do this with a callback instead?
    boolean needFilter = deferredAggs != null || freq.getSubFacets().size() > 0;

    for (int slotNum : sortedSlots) {
      SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();

      // get the ord of the slot...
      int ord = startTermIndex + slotNum;

      BytesRef br = lookupOrd(ord);
      Object val = sf.getType().toObject(sf, br);

      bucket.add("val", val);

      TermQuery filter = needFilter ? new TermQuery(new Term(sf.getName(), br)) : null;
      fillBucket(bucket, countAcc.getCount(slotNum), slotNum, null, filter);

      bucketList.add(bucket);
    }

    if (freq.missing) {
      SimpleOrderedMap<Object> missingBucket = new SimpleOrderedMap<>();
      fillBucket(missingBucket, getFieldMissingQuery(fcontext.searcher, freq.field), null);
      res.add("missing", missingBucket);
    }

    return res;
  }

}
