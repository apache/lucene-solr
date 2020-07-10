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
import java.util.Date;
import java.util.function.IntFunction;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.search.Query;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.facet.SlotAcc.SlotContext;
import org.apache.solr.search.facet.SlotAcc.SweepingCountSlotAcc;

import static org.apache.solr.search.facet.FacetContext.SKIP_FACET;

/**
 * Base class for DV/UIF accumulating counts into an array by ordinal.  It's
 * for {@link org.apache.lucene.index.SortedDocValues} and {@link org.apache.lucene.index.SortedSetDocValues} only.
 * It can handle terms (strings), not numbers directly but those encoded as terms, and is multi-valued capable.
 * By default, this class assumes subclasses can support sweeping collection unless subclasses initialize <code>countAcc</code> directly in their constructors.
 * 
 * @see SweepingCountSlotAcc
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


  /**
   * {@inheritDoc}
   *
   * This impl first initializes <code>countAcc</code> as a {@link SweepingCountSlotAcc} if null.
   */
  @Override
  protected void createAccs(int docCount, int slotCount) throws IOException {
    if (countAcc == null) {
      countAcc = new SweepingCountSlotAcc(slotCount, this);
    }
    super.createAccs(docCount, slotCount);
  }

  /**
   * {@inheritDoc}
   *
   * This impl first initializes <code>countAcc</code> as a {@link SweepingCountSlotAcc} if null.
   */
  @Override
  void createCollectAcc(int numDocs, int numSlots) throws IOException {
    if (countAcc == null) {
      countAcc = new SweepingCountSlotAcc(numSlots, this);
    }
    super.createCollectAcc(numDocs, numSlots);
    registerSweepingAccIfSupportedByCollectAcc();
  }

  @Override
  public void process() throws IOException {
    super.process();
    response = calcFacets();
  }

  private SimpleOrderedMap<Object> calcFacets() throws IOException {
    SimpleOrderedMap<Object> refineResult = null;
    boolean skipThisFacet = (fcontext.flags & SKIP_FACET) != 0;

    if (fcontext.facetInfo != null) {
      refineResult = refineFacets();
      // if we've seen this facet bucket, then refining can be done.  If we haven't, we still
      // only need to continue if we need allBuckets or numBuckets info.
      if (skipThisFacet || !freq.allBuckets) return refineResult;
    }

    String prefix = freq.prefix;
    if (prefix == null || prefix.length() == 0) {
      prefixRef = null;
    } else {
      prefixRef = new BytesRefBuilder();
      prefixRef.copyChars(prefix);
    }

    findStartAndEndOrds();

    if (refineResult != null) {
      if (freq.allBuckets) {
        // count is irrelevant, but hardcoded in collect(...), so intercept/mask normal counts.
        // Set here to prevent createAccs(...) from creating a 1-slot countAcc that will fail with AIOOBE
        // NOTE: because collectAcc will be null, it is fine/irrelevant to set a countAcc that doesn't support sweeping
        countAcc = SlotAcc.DEV_NULL_SLOT_ACC;
        createAccs(nDocs, 1);
        assert collectAcc == null;
        otherAccs = accs; // accs is created above and set on allBucketsAcc; but during collection, setNextReader is called on otherAccs.
        allBucketsAcc = new SpecialSlotAcc(fcontext, null, -1, accs, 0);
        collectDocs();

        SimpleOrderedMap<Object> allBuckets = new SimpleOrderedMap<>();
        allBuckets.add("count", allBucketsAcc.getSpecialCount());
        allBucketsAcc.setValues(allBuckets, -1); // -1 slotNum is unused for SpecialSlotAcc
        refineResult.add("allBuckets", allBuckets);
        return refineResult;
      }
    }

    maxSlots = nTerms;

    if (freq.allBuckets) {
      allBucketsSlot = maxSlots++;
    }

    createCollectAcc(nDocs, maxSlots);

    if (freq.allBuckets) {
      allBucketsAcc = new SpecialSlotAcc(fcontext, collectAcc, allBucketsSlot, otherAccs, 0);
    }

    collectDocs();

    return super.findTopSlots(nTerms, nTerms,
        slotNum -> { // getBucketValFromSlotNum
          try {
            return (Comparable) sf.getType().toObject(sf, lookupOrd(slotNum + startTermIndex));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        obj -> valueObjToString(obj)
    );
  }

  private static String valueObjToString(Object obj) {
    return (obj instanceof Date) ? ((Date)obj).toInstant().toString() : obj.toString();
  }

                                                           
  /**
   * SlotContext to use during all {@link SlotAcc} collection.
   *
   * @see #lookupOrd
   */
  public IntFunction<SlotContext> slotContext = (slotNum) -> {
    try {
      Object value = sf.getType().toObject(sf, lookupOrd(slotNum + startTermIndex));
      Query q = makeBucketQuery(valueObjToString(value));
      assert null != q : "null query for: '" + value + "'";
      return new SlotContext(q);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  };
}
