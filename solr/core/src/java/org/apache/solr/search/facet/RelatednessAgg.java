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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Map;
import java.util.function.IntFunction;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.search.Query;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An aggregation function designed to be nested under other (possibly deeply nested) facets for 
 * the purposes of computing the "relatedness" of facet buckets relative to 
 * "foreground" and "background" sets -- primarily for the purpose of building "Semantic Knowledge Graphs"
 *
 * @see <a href="https://arxiv.org/pdf/1609.00464.pdf">The Semantic Knowledge Graph: 
 *     A compact, auto-generated model for real-time traversal and ranking of any relationship 
 *     within a domain</a>
 */
public class RelatednessAgg extends AggValueSource {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // end user values
  private static final String RELATEDNESS = "relatedness";
  private static final String FG_POP = "foreground_popularity";
  private static final String BG_POP = "background_popularity";

  // needed for distrib calculation
  private static final String FG_SIZE = "foreground_size";
  private static final String FG_COUNT = "foreground_count";
  private static final String BG_SIZE = "background_size";
  private static final String BG_COUNT = "background_count";
  
  final protected Query fgQ;
  final protected Query bgQ;
  protected double min_pop = 0.0D;
  
  public static final String NAME = RELATEDNESS;
  public RelatednessAgg(Query fgQ, Query bgQ) {
    super(NAME); 
    // NOTE: ideally we don't want to assume any defaults *yet* if fgQ/bgQ are null
    // keep them null until it's time to created a SlotAcc, at which point we might inherit values
    // from an ancestor facet context w/same key -- see comments in createSlotAcc
    this.fgQ = fgQ;
    this.bgQ = bgQ;

    // TODO: defaults not supported yet -- see comments in createSlotAcc
    if (null == fgQ || null == bgQ) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                              NAME + " aggregate function requires both foreground & background " +
                              "to be real (non-null) queries");
    }
  }

  public void setOpts(QParser parser) {
    final boolean isShard = parser.getReq().getParams().getBool(ShardParams.IS_SHARD, false);
    SolrParams opts = parser.getLocalParams();
    if (null != opts) {
      if (!isShard) { // ignore min_pop if this is a shard request
        this.min_pop = opts.getDouble("min_popularity", 0.0D);
      }
    }
  }
  
  @Override
  public String description() {
    // TODO: need better output processing when we start supporting null fgQ/bgQ in constructor
    return name +"(fgQ=" + fgQ + ",bgQ=" + bgQ + ",min_pop="+min_pop+")";
  }
  
  @Override
  public boolean equals(Object o) {
    if (! Objects.equals(this.getClass(), o.getClass())) {
      return false;
    }
    RelatednessAgg that = (RelatednessAgg) o;
    return Objects.equals(fgQ, that.fgQ)
      && Objects.equals(bgQ, that.bgQ)
      && Objects.equals(min_pop, that.min_pop);
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(getClass(), fgQ, bgQ, min_pop);
  }

  @Override
  public FunctionValues getValues(@SuppressWarnings("rawtypes") Map context, LeafReaderContext readerContext) throws IOException {
    throw new UnsupportedOperationException("NOT IMPLEMENTED " + name + " " + this);
  }


  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    // TODO: Ideally this is where we should check fgQ/bgQ for 'null' and apply defaults...
    //
    // we want to walk up the fcontext and inherit the queries from any ancestor SKGAgg
    // with the same "key" that we have in our own context -- and as a last resort use
    // "$q" for the foreground and "*:*" for the bgQ (if no ancestors)
    // (Hmmm... or maybe we should use the "Domain" of our FacetRequest as the default bg?)
    //
    // How do we find our what key we have in the current context?
    // loop over all the stats in the current context until we find one that's '==' to this???
    
    List<Query> fgFilters = new ArrayList<Query>(3);
    fgFilters.add(fgQ);
    for (FacetContext ctx = fcontext; ctx != null; ctx = ctx.parent) {
      if (null != ctx.filter) {
        fgFilters.add(ctx.filter);
      } else {
        // sanity check...
        // the only way the filter on the current context should be null is...
        assert (// 1) it's the actual top most context,
                //    (ie: the func is directly used w/o being nested under a facet)
                (null == ctx.parent && fcontext == ctx) ||
                // 2) it's a child of the top most context
                //    (ie: the context of a top level facet)
                (null == ctx.parent.parent && null == ctx.parent.filter));
        // either way, no reason to keep looping up the (0 or 1) remaining ancestors
        // (which is why #1 can assert '&& fcontext == ctx')
        break;
      }
    }
    
    DocSet fgSet = fcontext.searcher.getDocSet(fgFilters);
    DocSet bgSet = fcontext.searcher.getDocSet(bgQ);
    return new SKGSlotAcc(this, fcontext, numSlots, fgSet, bgSet);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new Merger(this);
  }
  
  private static final String IMPLIED_KEY = "implied";

  private static final class SKGSlotAcc extends SlotAcc {
    private final RelatednessAgg agg;
    private BucketData[] slotvalues;
    private final DocSet fgSet;
    private final DocSet bgSet;
    private final long fgSize;
    private final long bgSize;
    public SKGSlotAcc(final RelatednessAgg agg, final FacetContext fcontext, final int numSlots,
                      final DocSet fgSet, final DocSet bgSet) throws IOException {
      super(fcontext);
      this.agg = agg;
      this.fgSet = fgSet;
      this.bgSet = bgSet;
      // cache the set sizes for frequent re-use on every slot
      this.fgSize = fgSet.size();
      this.bgSize = bgSet.size();
      this.slotvalues = new BucketData[numSlots];
      reset();
    }
    private void processSlot(int slot, IntFunction<SlotContext> slotContext) throws IOException {
      
      assert null != slotContext;
      
      final BucketData slotVal = new BucketData(agg);
      slotvalues[slot] = slotVal;
      
      final SlotContext ctx = slotContext.apply(slot);
      if (ctx.isAllBuckets()) {
        // relatedness is meaningless for allBuckets (see SOLR-14467)
        // our current (implied & empty) BucketData is all we need
        //
        // NOTE: it might be temping to use 'slotvalues[slot] = null' in this case
        // since getValue() will also ultimately generate an implied bucket in that case,
        // but by using a non-null bucket we let collect(int,...) know it doesn't need to keep calling
        // processSlot over and over.
        return;
      }
      
      Query slotQ = ctx.getSlotQuery();
      if (null == slotQ) {
        // extremeley special edge case...
        // the only way this should be possible is if our relatedness() function is used as a "top level"
        // stat w/o being nested under any facet, in which case it should be a FacetQuery w/no parent...
        assert fcontext.processor.freq instanceof FacetQuery : fcontext.processor.freq;
        assert null == fcontext.parent;
        assert null == fcontext.filter;
      }
      // ...and in which case we should just use the current base
      final DocSet slotSet = null == slotQ ? fcontext.base : fcontext.searcher.getDocSet(slotQ);

      slotVal.incSizes(fgSize, bgSize);
      slotVal.incCounts(fgSet.intersectionSize(slotSet),
                        bgSet.intersectionSize(slotSet));
    }
    
    @Override
    public void collect(int perSegDocId, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      // NOTE: we don't actaully care about the individual docs being collected
      // (the only reason we even bother implementing this method is because it's needed for sorting
      // buckets by a function)
      
      // so we only worry about ensuring that every "slot" / bucket is processed the first time
      // we're asked about it...
      if (null == slotvalues[slot]) {
        processSlot(slot, slotContext);
      }
    }

    @Override
    public int collect(DocSet docs, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      // NOTE: we don't actaully care about the doc set being collected for the bucket
      // so we only worry about ensuring that every "slot" / bucket is processed exactly once
      
      // if we're doing bulk collection, we better not be getting asked to re-use slots
      assert null == slotvalues[slot];
      processSlot(slot, slotContext);

      // we don't do any filtering, we collect the whole docset, so return that as out collected count
      // (as a stat, we're actually required to return this by assertions in FacetFieldProcessor.processStats)
      return docs.size();
    }

    public int compare(int slotA, int slotB) {
      final BucketData a = slotvalues[slotA];
      final BucketData b = slotvalues[slotB];
      
      // we initialize & reset() (unused) slotvalues elements to null
      // but we should never be asked to compare a slot that hasn't been collected...
      assert null != a;
      assert null != b;
      return a.compareTo(b);
    }

    @Override
    public Object getValue(int slotNum) {
      BucketData slotVal = slotvalues[slotNum];
      if (null == slotVal) {
        // since we haven't collected any docs for this slot, use am (implied) slot w/no counts,
        // just the known fg/bg sizes. (this is most likely a refinement request for a bucket we dont have)
        slotVal = new BucketData(agg);
        slotVal.incSizes(fgSize, bgSize);
      }

      @SuppressWarnings({"rawtypes"})
      SimpleOrderedMap res = slotVal.externalize(fcontext.isShard());
      return res;
    }

    @Override
    public void reset() {
      Arrays.fill(slotvalues, null);
    }

    @Override
    public void resize(Resizer resizer) {
      slotvalues = resizer.resize(slotvalues, null);
    }

    @Override
    public void close() throws IOException {
      slotvalues = null;
    }
  }
  
  /**
   * Encapsulates all data needed for a single bucket/slot
   * 
   * @see SKGSlotAcc
   * @see Merger
   */
  private static class BucketData implements Comparable<BucketData> {
    private RelatednessAgg agg;
    private long fg_size = 0;
    private long bg_size = 0;
    private long fg_count = 0;
    private long bg_count = 0;

    /**
     * Buckets are implied until/unless counts are explicitly incremented (even if those counts are 0)
     * An implied bucket means we have no real data for it -- it may be useful for a per-Shard request
     * to return "size" info of a bucket that doesn't exist on the current shard, or it may represent
     * the <code>allBuckets</code> bucket.
     *
     * @see #incCounts
     */
    private boolean implied;
    
    /** 
     * NaN indicates that <b>all</a> derived values need (re)-computed
     * @see #computeDerivedValues
     * @see #getRelatedness
     */
    private double relatedness = Double.NaN;
    /** 
     * @see #computeDerivedValues 
     * @see #getForegroundPopularity
     */
    private double fg_pop;
    /** 
     * @see #computeDerivedValues
     * @see #getBackgroundPopularity
     */
    private double bg_pop;
    
    public BucketData(final RelatednessAgg agg) {
      this.agg = agg;
      this.implied = true;
    }

    /** 
     * Increment both the foreground &amp; background <em>counts</em> for the current bucket, reseting any
     * derived values that may be cached
     */
    public void incCounts(final long fgInc, final long bgInc) {
      this.implied = false;
      this.relatedness = Double.NaN;
      fg_count += fgInc;
      bg_count += bgInc;
    }
    /** 
     * Increment both the foreground &amp; background <em>sizes</em> for the current bucket, reseting any
     * derived values that may be cached
     */
    public void incSizes(final long fgInc, final long bgInc) {
        this.relatedness = Double.NaN;
        fg_size += fgInc;
        bg_size += bgInc;
    }
    
    @Override
    public int hashCode() {
      return Objects.hash(this.getClass(), implied, fg_count, bg_count, fg_size, bg_size, agg);
    }
    
    @Override
    public boolean equals(Object other) {
      if (!Objects.equals(this.getClass(), other.getClass())) {
        return false;
      }
      BucketData that = (BucketData)other;
      // we will most certainly be compared to other buckets of the same Agg instance, so compare counts first
      return Objects.equals(this.implied, that.implied)
        && Objects.equals(this.fg_count, that.fg_count)
        && Objects.equals(this.bg_count, that.bg_count)
        && Objects.equals(this.fg_size, that.fg_size)
        && Objects.equals(this.bg_size, that.bg_size)
        && Objects.equals(this.agg, that.agg);
    }

    /**
     * Computes (and caches) the derived relatedness &amp; popularity scores for this bucket if needed
     */
    private void computeDerivedValues() {
      if (! Double.isNaN(this.relatedness)) {
        return; // values already computed;
      }

      this.fg_pop = roundTo5Digits((double) fg_count / bg_size); // yes, BACKGROUND size is intentional
      this.bg_pop = roundTo5Digits((double) bg_count / bg_size);
      
      if (0.0D < agg.min_pop) {
        // if min_pop is configured, and either (fg|bg) popularity is lower then that value
        // then "this.relatedness=-Infinity" so it sorts below any "valid" relatedness scores
        if (fg_pop < agg.min_pop || bg_pop < agg.min_pop) {
          this.relatedness = Double.NEGATIVE_INFINITY;
          return;
        }
      }
      
      this.relatedness = computeRelatedness(this.fg_count, this.fg_size,
                                            this.bg_count, this.bg_size);
    }
    private double getRelatedness() {
      computeDerivedValues();
      return this.relatedness;
    }
    private double getForegroundPopularity() {
      computeDerivedValues();
      return this.fg_pop;
    }
    private double getBackgroundPopularity() {
      computeDerivedValues();
      return this.bg_pop;
    }
    
    @Override
    public int compareTo(BucketData that) {
      // TODO: add support for a "sort_val" option...
      //
      // default should be "relatedness" but also support "foreground" and "background" ...
      // either of those should sort by the corrisponding ratio
      // To do this, we should probably precommpute the ratios in incCounts
      
      int r = Double.compare(this.getRelatedness(), that.getRelatedness());
      if (0 == r) {
        r = Long.compare(this.fg_count, that.fg_count);
      }
      if (0 == r) {
        r = Long.compare(this.bg_count, that.bg_count);
      }
      return r;
    }
    
    /**
     * @see SlotAcc#getValue
     * @see Merger#getMergedResult
     */

    @SuppressWarnings({"unchecked", "rawtypes"})
    public SimpleOrderedMap externalize(final boolean isShardRequest) {
      SimpleOrderedMap result = new SimpleOrderedMap<Number>();

      // if counts are non-zero, then this bucket must not be implied
      assert 0 == fg_count || ! implied : "Implied bucket has non-zero fg_count";
      assert 0 == bg_count || ! implied : "Implied bucket has non-zero bg_count";
      
      if (isShardRequest) {
        // shard responses must include size info, but don't need the derived stats
        //
        // NOTE: sizes will be the same for every slot...
        // TODO: it would be nice to put them directly in the parent facet, instead of every bucket,
        // in order to reduce the size of the response.
        result.add(FG_SIZE, fg_size); 
        result.add(BG_SIZE, bg_size);
        
        if (implied) {
          // for an implied bucket on this shard, we don't need to bother returning the (empty)
          // counts, just the flag explaining that this bucket is (locally) implied...
          result.add(IMPLIED_KEY, Boolean.TRUE);
        } else {
          result.add(FG_COUNT, fg_count); 
          result.add(BG_COUNT, bg_count);
        }
      } else {
        if (implied) {
          // When returning results to an external client, any bucket still 'implied' shouldn't return
          // any results at all.
          // (practically speaking this should only happen for the 'allBuckets' bucket
          return null;
        }

        // there's no need to bother computing these when returning results *to* a shard coordinator
        // only useful to external clients 
        result.add(RELATEDNESS, this.getRelatedness());
        result.add(FG_POP, this.getForegroundPopularity());
        result.add(BG_POP, this.getBackgroundPopularity());
      }
      
      return result;
    }
  }

  /**
   * Merges in the per shard {@link BucketData} output into a unified {@link BucketData}
   */
  private static final class Merger extends FacetModule.FacetSortableMerger {
    private final BucketData mergedData;
    public Merger(final RelatednessAgg agg) {
      this.mergedData = new BucketData(agg);
    }
    
    @Override
    public void merge(Object facetResult, Context mcontext) {
      @SuppressWarnings({"unchecked"})
      final NamedList<Object> shardData = (NamedList<Object>)facetResult;
      
      final boolean shardImplied = Optional.ofNullable((Boolean)shardData.remove(IMPLIED_KEY)).orElse(false);
      
      // regardless of wether this shard is implied, we want to know it's size info...
      mergedData.incSizes((Long)shardData.remove(FG_SIZE), (Long)shardData.remove(BG_SIZE));

      if (! shardImplied) {
        // only merge in counts from non-implied shard buckets...
        mergedData.incCounts((Long)shardData.remove(FG_COUNT), (Long)shardData.remove(BG_COUNT));
      } else {
        // if this shard is implied, we shouldn't have even gotten counts...
        assert shardImplied;
        assert null == shardData.remove(FG_COUNT);
        assert null == shardData.remove(BG_COUNT);
      }
    }

    @Override
    public int compareTo(FacetModule.FacetSortableMerger other, FacetRequest.SortDirection direction) {
      // NOTE: regardless of the SortDirection hint, we want normal comparison of the BucketData
      
      assert other instanceof Merger;
      Merger that = (Merger)other;
      return mergedData.compareTo(that.mergedData);
    }
    
    @Override
    public Object getMergedResult() {
      return mergedData.externalize(false);
    }
  }

  
  /**
   * This is an aproximated Z-Score, as described in the "Scoring Semantic Relationships" 
   * section of "<a href="https://arxiv.org/pdf/1609.00464.pdf">The Semantic Knowledge Graph: 
   * A compact, auto-generated model for real-time traversal and ranking of any relationship 
   * within a domain</a>"
   *
   * See Also:<ul>
   * <li><a href="https://s.apache.org/Mfu2">java-user@lucene Message-ID: 449AEB60.4070300@alias-i.com</a></li>
   * <li><a href="https://lingpipe-blog.com/2006/03/29/interesting-phrase-extraction-binomial-hypothesis-testing-vs-coding-loss/">Phrase Extraction: Binomial Hypothesis Testing vs. Coding Loss</a></li>
   * </ul>
   */
  // NOTE: javadoc linter freaks out if we try doing those links as '@see <a href=...' tags
  public static double computeRelatedness(final long fg_count, final long fg_size,
                                          final long bg_count, final long bg_size) {
    final double fg_size_d = (double) fg_size;
    final double bg_size_d = (double) bg_size;
    final double bg_prob = (bg_count / bg_size_d);
    final double num = fg_count - fg_size_d * bg_prob;
    double denom = Math.sqrt(fg_size_d * bg_prob * (1 - bg_prob));
    denom = (denom == 0) ? 1e-10 : denom;
    final double z = num / denom;
    final double result = 0.2 * sigmoidHelper(z, -80, 50)
      + 0.2 * sigmoidHelper(z, -30, 30)
      + 0.2 * sigmoidHelper(z, 0, 30)
      + 0.2 * sigmoidHelper(z, 30, 30)
      + 0.2 * sigmoidHelper(z, 80, 50);
    return roundTo5Digits(result);
    
  }
  /**
   * Helper function for rounding/truncating relatedness &amp; popularity values to 
   * 5 decimal digits, since these values are all probabilistic more then 5 digits aren't really relevant
   * and may give a missleading impression of added precision.
   */
  public static double roundTo5Digits(final double val) {
    return Math.round(val * 1e5) / 1e5;
  }
  
  /** A helper function for scaling values */
  private static double sigmoidHelper(final double x, final double offset, final double scale) {
    return (x+offset) / (scale + Math.abs(x+offset));
  }
}

