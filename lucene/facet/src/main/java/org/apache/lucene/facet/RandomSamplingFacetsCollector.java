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
package org.apache.lucene.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.facet.FacetsConfig.DimConfig;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;

/**
 * Collects hits for subsequent faceting, using sampling if needed. Once you've
 * run a search and collect hits into this, instantiate one of the
 * {@link Facets} subclasses to do the facet counting. Note that this collector
 * does not collect the scores of matching docs (i.e.
 * {@link FacetsCollector.MatchingDocs#scores}) is {@code null}.
 * <p>
 * If you require the original set of hits, you can call
 * {@link #getOriginalMatchingDocs()}. Also, since the counts of the top-facets
 * is based on the sampled set, you can amortize the counts by calling
 * {@link #amortizeFacetCounts}.
 */
public class RandomSamplingFacetsCollector extends FacetsCollector {
  
  /**
   * Faster alternative for java.util.Random, inspired by
   * http://dmurphy747.wordpress.com/2011/03/23/xorshift-vs-random-
   * performance-in-java/
   * <p>
   * Has a period of 2^64-1
   */
  private static class XORShift64Random {
    
    private long x;
    
    /** Creates a xorshift random generator using the provided seed */
    public XORShift64Random(long seed) {
      x = seed == 0 ? 0xdeadbeef : seed;
    }
    
    /** Get the next random long value */
    public long randomLong() {
      x ^= (x << 21);
      x ^= (x >>> 35);
      x ^= (x << 4);
      return x;
    }
    
    /** Get the next random int, between 0 (inclusive) and n (exclusive) */
    public int nextInt(int n) {
      int res = (int) (randomLong() % n);
      return (res < 0) ? -res : res;
    }
    
  }
  
  private final static int NOT_CALCULATED = -1;
  
  private final int sampleSize;
  private final XORShift64Random random;
  
  private double samplingRate;
  private List<MatchingDocs> sampledDocs;
  private int totalHits = NOT_CALCULATED;
  private int leftoverBin = NOT_CALCULATED;
  private int leftoverIndex = NOT_CALCULATED;
  
  /**
   * Constructor with the given sample size and default seed.
   * 
   * @see #RandomSamplingFacetsCollector(int, long)
   */
  public RandomSamplingFacetsCollector(int sampleSize) {
    this(sampleSize, 0);
  }
  
  /**
   * Constructor with the given sample size and seed.
   * 
   * @param sampleSize
   *          The preferred sample size. If the number of hits is greater than
   *          the size, sampling will be done using a sample ratio of sampling
   *          size / totalN. For example: 1000 hits, sample size = 10 results in
   *          samplingRatio of 0.01. If the number of hits is lower, no sampling
   *          is done at all
   * @param seed
   *          The random seed. If {@code 0} then a seed will be chosen for you.
   */
  public RandomSamplingFacetsCollector(int sampleSize, long seed) {
    super(false);
    this.sampleSize = sampleSize;
    this.random = new XORShift64Random(seed);
    this.sampledDocs = null;
  }
  
  /**
   * Returns the sampled list of the matching documents. Note that a
   * {@link FacetsCollector.MatchingDocs} instance is returned per segment, even
   * if no hits from that segment are included in the sampled set.
   * <p>
   * Note: One or more of the MatchingDocs might be empty (not containing any
   * hits) as result of sampling.
   * <p>
   * Note: {@code MatchingDocs.totalHits} is copied from the original
   * MatchingDocs, scores is set to {@code null}
   */
  @Override
  public List<MatchingDocs> getMatchingDocs() {
    List<MatchingDocs> matchingDocs = super.getMatchingDocs();
    
    if (totalHits == NOT_CALCULATED) {
      totalHits = 0;
      for (MatchingDocs md : matchingDocs) {
        totalHits += md.totalHits;
      }
    }
    
    if (totalHits <= sampleSize) {
      return matchingDocs;
    }
    
    if (sampledDocs == null) {
      samplingRate = (1.0 * sampleSize) / totalHits;
      sampledDocs = createSampledDocs(matchingDocs);
    }
    return sampledDocs;
  }
  
  /** Returns the original matching documents. */
  public List<MatchingDocs> getOriginalMatchingDocs() {
    return super.getMatchingDocs();
  }
  
  /** Create a sampled copy of the matching documents list. */
  private List<MatchingDocs> createSampledDocs(List<MatchingDocs> matchingDocsList) {
    List<MatchingDocs> sampledDocsList = new ArrayList<>(matchingDocsList.size());
    for (MatchingDocs docs : matchingDocsList) {
      sampledDocsList.add(createSample(docs));
    }
    return sampledDocsList;
  }
  
  /** Create a sampled of the given hits. */
  private MatchingDocs createSample(MatchingDocs docs) {
    int maxdoc = docs.context.reader().maxDoc();
    
    // TODO: we could try the WAH8DocIdSet here as well, as the results will be sparse
    FixedBitSet sampleDocs = new FixedBitSet(maxdoc);
    
    int binSize = (int) (1.0 / samplingRate);
    
    try {
      int counter = 0;
      int limit, randomIndex;
      if (leftoverBin != NOT_CALCULATED) {
        limit = leftoverBin;
        // either NOT_CALCULATED, which means we already sampled from that bin,
        // or the next document to sample
        randomIndex = leftoverIndex;
      } else {
        limit = binSize;
        randomIndex = random.nextInt(binSize);
      }
      final DocIdSetIterator it = docs.bits.iterator();
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        if (counter == randomIndex) {
          sampleDocs.set(doc);
        }
        counter++;
        if (counter >= limit) {
          counter = 0;
          limit = binSize;
          randomIndex = random.nextInt(binSize);
        }
      }
      
      if (counter == 0) {
        // we either exhausted the bin and the iterator at the same time, or
        // this segment had no results. in the latter case we might want to
        // carry leftover to the next segment as is, but that complicates the
        // code and doesn't seem so important.
        leftoverBin = leftoverIndex = NOT_CALCULATED;
      } else {
        leftoverBin = limit - counter;
        if (randomIndex > counter) {
          // the document to sample is in the next bin
          leftoverIndex = randomIndex - counter;
        } else if (randomIndex < counter) {
          // we sampled a document from the bin, so just skip over remaining
          // documents in the bin in the next segment.
          leftoverIndex = NOT_CALCULATED;
        }
      }
      
      return new MatchingDocs(docs.context, new BitDocIdSet(sampleDocs), docs.totalHits, null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Note: if you use a counting {@link Facets} implementation, you can amortize the
   * sampled counts by calling this method. Uses the {@link FacetsConfig} and
   * the {@link IndexSearcher} to determine the upper bound for each facet value.
   */
  public FacetResult amortizeFacetCounts(FacetResult res, FacetsConfig config, IndexSearcher searcher) throws IOException {
    if (res == null || totalHits <= sampleSize) {
      return res;
    }
    
    LabelAndValue[] fixedLabelValues = new LabelAndValue[res.labelValues.length];
    IndexReader reader = searcher.getIndexReader();
    DimConfig dimConfig = config.getDimConfig(res.dim);
    
    // +2 to prepend dimension, append child label
    String[] childPath = new String[res.path.length + 2];
    childPath[0] = res.dim;
    
    System.arraycopy(res.path, 0, childPath, 1, res.path.length); // reuse
    
    for (int i = 0; i < res.labelValues.length; i++) {
      childPath[res.path.length + 1] = res.labelValues[i].label;
      String fullPath = FacetsConfig.pathToString(childPath, childPath.length);
      int max = reader.docFreq(new Term(dimConfig.indexFieldName, fullPath));
      int correctedCount = (int) (res.labelValues[i].value.doubleValue() / samplingRate);
      correctedCount = Math.min(max, correctedCount);
      fixedLabelValues[i] = new LabelAndValue(res.labelValues[i].label, correctedCount);
    }
    
    // cap the total count on the total number of non-deleted documents in the reader
    int correctedTotalCount = res.value.intValue();
    if (correctedTotalCount > 0) {
      correctedTotalCount = Math.min(reader.numDocs(), (int) (res.value.doubleValue() / samplingRate));
    }
    
    return new FacetResult(res.dim, res.path, correctedTotalCount, fixedLabelValues, res.childCount);
  }
  
  /** Returns the sampling rate that was used. */
  public double getSamplingRate() {
    return samplingRate;
  }
  
}
