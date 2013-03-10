package org.apache.lucene.facet.sampling;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.util.PriorityQueue;

import org.apache.lucene.facet.search.ScoredDocIDs;
import org.apache.lucene.facet.search.ScoredDocIDsIterator;
import org.apache.lucene.facet.util.ScoredDocIdsUtils;

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

/**
 * Take random samples of large collections.
 * @lucene.experimental
 */
public class RepeatableSampler extends Sampler {

  private static final Logger logger = Logger.getLogger(RepeatableSampler.class.getName());

  public RepeatableSampler(SamplingParams params) {
    super(params);
  }
  
  @Override
  protected SampleResult createSample(ScoredDocIDs docids, int actualSize,
      int sampleSetSize) throws IOException {
    int[] sampleSet = null;
    try {
      sampleSet = repeatableSample(docids, actualSize,
          sampleSetSize);
    } catch (IOException e) {
      if (logger.isLoggable(Level.WARNING)) {
        logger.log(Level.WARNING, "sampling failed: "+e.getMessage()+" - falling back to no sampling!", e);
      }
      return new SampleResult(docids, 1d);
    }

    ScoredDocIDs sampled = ScoredDocIdsUtils.createScoredDocIDsSubset(docids,
        sampleSet);
    if (logger.isLoggable(Level.FINEST)) {
      logger.finest("******************** " + sampled.size());
    }
    return new SampleResult(sampled, sampled.size()/(double)docids.size());
  }
  
  /**
   * Returns <code>sampleSize</code> values from the first <code>collectionSize</code>
   * locations of <code>collection</code>, chosen using
   * the <code>TRAVERSAL</code> algorithm. The sample values are not sorted.
   * @param collection The values from which a sample is wanted.
   * @param collectionSize The number of values (from the first) from which to draw the sample.
   * @param sampleSize The number of values to return.
   * @return An array of values chosen from the collection.
   * @see Algorithm#TRAVERSAL
   */
  private static int[] repeatableSample(ScoredDocIDs collection,
      int collectionSize, int sampleSize)
  throws IOException {
    return repeatableSample(collection, collectionSize,
        sampleSize, Algorithm.HASHING, Sorted.NO);
  }

  /**
   * Returns <code>sampleSize</code> values from the first <code>collectionSize</code>
   * locations of <code>collection</code>, chosen using <code>algorithm</code>.
   * @param collection The values from which a sample is wanted.
   * @param collectionSize The number of values (from the first) from which to draw the sample.
   * @param sampleSize The number of values to return.
   * @param algorithm Which algorithm to use.
   * @param sorted Sorted.YES to sort the sample values in ascending order before returning;
   * Sorted.NO to return them in essentially random order.
   * @return An array of values chosen from the collection.
   */
  private static int[] repeatableSample(ScoredDocIDs collection,
      int collectionSize, int sampleSize,
      Algorithm algorithm, Sorted sorted)
  throws IOException {
    if (collection == null) {
      throw new IOException("docIdSet is null");
    }
    if (sampleSize < 1) {
      throw new IOException("sampleSize < 1 (" + sampleSize + ")");
    }
    if (collectionSize < sampleSize) {
      throw new IOException("collectionSize (" + collectionSize + ") less than sampleSize (" + sampleSize + ")");
    }
    int[] sample = new int[sampleSize];
    long[] times = new long[4];
    if (algorithm == Algorithm.TRAVERSAL) {
      sample1(collection, collectionSize, sample, times);
    } else if (algorithm == Algorithm.HASHING) {
      sample2(collection, collectionSize, sample, times);
    } else {
      throw new IllegalArgumentException("Invalid algorithm selection");
    }
    if (sorted == Sorted.YES) {
      Arrays.sort(sample);
    }
    if (returnTimings) {
      times[3] = System.currentTimeMillis();
      if (logger.isLoggable(Level.FINEST)) {
        logger.finest("Times: " + (times[1] - times[0]) + "ms, "
            + (times[2] - times[1]) + "ms, " + (times[3] - times[2])+"ms");
      }
    }
    return sample;
  }

  /**
   * Returns <code>sample</code>.length values chosen from the first <code>collectionSize</code>
   * locations of <code>collection</code>, using the TRAVERSAL algorithm. The sample is
   * pseudorandom: no subset of the original collection
   * is in principle more likely to occur than any other, but for a given collection
   * and sample size, the same sample will always be returned. This algorithm walks the
   * original collection in a methodical way that is guaranteed not to visit any location
   * more than once, which makes sampling without replacement faster because removals don't
   * have to be tracked, and the number of operations is proportional to the sample size,
   * not the collection size.
   * Times for performance measurement
   * are returned in <code>times</code>, which must be an array of at least three longs, containing
   * nanosecond event times. The first
   * is set when the algorithm starts; the second, when the step size has been calculated;
   * and the third when the sample has been taken.
   * @param collection The set to be sampled.
   * @param collectionSize The number of values to use (starting from first).
   * @param sample The array in which to return the sample.
   * @param times The times of three events, for measuring performance.
   */
  private static void sample1(ScoredDocIDs collection, int collectionSize, int[] sample, long[] times) 
  throws IOException {
    ScoredDocIDsIterator it = collection.iterator();
    if (returnTimings) {
      times[0] = System.currentTimeMillis();
    }
    int sampleSize = sample.length;
    int prime = findGoodStepSize(collectionSize, sampleSize);
    int mod = prime % collectionSize;
    if (returnTimings) {
      times[1] = System.currentTimeMillis();
    }
    int sampleCount = 0;
    int index = 0;
    for (; sampleCount < sampleSize;) {
      if (index + mod < collectionSize) {
        for (int i = 0; i < mod; i++, index++) {
          it.next();
        }
      } else {
        index = index + mod - collectionSize;
        it = collection.iterator();
        for (int i = 0; i < index; i++) {
          it.next();
        }
      }
      sample[sampleCount++] = it.getDocID();
    }
    if (returnTimings) {
      times[2] = System.currentTimeMillis();
    }
  }

  /**
   * Returns a value which will allow the caller to walk
   * a collection of <code>collectionSize</code> values, without repeating or missing
   * any, and spanning the collection from beginning to end at least once with
   * <code>sampleSize</code> visited locations. Choosing a value
   * that is relatively prime to the collection size ensures that stepping by that size (modulo
   * the collection size) will hit all locations without repeating, eliminating the need to
   * track previously visited locations for a "without replacement" sample. Starting with the
   * square root of the collection size ensures that either the first or second prime tried will
   * work (they can't both divide the collection size). It also has the property that N steps of
   * size N will span a collection of N**2 elements once. If the sample is bigger than N, it will
   * wrap multiple times (without repeating). If the sample is smaller, a step size is chosen
   * that will result in at least one spanning of the collection.
   * 
   * @param collectionSize The number of values in the collection to be sampled.
   * @param sampleSize The number of values wanted in the sample.
   * @return A good increment value for walking the collection.
   */
  private static int findGoodStepSize(int collectionSize, int sampleSize) {
    int i = (int) Math.sqrt(collectionSize);
    if (sampleSize < i) {
      i = collectionSize / sampleSize;
    }
    do {
      i = findNextPrimeAfter(i);
    } while (collectionSize % i == 0);
    return i;
  }

  /**
   * Returns the first prime number that is larger than <code>n</code>.
   * @param n A number less than the prime to be returned.
   * @return The smallest prime larger than <code>n</code>.
   */
  private static int findNextPrimeAfter(int n) {
    n += (n % 2 == 0) ? 1 : 2; // next odd
    foundFactor: for (;; n += 2) { //TODO labels??!!
      int sri = (int) (Math.sqrt(n));
      for (int primeIndex = 0; primeIndex < N_PRIMES; primeIndex++) {
        int p = primes[primeIndex];
        if (p > sri) {
          return n;
        }
        if (n % p == 0) {
          continue foundFactor;
        }
      }
      for (int p = primes[N_PRIMES - 1] + 2;; p += 2) {
        if (p > sri) {
          return n;
        }
        if (n % p == 0) {
          continue foundFactor;
        }
      }
    }
  }

  /**
   * The first N_PRIMES primes, after 2.
   */
  private static final int N_PRIMES = 4000;
  private static int[] primes = new int[N_PRIMES];
  static {
    primes[0] = 3;
    for (int count = 1; count < N_PRIMES; count++) {
      primes[count] = findNextPrimeAfter(primes[count - 1]);
    }
  }

  /**
   * Returns <code>sample</code>.length values chosen from the first <code>collectionSize</code>
   * locations of <code>collection</code>, using the HASHING algorithm. Performance measurements
   * are returned in <code>times</code>, which must be an array of at least three longs. The first
   * will be set when the algorithm starts; the second, when a hash key has been calculated and
   * inserted into the priority queue for every element in the collection; and the third when the
   * original elements associated with the keys remaining in the PQ have been stored in the sample
   * array for return.
   * <P>
   * This algorithm slows as the sample size becomes a significant fraction of the collection
   * size, because the PQ is as large as the sample set, and will not do early rejection of values
   * below the minimum until it fills up, and a larger PQ contains more small values to be purged,
   * resulting in less early rejection and more logN insertions.
   * 
   * @param collection The set to be sampled.
   * @param collectionSize The number of values to use (starting from first).
   * @param sample The array in which to return the sample.
   * @param times The times of three events, for measuring performance.
   */
  private static void sample2(ScoredDocIDs collection, int collectionSize, int[] sample, long[] times) 
  throws IOException {
    if (returnTimings) {
      times[0] = System.currentTimeMillis();
    }
    int sampleSize = sample.length;
    IntPriorityQueue pq = new IntPriorityQueue(sampleSize);
    /*
     * Convert every value in the collection to a hashed "weight" value, and insert
     * into a bounded PQ (retains only sampleSize highest weights).
     */
    ScoredDocIDsIterator it = collection.iterator();
    MI mi = null;
    while (it.next()) {
      if (mi == null) {
        mi = new MI();
      }
      mi.value = (int) (it.getDocID() * PHI_32) & 0x7FFFFFFF;
      mi = pq.insertWithOverflow(mi);
    }
    if (returnTimings) {
      times[1] = System.currentTimeMillis();
    }
    /*
     * Extract heap, convert weights back to original values, and return as integers.
     */
    Object[] heap = pq.getHeap();
    for (int si = 0; si < sampleSize; si++) {
      sample[si] = (int)(((MI) heap[si+1]).value * PHI_32I) & 0x7FFFFFFF;
    }
    if (returnTimings) {
      times[2] = System.currentTimeMillis();
    }
  }
  
  /**
   * A mutable integer that lets queue objects be reused once they start overflowing.
   */
  private static class MI {
    MI() { }
    public int value;
  }

  /**
   * A bounded priority queue for Integers, to retain a specified number of
   * the highest-weighted values for return as a random sample.
   */
  private static class IntPriorityQueue extends PriorityQueue<MI> {

    /**
     * Creates a bounded PQ of size <code>size</code>.
     * @param size The number of elements to retain.
     */
    public IntPriorityQueue(int size) {
      super(size);
    }

    /**
     * Returns the underlying data structure for faster access. Extracting elements
     * one at a time would require N logN time, and since we want the elements sorted
     * in ascending order by value (not weight), the array is useful as-is.
     * @return The underlying heap array.
     */
    public Object[] getHeap() {
      return getHeapArray();
    }

    /**
     * Returns true if <code>o1<code>'s weight is less than that of <code>o2</code>, for
     * ordering in the PQ.
     * @return True if <code>o1</code> weighs less than <code>o2</code>.
     */
    @Override
    public boolean lessThan(MI o1, MI o2) {
      return o1.value < o2.value;
    }

  }

  /**
   * For specifying which sampling algorithm to use.
   */
  private enum Algorithm {

    /**
     * Specifies a methodical traversal algorithm, which is guaranteed to span the collection
     * at least once, and never to return duplicates. Faster than the hashing algorithm and
     * uses much less space, but the randomness of the sample may be affected by systematic
     * variations in the collection. Requires only an array for the sample, and visits only
     * the number of elements in the sample set, not the full set.
     */
    // TODO (Facet): This one produces a bimodal distribution (very flat around
    // each peak!) for collection size 10M and sample sizes 10k and 10544.
    // Figure out why.
    TRAVERSAL,

    /**
     * Specifies a Fibonacci-style hash algorithm (see Knuth, S&S), which generates a less
     * systematically distributed subset of the sampled collection than the traversal method,
     * but requires a bounded priority queue the size of the sample, and creates an object
     * containing a sampled value and its hash, for every element in the full set. 
     */
    HASHING
  }

  /**
   * For specifying whether to sort the sample.
   */
  private enum Sorted {

    /**
     * Sort resulting sample before returning.
     */
    YES,

    /**
     *Do not sort the resulting sample. 
     */
    NO
  }

  /**
   * Magic number 1: prime closest to phi, in 32 bits.
   */
  private static final long PHI_32 = 2654435769L;

  /**
   * Magic number 2: multiplicative inverse of PHI_32, modulo 2**32.
   */
  private static final long PHI_32I = 340573321L;

  /**
   * Switch to cause methods to return timings.
   */
  private static boolean returnTimings = false;

}
