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
package org.apache.lucene.search.suggest.fst;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.fst.FSTCompletion.Completion;
import org.apache.lucene.search.suggest.tst.TSTLookup;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.OfflineSorter.SortInfo;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.NoOutputs;

/**
 * An adapter from {@link Lookup} API to {@link FSTCompletion}.
 * 
 * <p>This adapter differs from {@link FSTCompletion} in that it attempts
 * to discretize any "weights" as passed from in {@link InputIterator#weight()}
 * to match the number of buckets. For the rationale for bucketing, see
 * {@link FSTCompletion}.
 * 
 * <p><b>Note:</b>Discretization requires an additional sorting pass.
 * 
 * <p>The range of weights for bucketing/ discretization is determined 
 * by sorting the input by weight and then dividing into
 * equal ranges. Then, scores within each range are assigned to that bucket. 
 * 
 * <p>Note that this means that even large differences in weights may be lost 
 * during automaton construction, but the overall distinction between "classes"
 * of weights will be preserved regardless of the distribution of weights. 
 * 
 * <p>For fine-grained control over which weights are assigned to which buckets,
 * use {@link FSTCompletion} directly or {@link TSTLookup}, for example.
 * 
 * @see FSTCompletion
 * @lucene.experimental
 */
public class FSTCompletionLookup extends Lookup implements Accountable {
  /** 
   * An invalid bucket count if we're creating an object
   * of this class from an existing FST.
   * 
   * @see #FSTCompletionLookup(FSTCompletion, boolean)
   */
  private static int INVALID_BUCKETS_COUNT = -1;
  
  /**
   * Shared tail length for conflating in the created automaton. Setting this
   * to larger values ({@link Integer#MAX_VALUE}) will create smaller (or minimal) 
   * automata at the cost of RAM for keeping nodes hash in the {@link FST}. 
   *  
   * <p>Empirical pick.
   */
  private final static int sharedTailLength = 5;

  private int buckets;
  private boolean exactMatchFirst;

  /**
   * Automaton used for completions with higher weights reordering.
   */
  private FSTCompletion higherWeightsCompletion;

  /**
   * Automaton used for normal completions.
   */
  private FSTCompletion normalCompletion;

  /** Number of entries the lookup was built with */
  private long count = 0;

  /**
   * This constructor prepares for creating a suggested FST using the
   * {@link #build(InputIterator)} method. The number of weight
   * discretization buckets is set to {@link FSTCompletion#DEFAULT_BUCKETS} and
   * exact matches are promoted to the top of the suggestions list.
   */
  public FSTCompletionLookup() {
    this(FSTCompletion.DEFAULT_BUCKETS, true);
  }

  /**
   * This constructor prepares for creating a suggested FST using the
   * {@link #build(InputIterator)} method.
   * 
   * @param buckets
   *          The number of weight discretization buckets (see
   *          {@link FSTCompletion} for details).
   * 
   * @param exactMatchFirst
   *          If <code>true</code> exact matches are promoted to the top of the
   *          suggestions list. Otherwise they appear in the order of
   *          discretized weight and alphabetical within the bucket.
   */
  public FSTCompletionLookup(int buckets, boolean exactMatchFirst) {
    this.buckets = buckets;
    this.exactMatchFirst = exactMatchFirst;
  }

  /**
   * This constructor takes a pre-built automaton.
   * 
   *  @param completion 
   *          An instance of {@link FSTCompletion}.
   *  @param exactMatchFirst
   *          If <code>true</code> exact matches are promoted to the top of the
   *          suggestions list. Otherwise they appear in the order of
   *          discretized weight and alphabetical within the bucket.
   */
  public FSTCompletionLookup(FSTCompletion completion, boolean exactMatchFirst) {
    this(INVALID_BUCKETS_COUNT, exactMatchFirst);
    this.normalCompletion = new FSTCompletion(
        completion.getFST(), false, exactMatchFirst);
    this.higherWeightsCompletion =  new FSTCompletion(
        completion.getFST(), true, exactMatchFirst);
  }

  @Override
  public void build(InputIterator iterator) throws IOException {
    if (iterator.hasPayloads()) {
      throw new IllegalArgumentException("this suggester doesn't support payloads");
    }
    if (iterator.hasContexts()) {
      throw new IllegalArgumentException("this suggester doesn't support contexts");
    }
    Path tempInput = Files.createTempFile(
        OfflineSorter.getDefaultTempDir(), FSTCompletionLookup.class.getSimpleName(), ".input");
    Path tempSorted = Files.createTempFile(
        OfflineSorter.getDefaultTempDir(), FSTCompletionLookup.class.getSimpleName(), ".sorted");

    OfflineSorter.ByteSequencesWriter writer = new OfflineSorter.ByteSequencesWriter(tempInput);
    OfflineSorter.ByteSequencesReader reader = null;
    ExternalRefSorter sorter = null;

    // Push floats up front before sequences to sort them. For now, assume they are non-negative.
    // If negative floats are allowed some trickery needs to be done to find their byte order.
    boolean success = false;
    count = 0;
    try {
      byte [] buffer = new byte [0];
      ByteArrayDataOutput output = new ByteArrayDataOutput(buffer);
      BytesRef spare;
      while ((spare = iterator.next()) != null) {
        if (spare.length + 4 >= buffer.length) {
          buffer = ArrayUtil.grow(buffer, spare.length + 4);
        }

        output.reset(buffer);
        output.writeInt(encodeWeight(iterator.weight()));
        output.writeBytes(spare.bytes, spare.offset, spare.length);
        writer.write(buffer, 0, output.getPosition());
      }
      writer.close();

      // We don't know the distribution of scores and we need to bucket them, so we'll sort
      // and divide into equal buckets.
      SortInfo info = new OfflineSorter().sort(tempInput, tempSorted);
      Files.delete(tempInput);
      FSTCompletionBuilder builder = new FSTCompletionBuilder(
          buckets, sorter = new ExternalRefSorter(new OfflineSorter()), sharedTailLength);

      final int inputLines = info.lines;
      reader = new OfflineSorter.ByteSequencesReader(tempSorted);
      long line = 0;
      int previousBucket = 0;
      int previousScore = 0;
      ByteArrayDataInput input = new ByteArrayDataInput();
      BytesRefBuilder tmp1 = new BytesRefBuilder();
      BytesRef tmp2 = new BytesRef();
      while (reader.read(tmp1)) {
        input.reset(tmp1.bytes());
        int currentScore = input.readInt();

        int bucket;
        if (line > 0 && currentScore == previousScore) {
          bucket = previousBucket;
        } else {
          bucket = (int) (line * buckets / inputLines);
        }
        previousScore = currentScore;
        previousBucket = bucket;

        // Only append the input, discard the weight.
        tmp2.bytes = tmp1.bytes();
        tmp2.offset = input.getPosition();
        tmp2.length = tmp1.length() - input.getPosition();
        builder.add(tmp2, bucket);

        line++;
        count++;
      }

      // The two FSTCompletions share the same automaton.
      this.higherWeightsCompletion = builder.build();
      this.normalCompletion = new FSTCompletion(
          higherWeightsCompletion.getFST(), false, exactMatchFirst);
      
      success = true;
    } finally {
      IOUtils.closeWhileHandlingException(reader, writer, sorter);

      if (success) {
        Files.delete(tempSorted);
      } else {
        IOUtils.deleteFilesIgnoringExceptions(tempInput, tempSorted);
      }
    }
  }
  
  /** weight -&gt; cost */
  private static int encodeWeight(long value) {
    if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("cannot encode value: " + value);
    }
    return (int)value;
  }

  @Override
  public List<LookupResult> lookup(CharSequence key, Set<BytesRef> contexts, boolean higherWeightsFirst, int num) {
    if (contexts != null) {
      throw new IllegalArgumentException("this suggester doesn't support contexts");
    }
    final List<Completion> completions;
    if (higherWeightsFirst) {
      completions = higherWeightsCompletion.lookup(key, num);
    } else {
      completions = normalCompletion.lookup(key, num);
    }
    
    final ArrayList<LookupResult> results = new ArrayList<>(completions.size());
    CharsRefBuilder spare = new CharsRefBuilder();
    for (Completion c : completions) {
      spare.copyUTF8Bytes(c.utf8);
      results.add(new LookupResult(spare.toString(), c.bucket));
    }
    return results;
  }

  /**
   * Returns the bucket (weight) as a Long for the provided key if it exists,
   * otherwise null if it does not.
   */
  public Object get(CharSequence key) {
    final int bucket = normalCompletion.getBucket(key);
    return bucket == -1 ? null : Long.valueOf(bucket);
  }


  @Override
  public synchronized boolean store(DataOutput output) throws IOException {
    output.writeVLong(count);
    if (this.normalCompletion == null || normalCompletion.getFST() == null) 
      return false;
    normalCompletion.getFST().save(output);
    return true;
  }

  @Override
  public synchronized boolean load(DataInput input) throws IOException {
    count = input.readVLong();
    this.higherWeightsCompletion = new FSTCompletion(new FST<>(
        input, NoOutputs.getSingleton()));
    this.normalCompletion = new FSTCompletion(
        higherWeightsCompletion.getFST(), false, exactMatchFirst);
    return true;
  }

  @Override
  public long ramBytesUsed() {
    long mem = RamUsageEstimator.shallowSizeOf(this) + RamUsageEstimator.shallowSizeOf(normalCompletion) + RamUsageEstimator.shallowSizeOf(higherWeightsCompletion);
    if (normalCompletion != null) {
      mem += normalCompletion.getFST().ramBytesUsed();
    }
    if (higherWeightsCompletion != null && (normalCompletion == null || normalCompletion.getFST() != higherWeightsCompletion.getFST())) {
      // the fst should be shared between the 2 completion instances, don't count it twice
      mem += higherWeightsCompletion.getFST().ramBytesUsed();
    }
    return mem;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    List<Accountable> resources = new ArrayList<>();
    if (normalCompletion != null) {
      resources.add(Accountables.namedAccountable("fst", normalCompletion.getFST()));
    }
    if (higherWeightsCompletion != null && (normalCompletion == null || normalCompletion.getFST() != higherWeightsCompletion.getFST())) {
      resources.add(Accountables.namedAccountable("higher weights fst", higherWeightsCompletion.getFST()));
    }
    return Collections.unmodifiableList(resources);
  }

  @Override
  public long getCount() {
    return count;
  }
}
