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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.SortedInputIterator;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.OfflineSorter.ByteSequencesWriter;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.fst.Util.Result;
import org.apache.lucene.util.fst.Util.TopResults;

/**
 * Suggester based on a weighted FST: it first traverses the prefix, 
 * then walks the <i>n</i> shortest paths to retrieve top-ranked
 * suggestions.
 * <p>
 * <b>NOTE</b>:
 * Input weights must be between 0 and {@link Integer#MAX_VALUE}, any
 * other values will be rejected.
 * 
 * @lucene.experimental
 */
// redundant 'implements Accountable' to workaround javadocs bugs
public class WFSTCompletionLookup extends Lookup implements Accountable {
  
  /**
   * FST<Long>, weights are encoded as costs: (Integer.MAX_VALUE-weight)
   */
  // NOTE: like FSTSuggester, this is really a WFSA, if you want to
  // customize the code to add some output you should use PairOutputs.
  private FST<Long> fst = null;
  
  /** 
   * True if exact match suggestions should always be returned first.
   */
  private final boolean exactFirst;
  
  /** Number of entries the lookup was built with */
  private long count = 0;

  private final Directory tempDir;
  private final String tempFileNamePrefix;

  /**
   * Calls {@link #WFSTCompletionLookup(Directory,String,boolean) WFSTCompletionLookup(null,null,true)}
   */
  public WFSTCompletionLookup(Directory tempDir, String tempFileNamePrefix) {
    this(tempDir, tempFileNamePrefix, true);
  }
  
  /**
   * Creates a new suggester.
   * 
   * @param exactFirst <code>true</code> if suggestions that match the 
   *        prefix exactly should always be returned first, regardless
   *        of score. This has no performance impact, but could result
   *        in low-quality suggestions.
   */
  public WFSTCompletionLookup(Directory tempDir, String tempFileNamePrefix, boolean exactFirst) {
    this.exactFirst = exactFirst;
    this.tempDir = tempDir;
    this.tempFileNamePrefix = tempFileNamePrefix;
  }
  
  @Override
  public void build(InputIterator iterator) throws IOException {
    if (iterator.hasPayloads()) {
      throw new IllegalArgumentException("this suggester doesn't support payloads");
    }
    if (iterator.hasContexts()) {
      throw new IllegalArgumentException("this suggester doesn't support contexts");
    }
    count = 0;
    BytesRef scratch = new BytesRef();
    InputIterator iter = new WFSTInputIterator(tempDir, tempFileNamePrefix, iterator);
    IntsRefBuilder scratchInts = new IntsRefBuilder();
    BytesRefBuilder previous = null;
    PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    Builder<Long> builder = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);
    while ((scratch = iter.next()) != null) {
      long cost = iter.weight();
      
      if (previous == null) {
        previous = new BytesRefBuilder();
      } else if (scratch.equals(previous.get())) {
        continue; // for duplicate suggestions, the best weight is actually
                  // added
      }
      Util.toIntsRef(scratch, scratchInts);
      builder.add(scratchInts.get(), cost);
      previous.copyBytes(scratch);
      count++;
    }
    fst = builder.finish();
  }

  
  @Override
  public boolean store(DataOutput output) throws IOException {
    output.writeVLong(count);
    if (fst == null) {
      return false;
    }
    fst.save(output);
    return true;
  }

  @Override
  public boolean load(DataInput input) throws IOException {
    count = input.readVLong();
    this.fst = new FST<>(input, PositiveIntOutputs.getSingleton());
    return true;
  }

  @Override
  public List<LookupResult> lookup(CharSequence key, Set<BytesRef> contexts, boolean onlyMorePopular, int num) {
    if (contexts != null) {
      throw new IllegalArgumentException("this suggester doesn't support contexts");
    }
    assert num > 0;

    if (onlyMorePopular) {
      throw new IllegalArgumentException("this suggester only works with onlyMorePopular=false");
    }

    if (fst == null) {
      return Collections.emptyList();
    }

    BytesRefBuilder scratch = new BytesRefBuilder();
    scratch.copyChars(key);
    int prefixLength = scratch.length();
    Arc<Long> arc = new Arc<>();
    
    // match the prefix portion exactly
    Long prefixOutput = null;
    try {
      prefixOutput = lookupPrefix(scratch.get(), arc);
    } catch (IOException bogus) { throw new RuntimeException(bogus); }
    
    if (prefixOutput == null) {
      return Collections.emptyList();
    }
    
    List<LookupResult> results = new ArrayList<>(num);
    CharsRefBuilder spare = new CharsRefBuilder();
    if (exactFirst && arc.isFinal()) {
      spare.copyUTF8Bytes(scratch.get());
      results.add(new LookupResult(spare.toString(), decodeWeight(prefixOutput + arc.nextFinalOutput())));
      if (--num == 0) {
        return results; // that was quick
      }
    }

    // complete top-N
    TopResults<Long> completions = null;
    try {
      completions = Util.shortestPaths(fst, arc, prefixOutput, weightComparator, num, !exactFirst);
      assert completions.isComplete;
    } catch (IOException bogus) {
      throw new RuntimeException(bogus);
    }
    
    BytesRefBuilder suffix = new BytesRefBuilder();
    for (Result<Long> completion : completions) {
      scratch.setLength(prefixLength);
      // append suffix
      Util.toBytesRef(completion.input, suffix);
      scratch.append(suffix);
      spare.copyUTF8Bytes(scratch.get());
      results.add(new LookupResult(spare.toString(), decodeWeight(completion.output)));
    }
    return results;
  }
  
  private Long lookupPrefix(BytesRef scratch, Arc<Long> arc) throws /*Bogus*/IOException {
    assert 0 == fst.outputs.getNoOutput().longValue();
    long output = 0;
    BytesReader bytesReader = fst.getBytesReader();
    
    fst.getFirstArc(arc);
    
    byte[] bytes = scratch.bytes;
    int pos = scratch.offset;
    int end = pos + scratch.length;
    while (pos < end) {
      if (fst.findTargetArc(bytes[pos++] & 0xff, arc, arc, bytesReader) == null) {
        return null;
      } else {
        output += arc.output().longValue();
      }
    }
    
    return output;
  }
  
  /**
   * Returns the weight associated with an input string,
   * or null if it does not exist.
   */
  public Object get(CharSequence key) {
    if (fst == null) {
      return null;
    }
    Arc<Long> arc = new Arc<>();
    Long result = null;
    try {
      result = lookupPrefix(new BytesRef(key), arc);
    } catch (IOException bogus) { throw new RuntimeException(bogus); }
    if (result == null || !arc.isFinal()) {
      return null;
    } else {
      return Integer.valueOf(decodeWeight(result + arc.nextFinalOutput()));
    }
  }
  
  /** cost -&gt; weight */
  private static int decodeWeight(long encoded) {
    return (int)(Integer.MAX_VALUE - encoded);
  }
  
  /** weight -&gt; cost */
  private static int encodeWeight(long value) {
    if (value < 0 || value > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("cannot encode value: " + value);
    }
    return Integer.MAX_VALUE - (int)value;
  }
  
  private static final class WFSTInputIterator extends SortedInputIterator {

    WFSTInputIterator(Directory tempDir, String tempFileNamePrefix, InputIterator source) throws IOException {
      super(tempDir, tempFileNamePrefix, source);
      assert source.hasPayloads() == false;
    }

    @Override
    protected void encode(ByteSequencesWriter writer, ByteArrayDataOutput output, byte[] buffer, BytesRef spare, BytesRef payload, Set<BytesRef> contexts, long weight) throws IOException {
      if (spare.length + 4 >= buffer.length) {
        buffer = ArrayUtil.grow(buffer, spare.length + 4);
      }
      output.reset(buffer);
      output.writeBytes(spare.bytes, spare.offset, spare.length);
      output.writeInt(encodeWeight(weight));
      writer.write(buffer, 0, output.getPosition());
    }
    
    @Override
    protected long decode(BytesRef scratch, ByteArrayDataInput tmpInput) {
      scratch.length -= 4; // int
      // skip suggestion:
      tmpInput.reset(scratch.bytes, scratch.offset+scratch.length, 4);
      return tmpInput.readInt();
    }
  }
  
  static final Comparator<Long> weightComparator = new Comparator<Long> () {
    @Override
    public int compare(Long left, Long right) {
      return left.compareTo(right);
    }  
  };

  /** Returns byte size of the underlying FST. */
  @Override
  public long ramBytesUsed() {
    return (fst == null) ? 0 : fst.ramBytesUsed();
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    if (fst == null) {
      return Collections.emptyList();
    } else {
      return Collections.singleton(Accountables.namedAccountable("fst", fst));
    }
  }

  @Override
  public long getCount() {
    return count;
  }
}
