package org.apache.lucene.search.suggest.fst;

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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.search.spell.TermFreqIterator;
import org.apache.lucene.search.spell.TermFreqPayloadIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.Sort.ByteSequencesWriter;
import org.apache.lucene.search.suggest.SortedTermFreqIteratorWrapper;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util.MinResult;
import org.apache.lucene.util.fst.Util;

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
public class WFSTCompletionLookup extends Lookup {
  
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
  
  /**
   * Calls {@link #WFSTCompletionLookup(boolean) WFSTCompletionLookup(true)}
   */
  public WFSTCompletionLookup() {
    this(true);
  }
  
  /**
   * Creates a new suggester.
   * 
   * @param exactFirst <code>true</code> if suggestions that match the 
   *        prefix exactly should always be returned first, regardless
   *        of score. This has no performance impact, but could result
   *        in low-quality suggestions.
   */
  public WFSTCompletionLookup(boolean exactFirst) {
    this.exactFirst = exactFirst;
  }
  
  @Override
  public void build(TermFreqIterator iterator) throws IOException {
    if (iterator instanceof TermFreqPayloadIterator) {
      throw new IllegalArgumentException("this suggester doesn't support payloads");
    }
    BytesRef scratch = new BytesRef();
    TermFreqIterator iter = new WFSTTermFreqIteratorWrapper(iterator);
    IntsRef scratchInts = new IntsRef();
    BytesRef previous = null;
    PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);
    Builder<Long> builder = new Builder<Long>(FST.INPUT_TYPE.BYTE1, outputs);
    while ((scratch = iter.next()) != null) {
      long cost = iter.weight();
      
      if (previous == null) {
        previous = new BytesRef();
      } else if (scratch.equals(previous)) {
        continue; // for duplicate suggestions, the best weight is actually
                  // added
      }
      Util.toIntsRef(scratch, scratchInts);
      builder.add(scratchInts, cost);
      previous.copyBytes(scratch);
    }
    fst = builder.finish();
  }

  
  @Override
  public boolean store(OutputStream output) throws IOException {
    try {
      if (fst == null) {
        return false;
      }
      fst.save(new OutputStreamDataOutput(output));
    } finally {
      IOUtils.close(output);
    }
    return true;
  }

  @Override
  public boolean load(InputStream input) throws IOException {
    try {
      this.fst = new FST<Long>(new InputStreamDataInput(input), PositiveIntOutputs.getSingleton(true));
    } finally {
      IOUtils.close(input);
    }
    return true;
  }

  @Override
  public List<LookupResult> lookup(CharSequence key, boolean onlyMorePopular, int num) {
    assert num > 0;

    if (onlyMorePopular) {
      throw new IllegalArgumentException("this suggester only works with onlyMorePopular=false");
    }

    if (fst == null) {
      return Collections.emptyList();
    }

    BytesRef scratch = new BytesRef(key);
    int prefixLength = scratch.length;
    Arc<Long> arc = new Arc<Long>();
    
    // match the prefix portion exactly
    Long prefixOutput = null;
    try {
      prefixOutput = lookupPrefix(scratch, arc);
    } catch (IOException bogus) { throw new RuntimeException(bogus); }
    
    if (prefixOutput == null) {
      return Collections.<LookupResult>emptyList();
    }
    
    List<LookupResult> results = new ArrayList<LookupResult>(num);
    CharsRef spare = new CharsRef();
    if (exactFirst && arc.isFinal()) {
      spare.grow(scratch.length);
      UnicodeUtil.UTF8toUTF16(scratch, spare);
      results.add(new LookupResult(spare.toString(), decodeWeight(prefixOutput + arc.nextFinalOutput)));
      if (--num == 0) {
        return results; // that was quick
      }
    }

    // complete top-N
    MinResult<Long> completions[] = null;
    try {
      completions = Util.shortestPaths(fst, arc, prefixOutput, weightComparator, num, !exactFirst);
    } catch (IOException bogus) {
      throw new RuntimeException(bogus);
    }
    
    BytesRef suffix = new BytesRef(8);
    for (MinResult<Long> completion : completions) {
      scratch.length = prefixLength;
      // append suffix
      Util.toBytesRef(completion.input, suffix);
      scratch.append(suffix);
      spare.grow(scratch.length);
      UnicodeUtil.UTF8toUTF16(scratch, spare);
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
        output += arc.output.longValue();
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
    Arc<Long> arc = new Arc<Long>();
    Long result = null;
    try {
      result = lookupPrefix(new BytesRef(key), arc);
    } catch (IOException bogus) { throw new RuntimeException(bogus); }
    if (result == null || !arc.isFinal()) {
      return null;
    } else {
      return Integer.valueOf(decodeWeight(result + arc.nextFinalOutput));
    }
  }
  
  /** cost -> weight */
  private static int decodeWeight(long encoded) {
    return (int)(Integer.MAX_VALUE - encoded);
  }
  
  /** weight -> cost */
  private static int encodeWeight(long value) {
    if (value < 0 || value > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("cannot encode value: " + value);
    }
    return Integer.MAX_VALUE - (int)value;
  }
  
  private final class WFSTTermFreqIteratorWrapper extends SortedTermFreqIteratorWrapper {

    WFSTTermFreqIteratorWrapper(TermFreqIterator source) throws IOException {
      super(source);
    }

    @Override
    protected void encode(ByteSequencesWriter writer, ByteArrayDataOutput output, byte[] buffer, BytesRef spare, long weight) throws IOException {
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
}
