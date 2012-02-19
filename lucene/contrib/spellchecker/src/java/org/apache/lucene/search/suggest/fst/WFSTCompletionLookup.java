package org.apache.lucene.search.suggest.fst;

/**
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.search.spell.TermFreqIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.fst.Util.MinResult;

/**
 * Suggester based on a weighted FST: it first traverses the prefix, 
 * then walks the <i>n</i> shortest paths to retrieve top-ranked
 * suggestions.
 * <p>
 * <b>NOTE</b>: Although the {@link TermFreqIterator} API specifies
 * floating point weights, input weights should be whole numbers.
 * Input weights will be cast to a java integer, and any
 * negative, infinite, or NaN values will be rejected.
 * 
 * @see Util#shortestPaths(FST, FST.Arc, int)
 * @lucene.experimental
 */
public class WFSTCompletionLookup extends Lookup {
  
  /**
   * File name for the automaton.
   * 
   * @see #store(File)
   * @see #load(File)
   */
  private static final String FILENAME = "wfst.bin";
  
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
    String prefix = getClass().getSimpleName();
    File directory = Sort.defaultTempDir();
    File tempInput = File.createTempFile(prefix, ".input", directory);
    File tempSorted = File.createTempFile(prefix, ".sorted", directory);
    
    Sort.ByteSequencesWriter writer = new Sort.ByteSequencesWriter(tempInput);
    Sort.ByteSequencesReader reader = null;
    BytesRef scratch = new BytesRef();
    
    boolean success = false;
    try {
      byte [] buffer = new byte [0];
      ByteArrayDataOutput output = new ByteArrayDataOutput(buffer);
      while (iterator.hasNext()) {
        String key = iterator.next();
        UnicodeUtil.UTF16toUTF8(key, 0, key.length(), scratch);

        if (scratch.length + 5 >= buffer.length) {
          buffer = ArrayUtil.grow(buffer, scratch.length + 5);
        }

        output.reset(buffer);
        output.writeBytes(scratch.bytes, scratch.offset, scratch.length);
        output.writeByte((byte)0); // separator: not used, just for sort order
        output.writeInt((int)encodeWeight(iterator.freq()));
        writer.write(buffer, 0, output.getPosition());
      }
      writer.close();
      new Sort().sort(tempInput, tempSorted);
      reader = new Sort.ByteSequencesReader(tempSorted);
      
      PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);
      Builder<Long> builder = new Builder<Long>(FST.INPUT_TYPE.BYTE1, outputs);
      
      BytesRef previous = null;
      BytesRef suggestion = new BytesRef();
      IntsRef scratchInts = new IntsRef();
      ByteArrayDataInput input = new ByteArrayDataInput();
      while (reader.read(scratch)) {
        suggestion.bytes = scratch.bytes;
        suggestion.offset = scratch.offset;
        suggestion.length = scratch.length - 5; // int + separator

        input.reset(scratch.bytes);
        input.skipBytes(suggestion.length + 1); // suggestion + separator
        long cost = input.readInt();
   
        if (previous == null) {
          previous = new BytesRef();
        } else if (suggestion.equals(previous)) {
          continue; // for duplicate suggestions, the best weight is actually added
        }
        Util.toIntsRef(suggestion, scratchInts);
        builder.add(scratchInts, cost);
        previous.copyBytes(suggestion);
      }
      fst = builder.finish();
      success = true;
    } finally {
      if (success) {
        IOUtils.close(reader, writer);
      } else {
        IOUtils.closeWhileHandlingException(reader, writer);
      }
      
      tempInput.delete();
      tempSorted.delete();
    }
  }

  @Override
  public boolean store(File storeDir) throws IOException {
    fst.save(new File(storeDir, FILENAME));
    return true;
  }

  @Override
  public boolean load(File storeDir) throws IOException {
    this.fst = FST.read(new File(storeDir, FILENAME), PositiveIntOutputs.getSingleton(true));
    return true;
  }

  @Override
  public List<LookupResult> lookup(String key, boolean onlyMorePopular, int num) {
    assert num > 0;
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
    if (exactFirst && arc.isFinal()) {
      results.add(new LookupResult(scratch.utf8ToString(), decodeWeight(prefixOutput + arc.nextFinalOutput)));
      if (--num == 0) {
        return results; // that was quick
      }
    }
    
    // complete top-N
    MinResult completions[] = null;
    try {
      completions = Util.shortestPaths(fst, arc, num);
    } catch (IOException bogus) { throw new RuntimeException(bogus); }
    
    BytesRef suffix = new BytesRef(8);
    for (MinResult completion : completions) {
      scratch.length = prefixLength;
      // append suffix
      Util.toBytesRef(completion.input, suffix);
      scratch.append(suffix);

      results.add(new LookupResult(scratch.utf8ToString(), decodeWeight(prefixOutput + completion.output)));
    }
    return results;
  }
  
  private Long lookupPrefix(BytesRef scratch, Arc<Long> arc) throws /*Bogus*/IOException {
    assert 0 == fst.outputs.getNoOutput().longValue();
    long output = 0;
    BytesReader bytesReader = fst.getBytesReader(0);
    
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
  
  @Override
  public boolean add(String key, Object value) {
    return false; // Not supported.
  }

  /**
   * Returns the weight associated with an input string,
   * or null if it does not exist.
   */
  @Override
  public Float get(String key) {
    Arc<Long> arc = new Arc<Long>();
    Long result = null;
    try {
      result = lookupPrefix(new BytesRef(key), arc);
    } catch (IOException bogus) { throw new RuntimeException(bogus); }
    if (result == null || !arc.isFinal()) {
      return null;
    } else {
      return decodeWeight(result + arc.nextFinalOutput);
    }
  }
  
  /** cost -> weight */
  private static float decodeWeight(long encoded) {
    return Integer.MAX_VALUE - encoded;
  }
  
  /** weight -> cost */
  private static long encodeWeight(float value) {
    if (Float.isNaN(value) || Float.isInfinite(value) || value < 0 || value > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("cannot encode value: " + value);
    }
    return Integer.MAX_VALUE - (int)value;
  }
}
