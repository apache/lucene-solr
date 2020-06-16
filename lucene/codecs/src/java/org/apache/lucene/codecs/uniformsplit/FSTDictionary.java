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

package org.apache.lucene.codecs.uniformsplit;

import java.io.IOException;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.OffHeapFSTStore;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

/**
 * Immutable stateless {@link FST}-based index dictionary kept in memory.
 * <p>
 * Use {@link IndexDictionary.Builder} to build the {@link IndexDictionary}.
 * <p>
 * Create a stateful {@link IndexDictionary.Browser} to seek a term in this
 * {@link IndexDictionary} and get its corresponding block file pointer to
 * the terms block file.
 * <p>
 * Its greatest advantage is to be very compact in memory thanks to both
 * the compaction of the {@link FST} as a byte array, and the incremental
 * encoding of the leaves block pointer values, which are long integers in
 * increasing order, with {@link PositiveIntOutputs}.<br>
 * With a compact dictionary in memory we can increase the number of blocks.
 * This allows us to reduce the average block size, which means faster scan
 * inside a block.
 *
 * @lucene.experimental
 */
public class FSTDictionary implements IndexDictionary {

  private static final long BASE_RAM_USAGE = RamUsageEstimator.shallowSizeOfInstance(FSTDictionary.class);

  protected final FST<Long> fst;

  protected FSTDictionary(FST<Long> fst) {
    this.fst = fst;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_USAGE + fst.ramBytesUsed();
  }

  @Override
  public void write(DataOutput output, BlockEncoder blockEncoder) throws IOException {
    if (blockEncoder == null) {
      fst.save(output, output);
    } else {
      ByteBuffersDataOutput bytesDataOutput = ByteBuffersDataOutput.newResettableInstance();
      fst.save(bytesDataOutput, bytesDataOutput);
      BlockEncoder.WritableBytes encodedBytes = blockEncoder.encode(bytesDataOutput.toDataInput(), bytesDataOutput.size());
      output.writeVLong(encodedBytes.size());
      encodedBytes.writeTo(output);
    }
  }

  /**
   * Reads a {@link FSTDictionary} from the provided input.
   * @param blockDecoder The {@link BlockDecoder} to use for specific decoding; or null if none.
   */
  protected static FSTDictionary read(DataInput input, BlockDecoder blockDecoder, boolean isFSTOnHeap) throws IOException {
    DataInput fstDataInput;
    if (blockDecoder == null) {
      fstDataInput = input;
    } else {
      long numBytes = input.readVLong();
      BytesRef decodedBytes = blockDecoder.decode(input, numBytes);
      fstDataInput = new ByteArrayDataInput(decodedBytes.bytes, 0, decodedBytes.length);
      // OffHeapFSTStore.init() requires a DataInput which is an instance of IndexInput.
      // When the block is decoded we must load the FST on heap.
      isFSTOnHeap = true;
    }
    PositiveIntOutputs fstOutputs = PositiveIntOutputs.getSingleton();
    FST<Long> fst = isFSTOnHeap ? new FST<>(fstDataInput, fstDataInput, fstOutputs)
        : new FST<>(fstDataInput, fstDataInput, fstOutputs, new OffHeapFSTStore());
    return new FSTDictionary(fst);
  }

  @Override
  public Browser browser() {
    return new Browser();
  }

  /**
   * Stateful {@link Browser} to seek a term in this {@link FSTDictionary}
   * and get its corresponding block file pointer in the block file.
   */
  protected class Browser implements IndexDictionary.Browser {

    protected final BytesRefFSTEnum<Long> fstEnum = new BytesRefFSTEnum<>(fst);

    @Override
    public long seekBlock(BytesRef term) throws IOException {
      BytesRefFSTEnum.InputOutput<Long> seekFloor = fstEnum.seekFloor(term);
      return seekFloor == null ? -1 : seekFloor.output;
    }
  }

  /**
   * Provides stateful {@link Browser} to seek in the {@link FSTDictionary}.
   *
   * @lucene.experimental
   */
  public static class BrowserSupplier implements IndexDictionary.BrowserSupplier {

    protected final IndexInput dictionaryInput;
    protected final BlockDecoder blockDecoder;
    protected final boolean isFSTOnHeap;

    /**
     * Lazy loaded immutable index dictionary FST.
     * The FST is either kept off-heap, or hold in RAM on-heap.
     */
    protected IndexDictionary dictionary;

    public BrowserSupplier(IndexInput dictionaryInput, long dictionaryStartFP, BlockDecoder blockDecoder, boolean isFSTOnHeap) throws IOException {
      this.dictionaryInput = dictionaryInput.clone();
      this.dictionaryInput.seek(dictionaryStartFP);
      this.blockDecoder = blockDecoder;
      this.isFSTOnHeap = isFSTOnHeap;
    }

    @Override
    public IndexDictionary.Browser get() throws IOException {
      // This double-check idiom does not require the dictionary to be volatile
      // because it is immutable. See section "Double-Checked Locking Immutable Objects"
      // of https://www.cs.umd.edu/~pugh/java/memoryModel/DoubleCheckedLocking.html.
      if (dictionary == null) {
        synchronized (this) {
          if (dictionary == null) {
            dictionary = read(dictionaryInput, blockDecoder, isFSTOnHeap);
          }
        }
      }
      return dictionary.browser();
    }

    @Override
    public long ramBytesUsed() {
      return dictionary == null ? 0 : dictionary.ramBytesUsed();
    }
  }

  /**
   * Builds an immutable {@link FSTDictionary}.
   *
   * @lucene.experimental
   */
  public static class Builder implements IndexDictionary.Builder {

    protected final org.apache.lucene.util.fst.Builder<Long> fstBuilder;
    protected final IntsRefBuilder scratchInts;

    public Builder() {
      PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
      fstBuilder = new org.apache.lucene.util.fst.Builder<>(FST.INPUT_TYPE.BYTE1, outputs);
      scratchInts = new IntsRefBuilder();
    }

    @Override
    public void add(BytesRef blockKey, long blockFilePointer) throws IOException {
      fstBuilder.add(Util.toIntsRef(blockKey, scratchInts), blockFilePointer);
    }

    @Override
    public FSTDictionary build() throws IOException {
      return new FSTDictionary(fstBuilder.finish());
    }
  }
}
