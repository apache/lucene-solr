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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
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

  protected final FST<Long> dictionary;

  protected FSTDictionary(FST<Long> dictionary) {
    this.dictionary = dictionary;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_USAGE + dictionary.ramBytesUsed();
  }

  @Override
  public void write(DataOutput output, BlockEncoder blockEncoder) throws IOException {
    if (blockEncoder == null) {
      dictionary.save(output);
    } else {
      ByteBuffersDataOutput bytesDataOutput = ByteBuffersDataOutput.newResettableInstance();
      dictionary.save(bytesDataOutput);
      BlockEncoder.WritableBytes encodedBytes = blockEncoder.encode(bytesDataOutput.toDataInput(), bytesDataOutput.size());
      output.writeVLong(encodedBytes.size());
      encodedBytes.writeTo(output);
    }
  }

  /**
   * Reads a {@link FSTDictionary} from the provided input.
   * @param blockDecoder The {@link BlockDecoder} to use for specific decoding; or null if none.
   */
  protected static FSTDictionary read(DataInput input, BlockDecoder blockDecoder) throws IOException {
    DataInput fstDataInput;
    if (blockDecoder == null) {
      fstDataInput = input;
    } else {
      long numBytes = input.readVLong();
      BytesRef decodedBytes = blockDecoder.decode(input, numBytes);
      fstDataInput = new ByteArrayDataInput(decodedBytes.bytes, 0, decodedBytes.length);
    }
    PositiveIntOutputs fstOutputs = PositiveIntOutputs.getSingleton();
    FST<Long> dictionary = new FST<>(fstDataInput, fstOutputs);
    return new FSTDictionary(dictionary);
  }

  @Override
  public Browser browser() {
    return new Browser();
  }

  protected class Browser implements IndexDictionary.Browser {

    protected final BytesRefFSTEnum<Long> fstEnum = new BytesRefFSTEnum<>(dictionary);

    protected static final int STATE_SEEK = 0, STATE_NEXT = 1, STATE_END = 2;
    protected int state = STATE_SEEK;

    //  Note: key and pointer are one position prior to the current fstEnum position,
    //   since we need need the fstEnum to be one ahead to calculate the prefix.
    protected final BytesRefBuilder keyBuilder = new BytesRefBuilder();
    protected int blockPrefixLen = 0;
    protected long blockFilePointer = -1;

    @Override
    public long seekBlock(BytesRef term) {
      state = STATE_SEEK;
      try {
        BytesRefFSTEnum.InputOutput<Long> seekFloor = fstEnum.seekFloor(term);
        if (seekFloor == null) {
          blockFilePointer = -1;
        } else {
          blockFilePointer = seekFloor.output;
        }
        return blockFilePointer;
      } catch (IOException e) {
        // Should never happen.
        throw new RuntimeException(e);
      }
    }

    @Override
    public BytesRef nextKey() {
      try {
        if (state == STATE_END) {
          // if fstEnum is at end, then that's it.
          return null;
        }

        if (state == STATE_SEEK && blockFilePointer == -1) { // see seekBlock
          if (fstEnum.next() == null) { // advance.
            state = STATE_END; // probably never happens (empty FST)?  We code defensively.
            return null;
          }
        }
        keyBuilder.copyBytes(fstEnum.current().input);
        blockFilePointer = fstEnum.current().output;
        assert blockFilePointer >= 0;

        state = STATE_NEXT;

        BytesRef key = keyBuilder.get();

        // advance fstEnum
        BytesRefFSTEnum.InputOutput<Long> inputOutput = fstEnum.next();

        // calc common prefix
        if (inputOutput == null) {
          state = STATE_END; // for *next* call; current state is good
          blockPrefixLen = 0;
        } else {
          int sortKeyLength = StringHelper.sortKeyLength(key, inputOutput.input);
          assert sortKeyLength >= 1;
          blockPrefixLen = sortKeyLength - 1;
        }
        return key;
      } catch (IOException e) {
        // Should never happen.
        throw new RuntimeException(e);
      }
    }

    @Override
    public BytesRef peekKey() {
      assert state != STATE_SEEK;
      return (state == STATE_END) ? null : fstEnum.current().input;
    }

    @Override
    public int getBlockPrefixLen() {
      assert state != STATE_SEEK;
      assert blockPrefixLen >= 0;
      return blockPrefixLen;
    }

    @Override
    public long getBlockFilePointer() {
      assert state != STATE_SEEK;
      assert blockFilePointer >= 0;
      return blockFilePointer;
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
    public void add(BytesRef blockKey, long blockFilePointer) {
      try {
        fstBuilder.add(Util.toIntsRef(blockKey, scratchInts), blockFilePointer);
      } catch (IOException e) {
        // Should never happen.
        throw new RuntimeException(e);
      }
    }

    @Override
    public FSTDictionary build() {
      try {
        return new FSTDictionary(fstBuilder.finish());
      } catch (IOException e) {
        // Should never happen.
        throw new RuntimeException(e);
      }
    }
  }
}
