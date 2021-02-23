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

import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.BLOCK_SIZE;

import java.io.IOException;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.IntBlockTermState;
import org.apache.lucene.codecs.lucene90.Lucene90PostingsReader;
import org.apache.lucene.codecs.lucene90.Lucene90PostingsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.TermState;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * {@link TermState} serializer which encodes each file pointer as a delta relative to a base file
 * pointer. It differs from {@link Lucene90PostingsWriter#encodeTerm} which encodes each file
 * pointer as a delta relative to the previous file pointer.
 *
 * <p>It automatically sets the base file pointer to the first valid file pointer for doc start FP,
 * pos start FP, pay start FP. These base file pointers have to be {@link #resetBaseStartFP() reset}
 * by the caller before starting to write a new block.
 *
 * @lucene.experimental
 */
public class DeltaBaseTermStateSerializer implements Accountable {

  private static final long RAM_USAGE =
      RamUsageEstimator.shallowSizeOfInstance(DeltaBaseTermStateSerializer.class);
  private static final long INT_BLOCK_TERM_STATE_RAM_USAGE =
      RamUsageEstimator.shallowSizeOfInstance(IntBlockTermState.class);

  protected long baseDocStartFP;
  protected long basePosStartFP;
  protected long basePayStartFP;

  public DeltaBaseTermStateSerializer() {
    resetBaseStartFP();
  }

  /**
   * Resets the base file pointers to 0. This method has to be called before starting to write a new
   * block.
   */
  public void resetBaseStartFP() {
    this.baseDocStartFP = 0;
    this.basePosStartFP = 0;
    this.basePayStartFP = 0;
  }

  /**
   * @return The base doc start file pointer. It is the file pointer of the first {@link TermState}
   *     written after {@link #resetBaseStartFP()} is called.
   */
  public long getBaseDocStartFP() {
    return baseDocStartFP;
  }

  /**
   * @return The base position start file pointer. It is the file pointer of the first {@link
   *     TermState} written after {@link #resetBaseStartFP()} is called.
   */
  public long getBasePosStartFP() {
    return basePosStartFP;
  }

  /**
   * @return The base payload start file pointer. It is the file pointer of the first {@link
   *     TermState} written after {@link #resetBaseStartFP()} is called.
   */
  public long getBasePayStartFP() {
    return basePayStartFP;
  }

  /**
   * Writes a {@link BlockTermState} to the provided {@link DataOutput}.
   *
   * <p>Simpler variant of {@link Lucene90PostingsWriter#encodeTerm(DataOutput, FieldInfo,
   * BlockTermState, boolean)}.
   */
  public void writeTermState(
      DataOutput termStatesOutput, FieldInfo fieldInfo, BlockTermState termState)
      throws IOException {
    IndexOptions indexOptions = fieldInfo.getIndexOptions();
    boolean hasFreqs = indexOptions != IndexOptions.DOCS;
    boolean hasPositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    boolean hasOffsets =
        indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    boolean hasPayloads = fieldInfo.hasPayloads();

    IntBlockTermState intTermState = (IntBlockTermState) termState;

    termStatesOutput.writeVInt(intTermState.docFreq);
    if (hasFreqs) {
      assert intTermState.totalTermFreq >= intTermState.docFreq;
      termStatesOutput.writeVLong(intTermState.totalTermFreq - intTermState.docFreq);
    }

    if (intTermState.singletonDocID != -1) {
      termStatesOutput.writeVInt(intTermState.singletonDocID);
    } else {
      if (baseDocStartFP == 0) {
        baseDocStartFP = intTermState.docStartFP;
      }
      termStatesOutput.writeVLong(intTermState.docStartFP - baseDocStartFP);
    }

    if (hasPositions) {
      if (basePosStartFP == 0) {
        basePosStartFP = intTermState.posStartFP;
      }
      termStatesOutput.writeVLong(intTermState.posStartFP - basePosStartFP);
      if (hasPayloads || hasOffsets) {
        if (basePayStartFP == 0) {
          basePayStartFP = intTermState.payStartFP;
        }
        termStatesOutput.writeVLong(intTermState.payStartFP - basePayStartFP);
      }
      if (intTermState.lastPosBlockOffset != -1) {
        termStatesOutput.writeVLong(intTermState.lastPosBlockOffset);
      }
    }
    if (intTermState.skipOffset != -1) {
      termStatesOutput.writeVLong(intTermState.skipOffset);
    }
  }

  /**
   * Reads a {@link BlockTermState} from the provided {@link DataInput}.
   *
   * <p>Simpler variant of {@link Lucene90PostingsReader#decodeTerm(DataInput, FieldInfo,
   * BlockTermState, boolean)}.
   *
   * @param reuse {@link BlockTermState} to reuse; or null to create a new one.
   */
  public BlockTermState readTermState(
      long baseDocStartFP,
      long basePosStartFP,
      long basePayStartFP,
      DataInput termStatesInput,
      FieldInfo fieldInfo,
      BlockTermState reuse)
      throws IOException {
    IndexOptions indexOptions = fieldInfo.getIndexOptions();
    boolean hasFreqs = indexOptions != IndexOptions.DOCS;
    boolean hasPositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;

    IntBlockTermState intTermState =
        reuse != null ? reset((IntBlockTermState) reuse) : new IntBlockTermState();

    intTermState.docFreq = termStatesInput.readVInt();
    intTermState.totalTermFreq =
        hasFreqs ? intTermState.docFreq + termStatesInput.readVLong() : intTermState.docFreq;
    assert intTermState.totalTermFreq >= intTermState.docFreq;

    if (intTermState.docFreq == 1) {
      intTermState.singletonDocID = termStatesInput.readVInt();
    } else {
      intTermState.docStartFP = baseDocStartFP + termStatesInput.readVLong();
    }

    if (hasPositions) {
      intTermState.posStartFP = basePosStartFP + termStatesInput.readVLong();
      boolean hasOffsets =
          indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
      if (hasOffsets || fieldInfo.hasPayloads()) {
        intTermState.payStartFP = basePayStartFP + termStatesInput.readVLong();
      }
      if (intTermState.totalTermFreq > BLOCK_SIZE) {
        intTermState.lastPosBlockOffset = termStatesInput.readVLong();
      }
    }
    if (intTermState.docFreq > BLOCK_SIZE) {
      intTermState.skipOffset = termStatesInput.readVLong();
    }
    return intTermState;
  }

  protected IntBlockTermState reset(IntBlockTermState termState) {
    // OrdTermState.
    termState.ord = 0;

    // BlockTermState.
    termState.docFreq = 0;
    termState.totalTermFreq = 0;
    termState.termBlockOrd = 0;
    termState.blockFilePointer = 0;

    // IntBlockTermState.
    termState.docStartFP = 0;
    termState.posStartFP = 0;
    termState.payStartFP = 0;
    termState.skipOffset = -1;
    termState.lastPosBlockOffset = -1;
    termState.singletonDocID = -1;

    return termState;
  }

  @Override
  public long ramBytesUsed() {
    return RAM_USAGE;
  }

  /** @return The estimated RAM usage of the given {@link TermState}. */
  public static long ramBytesUsed(TermState termState) {
    return termState instanceof IntBlockTermState
        ? INT_BLOCK_TERM_STATE_RAM_USAGE
        : RamUsageEstimator.shallowSizeOf(termState);
  }
}
