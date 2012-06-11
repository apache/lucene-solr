package org.apache.lucene.util.encoding;

import java.io.IOException;
import java.io.OutputStream;

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
 * An {@link IntEncoder} which encodes values in chunks. Implementations of this
 * class assume the data which needs encoding consists of small, consecutive
 * values, and therefore the encoder is able to compress them better. You can
 * read more on the two implementations {@link FourFlagsIntEncoder} and
 * {@link EightFlagsIntEncoder}.
 * <p>
 * Extensions of this class need to implement {@link #encode(int)} in order to
 * build the proper indicator (flags). When enough values were accumulated
 * (typically the batch size), extensions can call {@link #encodeChunk()} to
 * flush the indicator and the rest of the values.
 * <p>
 * <b>NOTE:</b> flags encoders do not accept values &le; 0 (zero) in their
 * {@link #encode(int)}. For performance reasons they do not check that
 * condition, however if such value is passed the result stream may be corrupt
 * or an exception will be thrown. Also, these encoders perform the best when
 * there are many consecutive small values (depends on the encoder
 * implementation). If that is not the case, the encoder will occupy 1 more byte
 * for every <i>batch</i> number of integers, over whatever
 * {@link VInt8IntEncoder} would have occupied. Therefore make sure to check
 * whether your data fits into the conditions of the specific encoder.
 * <p>
 * For the reasons mentioned above, these encoders are usually chained with
 * {@link UniqueValuesIntEncoder} and {@link DGapIntEncoder} in the following
 * manner: <code><pre>
 * IntEncoder fourFlags = 
 *         new SortingEncoderFilter(new UniqueValuesIntEncoder(new DGapIntEncoder(new FlagsIntEncoderImpl())));
 * </code></pre>
 * 
 * @lucene.experimental
 */
public abstract class ChunksIntEncoder extends IntEncoder {

  /** Holds the values which must be encoded, outside the indicator. */
  protected final int[] encodeQueue;
  protected int encodeQueueSize = 0;

  /** Encoder used to encode values outside the indicator. */
  protected final IntEncoder encoder = new VInt8IntEncoder();

  /** Represents bits flag byte. */
  protected int indicator = 0;

  /** Counts the current ordinal of the encoded value. */
  protected byte ordinal = 0;

  protected ChunksIntEncoder(int chunkSize) {
    encodeQueue = new int[chunkSize];
  }

  /**
   * Encodes the values of the current chunk. First it writes the indicator, and
   * then it encodes the values outside the indicator.
   */
  protected void encodeChunk() throws IOException {
    out.write(indicator);
    for (int i = 0; i < encodeQueueSize; ++i) {
      encoder.encode(encodeQueue[i]);
    }
    encodeQueueSize = 0;
    ordinal = 0;
    indicator = 0;
  }

  @Override
  public void close() throws IOException {
    if (ordinal != 0) {
      encodeChunk();
    }
    encoder.close();
    super.close();
  }

  @Override
  public void reInit(OutputStream out) {
    encoder.reInit(out);
    super.reInit(out);
    ordinal = 0;
    indicator = 0;
    encodeQueueSize = 0;
  }

}
