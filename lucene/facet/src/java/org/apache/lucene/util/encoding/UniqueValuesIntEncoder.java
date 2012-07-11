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
 * An {@link IntEncoderFilter} which ensures only unique values are encoded. The
 * implementation assumes the values given to {@link #encode(int)} are sorted.
 * If this is not the case, you can chain this encoder with
 * {@link SortingIntEncoder}.
 * 
 * @lucene.experimental
 */
public final class UniqueValuesIntEncoder extends IntEncoderFilter {

  /**
   * Denotes an illegal value which we can use to init 'prev' to. Since all
   * encoded values are integers, this value is init to MAX_INT+1 and is of type
   * long. Therefore we are guaranteed not to get this value in encode.
   */
  private static final long ILLEGAL_VALUE = Integer.MAX_VALUE + 1;

  private long prev = ILLEGAL_VALUE;
  
  /** Constructs a new instance with the given encoder. */
  public UniqueValuesIntEncoder(IntEncoder encoder) {
    super(encoder);
  }

  @Override
  public void encode(int value) throws IOException {
    if (prev != value) {
      encoder.encode(value);
      prev = value;
    }
  }

  @Override
  public IntDecoder createMatchingDecoder() {
    return encoder.createMatchingDecoder();
  }
  
  @Override
  public void reInit(OutputStream out) {
    super.reInit(out);
    prev = ILLEGAL_VALUE;
  }

  @Override
  public String toString() {
    return "Unique (" + encoder.toString() + ")";
  }
  
}
