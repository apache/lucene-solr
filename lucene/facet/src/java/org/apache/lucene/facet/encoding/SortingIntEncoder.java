package org.apache.lucene.facet.encoding;

import java.util.Arrays;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

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
 * An {@link IntEncoderFilter} which sorts the values to encode in ascending
 * order before encoding them.
 * 
 * @lucene.experimental
 */
public final class SortingIntEncoder extends IntEncoderFilter {

  /** Initializes with the given encoder. */
  public SortingIntEncoder(IntEncoder encoder) {
    super(encoder);
  }

  @Override
  public void encode(IntsRef values, BytesRef buf) {
    Arrays.sort(values.ints, values.offset, values.offset + values.length);
    encoder.encode(values, buf);
  }

  @Override
  public IntDecoder createMatchingDecoder() {
    return encoder.createMatchingDecoder();
  }
  
  @Override
  public String toString() {
    return "Sorting(" + encoder.toString() + ")";
  }
  
}
