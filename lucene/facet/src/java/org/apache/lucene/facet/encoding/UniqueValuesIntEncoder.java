package org.apache.lucene.facet.encoding;

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
 * An {@link IntEncoderFilter} which ensures only unique values are encoded. The
 * implementation assumes the values given to {@link #encode(IntsRef, BytesRef)} are sorted.
 * If this is not the case, you can chain this encoder with
 * {@link SortingIntEncoder}.
 * 
 * @lucene.experimental
 */
public final class UniqueValuesIntEncoder extends IntEncoderFilter {

  /** Constructs a new instance with the given encoder. */
  public UniqueValuesIntEncoder(IntEncoder encoder) {
    super(encoder);
  }

  @Override
  public void encode(IntsRef values, BytesRef buf) {
    int prev = values.ints[values.offset];
    int idx = values.offset + 1;
    int upto = values.offset + values.length;
    for (int i = idx; i < upto; i++) {
      if (values.ints[i] != prev) {
        values.ints[idx++] = values.ints[i];
        prev = values.ints[i];
      }
    }
    values.length = idx - values.offset;
    encoder.encode(values, buf);
  }

  @Override
  public IntDecoder createMatchingDecoder() {
    return encoder.createMatchingDecoder();
  }
  
  @Override
  public String toString() {
    return "Unique(" + encoder.toString() + ")";
  }
  
}
