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

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;
import static org.apache.lucene.util.RamUsageEstimator.alignObjectSize;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.index.TermState;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Utility methods to estimate the RAM usage of objects. It relies on {@link RamUsageEstimator}.
 *
 * @lucene.experimental
 */
public class RamUsageUtil {

  private static final long BYTES_REF_BASE_RAM_USAGE = shallowSizeOfInstance(BytesRef.class);
  private static final long BYTES_REF_BUILDER_BASE_RAM_USAGE =
      shallowSizeOfInstance(BytesRefBuilder.class);
  private static final long HASH_MAP_BASE_RAM_USAGE =
      RamUsageEstimator.shallowSizeOfInstance(HashMap.class);
  private static final long HASH_MAP_ENTRY_BASE_RAM_USAGE;
  private static final long UNMODIFIABLE_ARRAY_LIST_BASE_RAM_USAGE;

  static {
    Map<Object, Object> map = new HashMap<>();
    map.put(map, map);
    HASH_MAP_ENTRY_BASE_RAM_USAGE =
        RamUsageEstimator.shallowSizeOf(map.entrySet().iterator().next());
    UNMODIFIABLE_ARRAY_LIST_BASE_RAM_USAGE =
        RamUsageEstimator.shallowSizeOf(Collections.unmodifiableList(new ArrayList<>()))
            + RamUsageEstimator.shallowSizeOfInstance(ArrayList.class);
  }

  public static long ramBytesUsed(BytesRef bytesRef) {
    return BYTES_REF_BASE_RAM_USAGE + RamUsageEstimator.sizeOf(bytesRef.bytes);
  }

  public static long ramBytesUsed(BytesRefBuilder bytesRefBuilder) {
    return BYTES_REF_BUILDER_BASE_RAM_USAGE + ramBytesUsed(bytesRefBuilder.get());
  }

  public static long ramBytesUsed(TermState termState) {
    return DeltaBaseTermStateSerializer.ramBytesUsed(termState);
  }

  public static long ramBytesUsedByByteArrayOfLength(int length) {
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Byte.BYTES * length);
  }

  public static long ramBytesUsedByHashMapOfSize(int size) {
    return HASH_MAP_BASE_RAM_USAGE
        + RamUsageUtil.ramBytesUsedByObjectArrayOfLength((int) (size / 0.6))
        + HASH_MAP_ENTRY_BASE_RAM_USAGE * size;
  }

  public static long ramBytesUsedByUnmodifiableArrayListOfSize(int size) {
    return UNMODIFIABLE_ARRAY_LIST_BASE_RAM_USAGE + ramBytesUsedByObjectArrayOfLength(size);
  }

  public static long ramBytesUsedByObjectArrayOfLength(int length) {
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_OBJECT_REF * length);
  }
}
