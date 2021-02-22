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
package org.apache.lucene.analysis.hunspell;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.util.ArrayUtil;

/**
 * A structure similar to {@link org.apache.lucene.util.BytesRefHash}, but specialized for sorted
 * char sequences used for Hunspell flags. It deduplicates flag sequences, gives them unique ids,
 * stores the sequences in a contiguous char[] (via {@link #finish()} and allows to query presence
 * of the flags later via {@link Lookup#hasFlag}.
 */
class FlagEnumerator {
  private final StringBuilder builder = new StringBuilder();
  private final Map<String, Integer> indices = new HashMap<>();

  FlagEnumerator() {
    add(new char[0]); // no flags -> ord 0
  }

  int add(char[] chars) {
    Arrays.sort(chars);
    String key = new String(chars);
    if (key.length() > Character.MAX_VALUE) {
      throw new IllegalArgumentException("Too many flags: " + key);
    }

    Integer existing = indices.get(key);
    if (existing != null) {
      return existing;
    }

    int result = builder.length();
    indices.put(key, result);
    builder.append((char) key.length());
    builder.append(key);
    return result;
  }

  Lookup finish() {
    char[] result = new char[builder.length()];
    builder.getChars(0, builder.length(), result, 0);
    return new Lookup(result);
  }

  static class Lookup {
    private final char[] data;

    private Lookup(char[] data) {
      this.data = data;
    }

    boolean hasFlag(int entryId, char flag) {
      if (entryId < 0 || flag == Dictionary.FLAG_UNSET) return false;

      int length = data[entryId];
      for (int i = entryId + 1; i < entryId + 1 + length; i++) {
        char c = data[i];
        if (c == flag) return true;
        if (c > flag) return false;
      }
      return false;
    }

    char[] getFlags(int entryId) {
      return ArrayUtil.copyOfSubArray(data, entryId + 1, entryId + 1 + data[entryId]);
    }
  }
}
