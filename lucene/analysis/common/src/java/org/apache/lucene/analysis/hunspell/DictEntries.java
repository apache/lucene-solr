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

import java.util.List;

/**
 * An object representing homonym dictionary entries. Note that the order of entries here may differ
 * from the order in the *.dic file!
 *
 * @see Dictionary#lookupEntries
 */
public interface DictEntries {
  /**
   * @return a positive number of dictionary entries with the same word. Most often it's 1 (unless
   *     there are homonyms). Entries are indexed from 0 to {@code size() - 1} and these indices can
   *     be passed into other methods of this class.
   */
  int size();

  /**
   * @param entryIndex an index from 0 (inclusive) to {@link #size()} (exclusive)
   * @return morphological fields (of {@code kk:vvvvvv} form, sorted, space-separated, excluding
   *     {@code ph:}) associated with the homonym at the given entry index, or an empty string
   */
  String getMorphologicalData(int entryIndex);

  /**
   * @param entryIndex an index from 0 (inclusive) to {@link #size()} (exclusive)
   * @param key the key in the form {@code kk:} by which to filter the morphological fields
   * @return the values (of {@code vvvvvv} form) of morphological fields with the given key
   *     associated with the homonym at the given entry index
   */
  List<String> getMorphologicalValues(int entryIndex, String key);
}
