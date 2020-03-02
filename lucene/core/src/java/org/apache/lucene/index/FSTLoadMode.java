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

package org.apache.lucene.index;

/**
 * An enum that allows to control if term index FSTs are loaded into memory or read off-heap
 *
 * @lucene.experimental
 */
public enum FSTLoadMode {
  /**
   * Always read FSTs from disk.
   * NOTE: If this option is used the FST will be read off-heap even if buffered directory implementations
   * are used.
   */
  OFF_HEAP,
  /**
   * Never read FSTs from disk ie. all fields FSTs are loaded into memory
   */
  ON_HEAP,
  /**
   * Always read FSTs from disk.
   * An exception is made for ID fields in an IndexWriter context which are always loaded into memory.
   * This is useful to guarantee best update performance even if a non MMapDirectory is used.
   * NOTE: If this option is used the FST will be read off-heap even if buffered directory implementations
   * are used.
   * See {@link FSTLoadMode#AUTO}
   */
  OPTIMIZE_UPDATES_OFF_HEAP,
  /**
   * Automatically make the decision if FSTs are read from disk depending if the segment read from an MMAPDirectory
   * An exception is made for ID fields in an IndexWriter context which are always loaded into memory.
   */
  AUTO;

  /** Attribute key for fst mode. */
  public static final String ATTRIBUTE_KEY = "terms.fst.mode";
}
