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

import org.apache.lucene.search.Sort;
import org.apache.lucene.util.Version;

/**
 * Provides read-only metadata about a leaf.
 * @lucene.experimental
 */
public final class LeafMetaData {

  private final int createdVersionMajor;
  private final Version minVersion;
  private final Sort sort;

  /** Expert: Sole constructor. Public for use by custom {@link LeafReader} impls. */
  public LeafMetaData(int createdVersionMajor, Version minVersion, Sort sort) {
    this.createdVersionMajor = createdVersionMajor;
    if (createdVersionMajor > Version.LATEST.major) {
      throw new IllegalArgumentException("createdVersionMajor is in the future: " + createdVersionMajor);
    }
    if (createdVersionMajor < 6) {
      throw new IllegalArgumentException("createdVersionMajor must be >= 6, got: " + createdVersionMajor);
    }
    if (minVersion != null && minVersion.onOrAfter(Version.LUCENE_7_0_0) == false) {
      throw new IllegalArgumentException("minVersion must be >= 7.0.0: " + minVersion);
    }
    if (createdVersionMajor >= 7 && minVersion == null) {
      throw new IllegalArgumentException("minVersion must be set when createdVersionMajor is >= 7");
    }
    this.minVersion = minVersion;
    this.sort = sort;
  }

  /** Get the Lucene version that created this index. This can be used to implement
   *  backward compatibility on top of the codec API. A return value of {@code 6}
   *  indicates that the created version is unknown. */
  public int getCreatedVersionMajor() {
    return createdVersionMajor;
  }

  /**
   * Return the minimum Lucene version that contributed documents to this index,
   * or {@code null} if this information is not available.
   */
  public Version getMinVersion() {
    return minVersion;
  }

  /**
   * Return the order in which documents from this index are sorted, or
   * {@code null} if documents are in no particular order.
   */
  public Sort getSort() {
    return sort;
  }

}
