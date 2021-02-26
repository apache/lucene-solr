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

package org.apache.lucene.util;

/**
 * {@link UnmodifiableBytesRefHash} wraps {@link BytesRefHash} and forwards only those methods that
 * are not modifying {@link BytesRefHash} structurally. Instance of {@link UnmodifiableBytesRefHash}
 * can be safely used from multiple threads as long as {@link BytesRefHash} that was used to create
 * it is no longer accessed directly.
 */
public final class UnmodifiableBytesRefHash {

  private final BytesRefHash bytesRefHash;

  public UnmodifiableBytesRefHash(BytesRefHash bytesRefHash) {
    this.bytesRefHash = bytesRefHash;
  }

  /**
   * Returns the number of {@link BytesRef} values in this {@link UnmodifiableBytesRefHash}.
   *
   * @return the number of {@link BytesRef} values in this {@link UnmodifiableBytesRefHash}.
   */
  public int size() {
    return bytesRefHash.size();
  }

  /**
   * Populates and returns a {@link BytesRef} with the bytes for the given bytesID.
   *
   * <p>Note: the given bytesID must be a positive integer less than the current size ({@link
   * #size()})
   *
   * @param bytesID the id
   * @param ref the {@link BytesRef} to populate
   * @return the given BytesRef instance populated with the bytes for the given bytesID
   */
  public BytesRef get(int bytesID, BytesRef ref) {
    return bytesRefHash.get(bytesID, ref);
  }

  /**
   * Returns the id of the given {@link BytesRef}.
   *
   * @param bytes the bytes to look for
   * @return the id of the given bytes, or {@code -1} if there is no mapping for the given bytes.
   */
  public int find(BytesRef bytes) {
    return bytesRefHash.find(bytes);
  }
}
