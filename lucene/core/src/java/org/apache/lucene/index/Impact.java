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
 * Per-document scoring factors.
 */
public final class Impact {

  /**
   * Term frequency of the term in the document.
   */
  public int freq;

  /**
   * Norm factor of the document.
   */
  public long norm;

  /**
   * Constructor.
   */
  public Impact(int freq, long norm) {
    this.freq = freq;
    this.norm = norm;
  }

  @Override
  public String toString() {
    return "{freq=" + freq + ",norm=" + norm + "}";
  }

  @Override
  public int hashCode() {
    int h = freq;
    h = 31 * h + Long.hashCode(norm);
    return h;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || getClass() != obj.getClass()) return false;
    Impact other = (Impact) obj;
    return freq == other.freq && norm == other.norm;
  }

}
