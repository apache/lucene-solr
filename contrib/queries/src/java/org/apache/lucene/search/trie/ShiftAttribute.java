package org.apache.lucene.search.trie;

/**
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

import org.apache.lucene.util.Attribute;

import java.io.Serializable;

/**
 * This attribute is updated by {@link IntTrieTokenStream} and {@link LongTrieTokenStream}
 * to the shift value of the current prefix-encoded token.
 * It may be used by filters or consumers to e.g. distribute the values to various fields.
 */
public final class ShiftAttribute extends Attribute implements Cloneable, Serializable {
  private int shift = 0;
  
  /**
   * Returns the shift value of the current prefix encoded token.
   */
  public int getShift() {
    return shift;
  }

  /**
   * Sets the shift value.
   */
  public void setShift(final int shift) {
    this.shift = shift;
  }
  
  public void clear() {
    shift = 0;
  }

  public String toString() {
    return "shift=" + shift;
  }

  public boolean equals(Object other) {
    if (this == other) return true;
    if (other instanceof ShiftAttribute) {
      return ((ShiftAttribute) other).shift == shift;
    }
    return false;
  }

  public int hashCode() {
    return shift;
  }
  
  public void copyTo(Attribute target) {
    final ShiftAttribute t = (ShiftAttribute) target;
    t.setShift(shift);
  }
}
