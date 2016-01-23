package org.apache.lucene.facet;

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

/** Single label and its value, usually contained in a
 *  {@link FacetResult}. */
public final class LabelAndValue {
  /** Facet's label. */
  public final String label;

  /** Value associated with this label. */
  public final Number value;

  /** Sole constructor. */
  public LabelAndValue(String label, Number value) {
    this.label = label;
    this.value = value;
  }

  @Override
  public String toString() {
    return label + " (" + value + ")";
  }

  @Override
  public boolean equals(Object _other) {
    if ((_other instanceof LabelAndValue) == false) {
      return false;
    }
    LabelAndValue other = (LabelAndValue) _other;
    return label.equals(other.label) && value.equals(other.value);
  }

  @Override
  public int hashCode() {
    return label.hashCode() + 1439 * value.hashCode();
  }
}
