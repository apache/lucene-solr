package org.apache.lucene.facet.index.attributes;

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
 * A {@link CategoryProperty} holding the ordinal from the taxonomy of the
 * current category in {@link CategoryAttribute}.
 * <p>
 * Ordinal properties are added internally during processing of category
 * streams, and it is recommended not to use it externally.
 * 
 * @lucene.experimental
 */
public class OrdinalProperty implements CategoryProperty {

  protected int ordinal = -1;

  public int getOrdinal() {
    return ordinal;
  }

  public boolean hasBeenSet() {
    return this.ordinal >= 0;
  }

  public void setOrdinal(int value) {
    this.ordinal = value;
  }

  public void clear() {
    this.ordinal = -1;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof OrdinalProperty)) {
      return false;
    }
    OrdinalProperty o = (OrdinalProperty) other;
    return o.ordinal == this.ordinal;
  }

  @Override
  public int hashCode() {
    return this.ordinal;
  }

  public void merge(CategoryProperty other) {
    throw new UnsupportedOperationException(
    "Merging ordinal attributes is prohibited");
  }

}
