package org.apache.lucene.index;

/**
 * Copyright 2007 The Apache Software Foundation
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Convenience class for holding TermVector information.
 */
public class TermVectorEntry {
  private String field;
  private String term;
  private int frequency;
  private TermVectorOffsetInfo [] offsets;
  int [] positions;


  public TermVectorEntry() {
  }

  public TermVectorEntry(String field, String term, int frequency, TermVectorOffsetInfo[] offsets, int[] positions) {
    this.field = field;
    this.term = term;
    this.frequency = frequency;
    this.offsets = offsets;
    this.positions = positions;
  }


  public String getField() {
    return field;
  }

  public int getFrequency() {
    return frequency;
  }

  public TermVectorOffsetInfo[] getOffsets() {
    return offsets;
  }

  public int[] getPositions() {
    return positions;
  }

  public String getTerm() {
    return term;
  }

  //Keep package local
  void setFrequency(int frequency) {
    this.frequency = frequency;
  }

  void setOffsets(TermVectorOffsetInfo[] offsets) {
    this.offsets = offsets;
  }

  void setPositions(int[] positions) {
    this.positions = positions;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TermVectorEntry that = (TermVectorEntry) o;

    if (term != null ? !term.equals(that.term) : that.term != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return (term != null ? term.hashCode() : 0);
  }

  @Override
  public String toString() {
    return "TermVectorEntry{" +
            "field='" + field + '\'' +
            ", term='" + term + '\'' +
            ", frequency=" + frequency +
            '}';
  }
}
