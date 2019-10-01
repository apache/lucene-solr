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

package org.apache.lucene.search;

/**
 * Maintains bottom feature value across multiple collectors
 */
public class FieldValueChecker {
  private volatile Object value;
  private FieldComparator[] fieldComparators;
  private int[] reverseMul;

  public FieldValueChecker(final FieldComparator[] fieldComparators, final int[] reverseMul) {
    assert fieldComparators.length == reverseMul.length;

    this.fieldComparators = fieldComparators;
    this.reverseMul = reverseMul;
  }

  boolean isBottomValuePresent() {
    //return false;
    return this.value != null;
  }

  void checkAndUpdateBottomValue(Object value) {
    synchronized (this) {
      if (this.value == null) {
        this.value = value;
        return;
      }

      if (isValueCompetitive(value, 0)== true) {
        this.value = value;
      }
    }
  }

  boolean isValueCompetitive(Object value, int doc) {
    if (value == null || this.value == null) {
      //System.out.println("Null case false " + doc);
      return false;
    }

    if (value instanceof Object[]) {
      assert this.value instanceof Object[];

      Object[] baseValue = (Object[]) this.value;
      Object[] candidateValue = (Object[]) value;

      assert baseValue.length == candidateValue.length;
      assert baseValue.length == fieldComparators.length;

      for (int i = 0; i < baseValue.length; i++) {
        int resultValue = reverseMul[i] * fieldComparators[i].compareValues(baseValue[i], candidateValue[i]);
        if (resultValue != 0) {
          if (resultValue > 0) {
            //System.out.println("True1 " + doc);
            return true;
          }

          //System.out.println("False1 " + doc);
          return false;
        }
      }

      // For equal values, we return false since docs are collected in order
      //System.out.println("False2 " + doc);
      return false;
    }

    return reverseMul[0] * fieldComparators[0].compareValues(this.value, value) > 0;
  }

  /* Create a FieldValueChecker instance from given sort parameters */
  static FieldValueChecker createFieldValueChecker(Sort sort, int numHits) {
    // Create dummy PQ to get comparators and reverseMul
    FieldValueHitQueue<FieldValueHitQueue.Entry> queue = FieldValueHitQueue.create(sort.fields, numHits);

    return new FieldValueChecker(queue.getComparators(), queue.getReverseMul());
  }
}
