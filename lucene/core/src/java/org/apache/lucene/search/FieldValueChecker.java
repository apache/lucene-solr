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
  public volatile Object value;
  public volatile int minimumDoc;
  private FieldComparator[] fieldComparators;
  private int[] reverseMul;

  public FieldValueChecker(final FieldComparator[] fieldComparators, final int[] reverseMul) {
    assert fieldComparators.length == reverseMul.length;

    this.fieldComparators = fieldComparators;
    this.reverseMul = reverseMul;
  }

  boolean isBottomValuePresent() {
    return this.value != null;
  }

  void checkAndUpdateBottomValue(Object value, int doc) {
    synchronized (this) {
      if (this.value == null) {
        //System.out.println("Setting value " + value);
        this.value = value;
        this.minimumDoc = doc;
        return;
      }

      if (isValueCompetitive(value, doc)== true) {
        //System.out.println("Updating value " + this.value + " new value " + value);
        this.value = value;
        this.minimumDoc = doc;
      }
    }
  }

  boolean isValueCompetitive(Object value, int doc) {
    if (this.value == null) {
      return true;
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
            //System.out.println("True1 " + " my val " + baseValue[i] + " incoming val " + candidateValue[i]);
            return true;
          }

          //System.out.println("False1 " + " my val " + baseValue[i] + " incoming val " + candidateValue[i]);
          return false;
        }
      }

      // For equal values, we return false since docs are collected in order
      if (doc > minimumDoc) {
        //System.out.println("False2 " + " incoming doc " + doc + " my doc " + minimumDoc);
      } else {
        //System.out.println("True equal1");
        return true;
      }
      return false;
    }

    int returnValue = reverseMul[0] * fieldComparators[0].compareValues(this.value, value);
    if (returnValue > 0) {
      //System.out.println("Returning3 true" + returnValue + " for doc " + doc + " my value " + this.value + " incoming value " + value);
      return true;
    } else if (returnValue == 0) {
      //System.out.println("Returning4 true" + returnValue + " for doc " + doc + " my value " + this.value + " incoming value " + value);
      return true;
    }
    //System.out.println("Returning5 false" + returnValue + " for doc " + doc + " my value " + this.value + " incoming value " + value);
    return false;
  }

  /* Create a FieldValueChecker instance from given sort parameters */
  static FieldValueChecker createFieldValueChecker(Sort sort, int numHits) {
    // Create dummy PQ to get comparators and reverseMul
    FieldValueHitQueue<FieldValueHitQueue.Entry> queue = FieldValueHitQueue.create(sort.fields, numHits);

    return new FieldValueChecker(queue.getComparators(), queue.getReverseMul());
  }
}
