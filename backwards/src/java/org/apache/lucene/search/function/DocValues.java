package org.apache.lucene.search.function;

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

import org.apache.lucene.search.Explanation;

/**
 * Expert: represents field values as different types.
 * Normally created via a 
 * {@link org.apache.lucene.search.function.ValueSource ValueSuorce} 
 * for a particular field and reader.
 *
 * <p><font color="#FF0000">
 * WARNING: The status of the <b>search.function</b> package is experimental. 
 * The APIs introduced here might change in the future and will not be 
 * supported anymore in such a case.</font>
 * 
 *
 */
public abstract class DocValues {
  /*
   * DocValues is distinct from ValueSource because
   * there needs to be an object created at query evaluation time that
   * is not referenced by the query itself because:
   * - Query objects should be MT safe
   * - For caching, Query objects are often used as keys... you don't
   *   want the Query carrying around big objects
   */

  /**
   * Return doc value as a float. 
   * <P>Mandatory: every DocValues implementation must implement at least this method. 
   * @param doc document whose float value is requested. 
   */
  public abstract float floatVal(int doc);
  
  /**
   * Return doc value as an int. 
   * <P>Optional: DocValues implementation can (but don't have to) override this method. 
   * @param doc document whose int value is requested.
   */
  public int intVal(int doc) { 
    return (int) floatVal(doc);
  }
  
  /**
   * Return doc value as a long. 
   * <P>Optional: DocValues implementation can (but don't have to) override this method. 
   * @param doc document whose long value is requested.
   */
  public long longVal(int doc) {
    return (long) floatVal(doc);
  }

  /**
   * Return doc value as a double. 
   * <P>Optional: DocValues implementation can (but don't have to) override this method. 
   * @param doc document whose double value is requested.
   */
  public double doubleVal(int doc) {
    return (double) floatVal(doc);
  }
  
  /**
   * Return doc value as a string. 
   * <P>Optional: DocValues implementation can (but don't have to) override this method. 
   * @param doc document whose string value is requested.
   */
  public String strVal(int doc) {
    return Float.toString(floatVal(doc));
  }
  
  /**
   * Return a string representation of a doc value, as required for Explanations.
   */
  public abstract String toString(int doc);
  
  /**
   * Explain the scoring value for the input doc.
   */
  public Explanation explain(int doc) {
    return new Explanation(floatVal(doc), toString(doc));
  }
  
  /**
   * Expert: for test purposes only, return the inner array of values, or null if not applicable.
   * <p>
   * Allows tests to verify that loaded values are:
   * <ol>
   *   <li>indeed cached/reused.</li>
   *   <li>stored in the expected size/type (byte/short/int/float).</li>
   * </ol>
   * Note: implementations of DocValues must override this method for 
   * these test elements to be tested, Otherwise the test would not fail, just 
   * print a warning.
   */
  Object getInnerArray() {
    throw new UnsupportedOperationException("this optional method is for test purposes only");
  }

  // --- some simple statistics on values
  private float minVal = Float.NaN;
  private float maxVal = Float.NaN;
  private float avgVal = Float.NaN;
  private boolean computed=false;
  // compute optional values
  private void compute() {
    if (computed) {
      return;
    }
    float sum = 0;
    int n = 0;
    while (true) {
      float val;
      try {
        val = floatVal(n);
      } catch (ArrayIndexOutOfBoundsException e) {
        break;
      }
      sum += val;
      minVal = Float.isNaN(minVal) ? val : Math.min(minVal, val);
      maxVal = Float.isNaN(maxVal) ? val : Math.max(maxVal, val);
      ++n;
    }

    avgVal = n == 0 ? Float.NaN : sum / n;
    computed = true;
  }

  /**
   * Returns the minimum of all values or <code>Float.NaN</code> if this
   * DocValues instance does not contain any value.
   * <p>
   * This operation is optional
   * </p>
   * 
   * @return the minimum of all values or <code>Float.NaN</code> if this
   *         DocValues instance does not contain any value.
   */
  public float getMinValue() {
    compute();
    return minVal;
  }

  /**
   * Returns the maximum of all values or <code>Float.NaN</code> if this
   * DocValues instance does not contain any value.
   * <p>
   * This operation is optional
   * </p>
   * 
   * @return the maximum of all values or <code>Float.NaN</code> if this
   *         DocValues instance does not contain any value.
   */
  public float getMaxValue() {
    compute();
    return maxVal;
  }

  /**
   * Returns the average of all values or <code>Float.NaN</code> if this
   * DocValues instance does not contain any value. *
   * <p>
   * This operation is optional
   * </p>
   * 
   * @return the average of all values or <code>Float.NaN</code> if this
   *         DocValues instance does not contain any value
   */
  public float getAverageValue() {
    compute();
    return avgVal;
  }

}
