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
 * @author yonik
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

  private int nVals;
  
  /**
   * Constructor with input number of values(docs).
   * @param nVals
   */
  public DocValues (int nVals) {
    this.nVals = nVals;
  }
  
  // prevent using this constructor
  private DocValues () {
    
  }
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
   * Return a string representation of a doc value, as reuired for Explanations.
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
   * Note: Tested implementations of DocValues must override this method for the test to pass!
   */
  Object getInnerArray() {
    return new Object[0];
  }

  // --- some simple statistics on values
  private float minVal;
  private float maxVal;
  private float avgVal;
  private boolean computed=false;
  // compute optional values
  private void compute () {
    if (computed) {
      return;
    }
    minVal = Float.MAX_VALUE;
    maxVal = 0;
    float sum = 0;
    for (int i=0; i<nVals; i++) {
      float val = floatVal(i); 
      sum += val;
      minVal = Math.min(minVal,val);
      maxVal = Math.max(maxVal,val);
    }
    avgVal = sum / nVals;
    computed = true;
  }
  /**
   * Optional op.
   * Returns the minimum of all values.
   */
  public float getMinValue () {
    compute();
    return minVal;
  }
  
  /**
   * Optional op.
   * Returns the maximum of all values. 
   */
  public float getMaxValue () {
    compute();
    return maxVal;
  }
  
  /**
   * Returns the average of all values. 
   */
  public float getAverageValue () {
    compute();
    return avgVal;
  }

}
