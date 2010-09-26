package org.apache.lucene.search.cache;

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

import org.apache.lucene.util.Bits;

public abstract class CachedArray 
{
  public Integer parserHashCode; // a flag to make sure you don't change what you are asking for in subsequent requests
  public int numDocs;
  public int numTerms;

  /**
   * NOTE: these Bits may have false positives for deleted documents.  That is,
   * Documents that are deleted may be marked as valid but the array value is not.
   */
  public Bits valid;

  public CachedArray() {
    this.parserHashCode = null;
    this.numDocs = 0;
    this.numTerms = 0;
  }
  
  /**
   * @return the native array
   */
  public abstract Object getRawArray();

  //-------------------------------------------------------------
  // Concrete Values
  //-------------------------------------------------------------

  public static class ByteValues extends CachedArray {
    public byte[] values = null;
    @Override public byte[] getRawArray() { return values; }
  };

  public static class ShortValues extends CachedArray {
    public short[] values = null;
    @Override public short[] getRawArray() { return values; }
  };

  public static class IntValues extends CachedArray {
    public int[] values = null;
    @Override public int[] getRawArray() { return values; }
  };

  public static class FloatValues extends CachedArray {
    public float[] values = null;
    @Override public float[] getRawArray() { return values; }
  };

  public static class LongValues extends CachedArray {
    public long[] values = null;
    @Override public long[] getRawArray() { return values; }
  };

  public static class DoubleValues extends CachedArray {
    public double[] values = null;
    @Override public double[] getRawArray() { return values; }
  };
}
