package org.apache.lucene.facet.search;

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
 * Resolves an ordinal's value to given the {@link FacetArrays}.
 * Implementations of this class are encouraged to initialize the needed array
 * from {@link FacetArrays} in the constructor.
 */
public abstract class OrdinalValueResolver {

  /**
   * An {@link OrdinalValueResolver} which resolves ordinals value from
   * {@link FacetArrays#getIntArray()}, by returning the value in the array.
   */
  public static final class IntValueResolver extends OrdinalValueResolver {

    private final int[] values;
    
    public IntValueResolver(FacetArrays arrays) {
      super(arrays);
      this.values = arrays.getIntArray();
    }

    @Override
    public final double valueOf(int ordinal) {
      return values[ordinal];
    }
    
  }
  
  /**
   * An {@link OrdinalValueResolver} which resolves ordinals value from
   * {@link FacetArrays#getFloatArray()}, by returning the value in the array.
   */
  public static final class FloatValueResolver extends OrdinalValueResolver {
    
    private final float[] values;
    
    public FloatValueResolver(FacetArrays arrays) {
      super(arrays);
      this.values = arrays.getFloatArray();
    }
    
    @Override
    public final double valueOf(int ordinal) {
      return values[ordinal];
    }
    
  }
  
  protected final FacetArrays arrays;
  
  protected OrdinalValueResolver(FacetArrays arrays) {
    this.arrays = arrays;
  }

  /** Returns the value of the given ordinal. */
  public abstract double valueOf(int ordinal);
  
}
