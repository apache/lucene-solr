package org.apache.lucene.facet.associations;

import java.io.IOException;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;

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

/** A {@link CategoryAssociation} that associates a float with a category. */
public class CategoryFloatAssociation implements CategoryAssociation {
  
  public static final String ASSOCIATION_LIST_ID = "$assoc_float$";
  
  private float value;
  
  public CategoryFloatAssociation() {
    // used for deserialization
  }
  
  public CategoryFloatAssociation(float value) {
    this.value = value;
  }
  
  @Override
  public void serialize(ByteArrayDataOutput output) {
    try {
      output.writeInt(Float.floatToIntBits(value));
    } catch (IOException e) {
      throw new RuntimeException("unexpected exception writing to a byte[]", e);
    }
  }
  
  @Override
  public void deserialize(ByteArrayDataInput input) {
    value = Float.intBitsToFloat(input.readInt());
  }
  
  @Override
  public int maxBytesNeeded() {
    // plain integer
    return 4;
  }
  
  @Override
  public String getCategoryListID() {
    return ASSOCIATION_LIST_ID;
  }
  
  /**
   * Returns the value associated with a category. If you used
   * {@link #CategoryFloatAssociation()}, you should call
   * {@link #deserialize(ByteArrayDataInput)} before calling this method, or
   * otherwise the value returned is undefined.
   */
  public float getValue() {
    return value;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + value + ")";
  }
  
}
