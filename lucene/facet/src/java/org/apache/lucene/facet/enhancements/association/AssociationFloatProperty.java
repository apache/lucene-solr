package org.apache.lucene.facet.enhancements.association;

import org.apache.lucene.facet.index.attributes.CategoryProperty;

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
 * An {@link AssociationProperty} which treats the association as float - the
 * association bits are actually float bits, and thus merging two associations
 * is done by float summation.
 * 
 * @lucene.experimental
 */
public class AssociationFloatProperty extends AssociationProperty {

  /**
   * Constructor.
   * 
   * @param value
   *            The association value.
   */
  public AssociationFloatProperty(float value) {
    super(Float.floatToIntBits(value));
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof AssociationFloatProperty)) {
      return false;
    }
    AssociationFloatProperty o = (AssociationFloatProperty) other;
    return o.association == this.association;
  }

  @Override
  public int hashCode() {
    return "AssociationFloatProperty".hashCode() * 31 + (int) association;
  }

  public void merge(CategoryProperty other) {
    AssociationFloatProperty o = (AssociationFloatProperty) other;
    this.association = Float.floatToIntBits(Float
        .intBitsToFloat((int) this.association)
        + Float.intBitsToFloat((int) o.association));
  }

  public float getFloatAssociation() {
    return Float.intBitsToFloat((int) association);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ": " + Float.intBitsToFloat(getAssociation());
  }

}
