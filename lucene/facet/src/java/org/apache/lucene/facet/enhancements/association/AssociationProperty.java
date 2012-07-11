package org.apache.lucene.facet.enhancements.association;

import org.apache.lucene.facet.index.attributes.CategoryAttribute;
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
 * A {@link CategoryProperty} associating a single integer value to a
 * {@link CategoryAttribute}. It should be used to describe the association
 * between the category and the document.
 * <p>
 * This class leave to extending classes the definition of
 * {@link #merge(CategoryProperty)} policy for the integer associations.
 * <p>
 * <B>Note:</B> The association value is added both to a special category list,
 * and to the category tokens.
 * 
 * @see AssociationEnhancement
 * @lucene.experimental
 */
public abstract class AssociationProperty implements CategoryProperty {

  protected long association = Integer.MAX_VALUE + 1;

  /**
   * Construct an {@link AssociationProperty}.
   * 
   * @param value
   *            The association value.
   */
  public AssociationProperty(int value) {
    this.association = value;
  }

  /**
   * Returns the association value.
   * 
   * @return The association value.
   */
  public int getAssociation() {
    return (int) association;
  }

  /**
   * Returns whether this attribute has been set (not all categories have an
   * association).
   */
  public boolean hasBeenSet() {
    return this.association <= Integer.MAX_VALUE;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ": " + association;
  }

}
