package org.apache.lucene.facet.index.attributes;

import java.util.Collection;
import java.util.Set;

import org.apache.lucene.util.Attribute;

import org.apache.lucene.facet.taxonomy.CategoryPath;

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
 * An attribute which contains for a certain category the {@link CategoryPath}
 * and additional properties.
 * 
 * @lucene.experimental
 */
public interface CategoryAttribute extends Attribute {

  /**
   * Set the content of this {@link CategoryAttribute} from another
   * {@link CategoryAttribute} object.
   * 
   * @param other
   *            The {@link CategoryAttribute} to take the content from.
   */
  public void set(CategoryAttribute other);

  /**
   * Sets the category path value of this attribute.
   * 
   * @param cp
   *            A category path. May not be null.
   */
  public void setCategoryPath(CategoryPath cp);

  /**
   * Returns the value of this attribute: a category path.
   * 
   * @return The category path last assigned to this attribute, or null if
   *         none has been assigned.
   */
  public CategoryPath getCategoryPath();

  /**
   * Add a property. The property can be later retrieved using
   * {@link #getProperty(Class)} with this property class .<br>
   * Adding multiple properties of the same class is forbidden.
   * 
   * @param property
   *            The property to add.
   * @throws UnsupportedOperationException
   *             When attempting to add a property of a class that was added
   *             before and merge is prohibited.
   */
  public void addProperty(CategoryProperty property)
      throws UnsupportedOperationException;

  /**
   * Get a property of a certain property class.
   * 
   * @param propertyClass
   *            The required property class.
   * @return The property of the given class, or null if no such property
   *         exists.
   */
  public CategoryProperty getProperty(
      Class<? extends CategoryProperty> propertyClass);

  /**
   * Get a property of one of given property classes.
   * 
   * @param propertyClasses
   *            The property classes.
   * @return A property matching one of the given classes, or null if no such
   *         property exists.
   */
  public CategoryProperty getProperty(
      Collection<Class<? extends CategoryProperty>> propertyClasses);

  /**
   * Get all the active property classes.
   * 
   * @return A set containing the active property classes, or {@code null} if
   *         there are no properties.
   */
  public Set<Class<? extends CategoryProperty>> getPropertyClasses();

  /**
   * Clone this {@link CategoryAttribute}.
   * 
   * @return A clone of this {@link CategoryAttribute}.
   */
  public CategoryAttribute clone();

  /**
   * Resets this attribute to its initial value: a null category path and no
   * properties.
   */
  public void clear();

  /**
   * Clear all properties.
   */
  public void clearProperties();

  /**
   * Remove an property of a certain property class.
   * 
   * @param propertyClass
   *            The required property class.
   */
  public void remove(Class<? extends CategoryProperty> propertyClass);
}
