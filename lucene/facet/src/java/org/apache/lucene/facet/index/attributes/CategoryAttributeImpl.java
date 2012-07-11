package org.apache.lucene.facet.index.attributes;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

import org.apache.lucene.util.AttributeImpl;

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
 * An implementation of {@link CategoryAttribute}.
 * 
 * @lucene.experimental
 */
public final class CategoryAttributeImpl extends AttributeImpl implements
    CategoryAttribute {

  /**
   * The category path instance.
   */
  protected CategoryPath categoryPath;

  /**
   * A map of properties associated to the current category path.
   */
  protected HashMap<Class<? extends CategoryProperty>, CategoryProperty> properties;

  /**
   * Construct an empty CategoryAttributeImpl.
   */
  public CategoryAttributeImpl() {
    // do nothing
  }

  /**
   * Construct a CategoryAttributeImpl with the given CategoryPath.
   * 
   * @param categoryPath
   *            The category path to use.
   */
  public CategoryAttributeImpl(CategoryPath categoryPath) {
    setCategoryPath(categoryPath);
  }

  public void set(CategoryAttribute other) {
    ((CategoryAttributeImpl) other).copyTo(this);
  }

  /**
   * Returns the category path value.
   * 
   * @return The category path last assigned to this attribute, or null if
   *         none has been assigned.
   */
  public CategoryPath getCategoryPath() {
    return categoryPath;
  }

  public void setCategoryPath(CategoryPath cp) {
    categoryPath = cp;
  }

  public void addProperty(CategoryProperty property)
      throws UnsupportedOperationException {
    if (properties == null) {
      properties = new HashMap<Class<? extends CategoryProperty>, CategoryProperty>();
    }
    CategoryProperty existing = properties.get(property.getClass());
    if (existing == null) {
      properties.put(property.getClass(), property);
    } else {
      existing.merge(property);
    }
  }

  public CategoryProperty getProperty(
      Class<? extends CategoryProperty> propertyClass) {
    if (properties == null) {
      return null;
    }
    return properties.get(propertyClass);
  }

  public CategoryProperty getProperty(
      Collection<Class<? extends CategoryProperty>> propertyClasses) {
    if (properties == null) {
      return null;
    }
    for (Class<? extends CategoryProperty> propertyClass : propertyClasses) {
      CategoryProperty categoryProperty = properties.get(propertyClass);
      if (categoryProperty != null) {
        return categoryProperty;
      }
    }
    return null;
  }

  @Override
  public void copyTo(AttributeImpl target) {
    ((CategoryAttributeImpl) target).categoryPath = this.categoryPath;
    ((CategoryAttributeImpl) target).properties = this.properties;
  }

  @SuppressWarnings("unchecked")
  @Override
  public CategoryAttributeImpl clone() {
    CategoryAttributeImpl ca = (CategoryAttributeImpl) super.clone();
    if (categoryPath != null) {
      ca.categoryPath = categoryPath.clone();
    }
    if (properties != null && !properties.isEmpty()) {
      ca.properties = (HashMap<Class<? extends CategoryProperty>, CategoryProperty>) properties
          .clone();
    }
    return ca;
  }

  @Override
  public void clear() {
    categoryPath = null;
    clearProperties();
  }

  public void clearProperties() {
    if (properties != null) {
      properties.clear();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof CategoryAttributeImpl)) {
      return false;
    }
    CategoryAttributeImpl other = (CategoryAttributeImpl) o;
    if (categoryPath == null) {
      return (other.categoryPath == null);
    }
    if (!categoryPath.equals(other.categoryPath)) {
      return false;
    }
    if (properties == null || properties.isEmpty()) {
      return (other.properties == null || other.properties.isEmpty());
    }
    return properties.equals(other.properties);
  }

  @Override
  public int hashCode() {
    if (categoryPath == null) {
      return 0;
    }
    int hashCode = categoryPath.hashCode();
    if (properties != null && !properties.isEmpty()) {
      hashCode ^= properties.hashCode();
    }
    return hashCode;
  }

  public Set<Class<? extends CategoryProperty>> getPropertyClasses() {
    if (properties == null || properties.isEmpty()) {
      return null;
    }
    return properties.keySet();
  }

  public void remove(Class<? extends CategoryProperty> propertyClass) {
    properties.remove(propertyClass);
  }

}
