package org.apache.lucene.facet.index;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.util.Attribute;

import org.apache.lucene.facet.index.attributes.CategoryAttribute;
import org.apache.lucene.facet.index.attributes.CategoryAttributeImpl;
import org.apache.lucene.facet.index.attributes.CategoryProperty;
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
 * A container to add categories which are to be introduced to
 * {@link CategoryDocumentBuilder#setCategories(Iterable)}. Categories can be
 * added with Properties. 
 * 
 * @lucene.experimental
 */
public class CategoryContainer implements Iterable<CategoryAttribute>, Serializable {

  protected transient Map<CategoryPath, CategoryAttribute> map;

  /**
   * Constructor.
   */
  public CategoryContainer() {
    map = new HashMap<CategoryPath, CategoryAttribute>();
  }

  /**
   * Add a category.
   * 
   * @param categoryPath
   *            The path of the category.
   * @return The {@link CategoryAttribute} of the category.
   */
  public CategoryAttribute addCategory(CategoryPath categoryPath) {
    return mapCategoryAttribute(categoryPath);
  }

  /**
   * Add a category with a property.
   * 
   * @param categoryPath
   *            The path of the category.
   * @param property
   *            The property to associate to the category.
   * @return The {@link CategoryAttribute} of the category.
   */
  public CategoryAttribute addCategory(CategoryPath categoryPath,
      CategoryProperty property) {
    /*
     * This method is a special case of addCategory with multiple
     * properties, but it is kept here for two reasons: 1) Using the array
     * version has some performance cost, and 2) it is expected that most
     * calls will be for this version (single property).
     */
    CategoryAttribute ca = mapCategoryAttribute(categoryPath);
    ca.addProperty(property);
    return ca;
  }

  /**
   * Add a category with multiple properties.
   * 
   * @param categoryPath
   *            The path of the category.
   * @param properties
   *            The properties to associate to the category.
   * @return The {@link CategoryAttribute} of the category.
   */
  public CategoryAttribute addCategory(CategoryPath categoryPath,
      CategoryProperty... properties) {
    CategoryAttribute ca = mapCategoryAttribute(categoryPath);
    for (CategoryProperty attribute : properties) {
      ca.addProperty(attribute);
    }
    return ca;
  }

  /**
   * Add an entire {@link CategoryAttribute}.
   * 
   * @param categoryAttribute
   *            The {@link CategoryAttribute} to add.
   * @return The {@link CategoryAttribute} of the category (could be different
   *         from the one provided).
   */
  public CategoryAttribute addCategory(CategoryAttribute categoryAttribute) {
    CategoryAttribute ca = mapCategoryAttribute(categoryAttribute
        .getCategoryPath());
    Set<Class<? extends CategoryProperty>> propertyClasses = categoryAttribute
    .getPropertyClasses();
    if (propertyClasses != null) {
      for (Class<? extends CategoryProperty> propertyClass : propertyClasses) {
        ca.addProperty(categoryAttribute.getProperty(propertyClass));
      }
    }
    return ca;
  }

  /**
   * Get the {@link CategoryAttribute} object for a specific
   * {@link CategoryPath}, from the map.
   */
  private final CategoryAttribute mapCategoryAttribute(
      CategoryPath categoryPath) {
    CategoryAttribute ca = map.get(categoryPath);
    if (ca == null) {
      ca = new CategoryAttributeImpl(categoryPath);
      map.put(categoryPath, ca);
    }
    return ca;
  }

  /**
   * Get the {@link CategoryAttribute} this container has for a certain
   * category, or {@code null} if the category is not in the container.
   * 
   * @param categoryPath
   *            The category path of the requested category.
   */
  public CategoryAttribute getCategoryAttribute(CategoryPath categoryPath) {
    return map.get(categoryPath);
  }

  public Iterator<CategoryAttribute> iterator() {
    return map.values().iterator();
  }

  /**
   * Remove all categories.
   */
  public void clear() {
    map.clear();
  }

  /** Add the categories from another {@link CategoryContainer} to this one. */
  public void merge(CategoryContainer other) {
    for (CategoryAttribute categoryAttribute : other.map.values()) {
      addCategory(categoryAttribute);
    }
  }

  /**
   * Get the number of categories in the container.
   * 
   * @return The number of categories in the container.
   */
  public int size() {
    return map.size();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("CategoryContainer");
    for (CategoryAttribute ca : map.values()) {
      builder.append('\n');
      builder.append('\t');
      builder.append(ca.toString());
    }
    return builder.toString();
  }
  
  /**
   * Serialize object content to given {@link ObjectOutputStream}
   */
  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    // write the number of categories
    out.writeInt(size());
    // write the category attributes
    for (CategoryAttribute ca : this) {
      serializeCategoryAttribute(out, ca);
    }
  }

  /**
   * Serialize each of the {@link CategoryAttribute}s to the given
   * {@link ObjectOutputStream}.<br>
   * NOTE: {@link CategoryProperty}s are {@link Serializable}, but do not
   * assume that Lucene's {@link Attribute}s are as well
   * @throws IOException 
   */
  protected void serializeCategoryAttribute(ObjectOutputStream out,
      CategoryAttribute ca) throws IOException {
    out.writeObject(ca.getCategoryPath());
    Set<Class<? extends CategoryProperty>> propertyClasses = ca.getPropertyClasses();
    if (propertyClasses != null) {
      out.writeInt(propertyClasses.size());
      for (Class<? extends CategoryProperty> clazz : propertyClasses) {
        out.writeObject(ca.getProperty(clazz));
      }
    } else {
      out.writeInt(0);
    }
  }
  
  /**
   * Deserialize object from given {@link ObjectInputStream}
   */
  private void readObject(ObjectInputStream in) throws IOException,
      ClassNotFoundException {
    in.defaultReadObject();
    map = new HashMap<CategoryPath, CategoryAttribute>();
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      deserializeCategoryAttribute(in);
    }
  }

  /**
   * De-Serialize each of the {@link CategoryAttribute}s from the given
   * {@link ObjectInputStream}.
   */
  protected void deserializeCategoryAttribute(ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    CategoryPath cp = (CategoryPath) in.readObject();
    int nProperties = in.readInt();
    if (nProperties == 0) {
      addCategory(cp);
    } else {
      for (int j = 0; j < nProperties; j++) {
        CategoryProperty property = (CategoryProperty) in.readObject();
        addCategory(cp, property);
      }
    }
  }
  
  @Override
  public boolean equals(Object o) {
    if (! (o instanceof CategoryContainer)) {
      return false;
    }
    
    CategoryContainer that = (CategoryContainer)o;
    return this.map.equals(that.map);
  }
  
  @Override
  public int hashCode() {
    return map.hashCode();
  }
}
