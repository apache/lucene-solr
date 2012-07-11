package org.apache.lucene.facet.enhancements.association;

import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;

import org.apache.lucene.facet.enhancements.CategoryEnhancement;
import org.apache.lucene.facet.enhancements.params.EnhancementsIndexingParams;
import org.apache.lucene.facet.index.attributes.CategoryAttribute;
import org.apache.lucene.facet.index.attributes.CategoryProperty;
import org.apache.lucene.facet.index.streaming.CategoryListTokenizer;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.util.Vint8;
import org.apache.lucene.util.Vint8.Position;

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
 * A {@link CategoryEnhancement} for adding associations data to the index
 * (categories with {@link AssociationProperty}s).
 * 
 * @lucene.experimental
 */
public class AssociationEnhancement implements CategoryEnhancement {

  static final String CATEGORY_LIST_TERM_TEXT = "CATEGORY_ASSOCIATION_LIST";

  /** Property Classes which extend AssociationProperty */
  private static final HashSet<Class<? extends CategoryProperty>> ASSOCIATION_PROPERTY_CLASSES;

  /** Property Classes which do not extend AssociationProperty */
  private static final HashSet<Class<? extends CategoryProperty>> NON_ASSOCIATION_PROPERTY_CLASSES;
  
  static {
    ASSOCIATION_PROPERTY_CLASSES = new HashSet<Class<? extends CategoryProperty>>();
    NON_ASSOCIATION_PROPERTY_CLASSES = new HashSet<Class<? extends CategoryProperty>>();
  }

  /**
   * For a given class which extends a CategoryProperty, answers whether it is
   * an instance of AssociationProperty (AP) or not. <br>
   * This method is a cheaper replacement for a call to
   * <code>instanceof</code>. It has two HashSets - one for classes which are
   * an extension to AP and one for the classes which are not. Whenever a
   * property class is introduced:
   * <ul>
   * <li>if it is known as a property class extending AP (contained in the
   * validHashSet)- returns true</li>
   * <li>if it is known as a property class NOT extending AP - returns false</li>
   * <li>
   * If it was not matched against both sets, it calls 'instanceof' to find
   * out if it extends AP, puts it in the matching Set and returning true or
   * false accordingly</li>
   *</ul>
   * 
   * NOTE: 'instanceof' is only called once per a Class (not instance) of a
   * property. And as there are few properties (currently 4 concrete
   * implementations) the two sets would be rather small
   */
  public static boolean isAssociationProperty(Class<? extends CategoryProperty> clazz) {
    if (ASSOCIATION_PROPERTY_CLASSES.contains(clazz)) {
      return true;
    }
    
    if (NON_ASSOCIATION_PROPERTY_CLASSES.contains(clazz)) {
      return false;
    }
    
    if (AssociationProperty.class.isAssignableFrom(clazz)) {
      ASSOCIATION_PROPERTY_CLASSES.add(clazz);
      return true;
    }
    
    NON_ASSOCIATION_PROPERTY_CLASSES.add(clazz);
    return false;
  }
  
  public boolean generatesCategoryList() {
    return true;
  }

  public String getCategoryListTermText() {
    return CATEGORY_LIST_TERM_TEXT;
  }

  public CategoryListTokenizer getCategoryListTokenizer(
      TokenStream tokenizer, EnhancementsIndexingParams indexingParams,
      TaxonomyWriter taxonomyWriter) {
    return new AssociationListTokenizer(tokenizer, indexingParams, this);
  }

  public byte[] getCategoryTokenBytes(CategoryAttribute categoryAttribute) {
    
    AssociationProperty property = getAssociationProperty(categoryAttribute);
    
    if (property == null) {
      return null;
    }
    
    int association = property.getAssociation();
    int bytesNeeded = Vint8.bytesNeeded(association);
    byte[] buffer = new byte[bytesNeeded];
    Vint8.encode(association, buffer, 0);
    return buffer;
  }

  public static AssociationProperty getAssociationProperty(
      CategoryAttribute categoryAttribute) {
    AssociationProperty property = null;
    Set<Class<? extends CategoryProperty>> propertyClasses = categoryAttribute
        .getPropertyClasses();
    if (propertyClasses == null) {
      return null;
    }
    for (Class<? extends CategoryProperty> clazz : propertyClasses) {
      if (isAssociationProperty(clazz)) {
        property = (AssociationProperty) categoryAttribute
            .getProperty(clazz);
        break;
      }
    }
    return property;
  }

  public Object extractCategoryTokenData(byte[] buffer, int offset, int length) {
    if (length == 0) {
      return null;
    }
    Integer i = Integer.valueOf(Vint8.decode(buffer, new Position(offset)));
    return i;
  }

  public Class<? extends CategoryProperty> getRetainableProperty() {
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    return (o instanceof AssociationEnhancement);
  }
  
  @Override
  public int hashCode() {
    return super.hashCode();
  }
  
}
