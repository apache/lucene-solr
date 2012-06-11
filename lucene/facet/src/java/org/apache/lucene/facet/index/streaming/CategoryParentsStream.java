package org.apache.lucene.facet.index.streaming;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.TokenFilter;

import org.apache.lucene.facet.index.attributes.CategoryAttribute;
import org.apache.lucene.facet.index.attributes.CategoryProperty;
import org.apache.lucene.facet.index.attributes.OrdinalProperty;
import org.apache.lucene.facet.index.categorypolicy.OrdinalPolicy;
import org.apache.lucene.facet.index.categorypolicy.PathPolicy;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;

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
 * This class adds parents to a {@link CategoryAttributesStream}. The parents
 * are added according to the {@link PathPolicy} and {@link OrdinalPolicy} from
 * the {@link FacetIndexingParams} given in the constructor.<br>
 * By default, category properties are removed when creating parents of a
 * certain category. However, it is possible to retain certain property types
 * using {@link #addRetainableProperty(Class)}.
 * 
 * @lucene.experimental
 */
public class CategoryParentsStream extends TokenFilter {

  /**
   * A {@link TaxonomyWriter} for adding categories and retrieving their
   * ordinals.
   */
  protected TaxonomyWriter taxonomyWriter;

  /** An attribute containing all data related to the category */
  protected CategoryAttribute categoryAttribute;

  /** A category property containing the category ordinal */
  protected OrdinalProperty ordinalProperty;

  /**
   * A set of property classes that are to be retained when creating a parent
   * token.
   */
  private Set<Class<? extends CategoryProperty>> retainableProperties;

  /** A {@link PathPolicy} for the category's parents' category paths. */
  private PathPolicy pathPolicy;

  /** An {@link OrdinalPolicy} for the category's parents' ordinals. */
  private OrdinalPolicy ordinalPolicy;

  /**
   * Constructor.
   * 
   * @param input
   *            The input stream to handle, must be derived from
   *            {@link CategoryAttributesStream}.
   * @param taxonomyWriter
   *            The taxonomy writer to use for adding categories and
   *            retrieving their ordinals.
   * @param indexingParams
   *            The indexing params used for filtering parents.
   */
  public CategoryParentsStream(CategoryAttributesStream input,
      TaxonomyWriter taxonomyWriter, FacetIndexingParams indexingParams) {
    super(input);
    this.categoryAttribute = this.addAttribute(CategoryAttribute.class);
    this.taxonomyWriter = taxonomyWriter;
    this.pathPolicy = indexingParams.getPathPolicy();
    this.ordinalPolicy = indexingParams.getOrdinalPolicy();
    this.ordinalPolicy.init(taxonomyWriter);
    this.ordinalProperty = new OrdinalProperty();
    
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (this.categoryAttribute.getCategoryPath() != null) {
      // try adding the parent of the current category to the stream
      clearCategoryProperties();
      boolean added = false;
      // set the parent's ordinal, if illegal set -1
      int ordinal = this.ordinalProperty.getOrdinal();
      if (ordinal != -1) {
        ordinal = this.taxonomyWriter.getParent(ordinal);
        if (this.ordinalPolicy.shouldAdd(ordinal)) {
          this.ordinalProperty.setOrdinal(ordinal);
          try {
            this.categoryAttribute.addProperty(ordinalProperty);
          } catch (UnsupportedOperationException e) {
            throw new IOException(e.getLocalizedMessage());
          }
          added = true;
        } else {
          this.ordinalProperty.setOrdinal(-1);
        }
      }
      // set the parent's category path, if illegal set null
      CategoryPath cp = this.categoryAttribute.getCategoryPath();
      if (cp != null) {
        cp.trim(1);
        // if ordinal added, must also have category paths
        if (added || this.pathPolicy.shouldAdd(cp)) {
          this.categoryAttribute.setCategoryPath(cp);
          added = true;
        } else {
          this.categoryAttribute.clear();
        }
      }
      if (added) {
        // a legal parent exists
        return true;
      }
    }
    // no more parents - get new category
    if (input.incrementToken()) {
      int ordinal = taxonomyWriter.addCategory(this.categoryAttribute.getCategoryPath());
      this.ordinalProperty.setOrdinal(ordinal);
      try {
        this.categoryAttribute.addProperty(this.ordinalProperty);
      } catch (UnsupportedOperationException e) {
        throw new IOException(e.getLocalizedMessage());
      }
      return true;
    }
    return false;
  }

  /**
   * Clear the properties of the current {@link CategoryAttribute} attribute
   * before setting the parent attributes. <br>
   * It is possible to retain properties of certain types the parent tokens,
   * using {@link #addRetainableProperty(Class)}.
   */
  protected void clearCategoryProperties() {
    if (this.retainableProperties == null
        || this.retainableProperties.isEmpty()) {
      this.categoryAttribute.clearProperties();
    } else {
      List<Class<? extends CategoryProperty>> propertyClassesToRemove = 
                            new LinkedList<Class<? extends CategoryProperty>>();
      for (Class<? extends CategoryProperty> propertyClass : this.categoryAttribute
          .getPropertyClasses()) {
        if (!this.retainableProperties.contains(propertyClass)) {
          propertyClassesToRemove.add(propertyClass);
        }
      }
      for (Class<? extends CategoryProperty> propertyClass : propertyClassesToRemove) {
        this.categoryAttribute.remove(propertyClass);
      }
    }
  }

  /**
   * Add a {@link CategoryProperty} class which is retained when creating
   * parent tokens.
   * 
   * @param toRetain
   *            The property class to retain.
   */
  public void addRetainableProperty(Class<? extends CategoryProperty> toRetain) {
    if (this.retainableProperties == null) {
      this.retainableProperties = new HashSet<Class<? extends CategoryProperty>>();
    }
    this.retainableProperties.add(toRetain);
  }

}
