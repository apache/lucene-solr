package org.apache.lucene.facet.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;

import org.apache.lucene.facet.index.attributes.CategoryAttribute;
import org.apache.lucene.facet.index.attributes.CategoryAttributesIterable;
import org.apache.lucene.facet.index.categorypolicy.OrdinalPolicy;
import org.apache.lucene.facet.index.categorypolicy.PathPolicy;
import org.apache.lucene.facet.index.params.DefaultFacetIndexingParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.index.streaming.CategoryAttributesStream;
import org.apache.lucene.facet.index.streaming.CategoryListTokenizer;
import org.apache.lucene.facet.index.streaming.CategoryParentsStream;
import org.apache.lucene.facet.index.streaming.CategoryTokenizer;
import org.apache.lucene.facet.index.streaming.CountingListTokenizer;
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
 * A utility class which allows attachment of {@link CategoryPath}s or
 * {@link CategoryAttribute}s to a given document using a taxonomy.<br>
 * Construction could be done with either a given {@link FacetIndexingParams} or
 * the default implementation {@link DefaultFacetIndexingParams}.<br>
 * A CategoryDocumentBuilder can be reused by repeatedly setting the categories
 * and building the document. Categories are provided either as
 * {@link CategoryAttribute} elements through {@link #setCategories(Iterable)},
 * or as {@link CategoryPath} elements through
 * {@link #setCategoryPaths(Iterable)}.
 * <p>
 * Note that both {@link #setCategories(Iterable)} and
 * {@link #setCategoryPaths(Iterable)} return this
 * {@link CategoryDocumentBuilder}, allowing the following pattern: {@code new
 * CategoryDocumentBuilder(taxonomy,
 * params).setCategories(categories).build(doc)}.
 * 
 * @lucene.experimental
 */
public class CategoryDocumentBuilder {

  /**
   * A {@link TaxonomyWriter} for adding categories and retrieving their
   * ordinals.
   */
  protected final TaxonomyWriter taxonomyWriter;

  /**
   * Parameters to be used when indexing categories.
   */
  protected final FacetIndexingParams indexingParams;

  /**
   * A list of fields which is filled at ancestors' construction and used
   * during {@link CategoryDocumentBuilder#build(Document)}.
   */
  protected final ArrayList<Field> fieldList = new ArrayList<Field>();

  protected Map<String, List<CategoryAttribute>> categoriesMap;

  /**
   * Creating a facets document builder with default facet indexing
   * parameters.<br>
   * See:
   * {@link #CategoryDocumentBuilder(TaxonomyWriter, FacetIndexingParams)}
   * 
   * @param taxonomyWriter
   *            to which new categories will be added, as well as translating
   *            known categories to ordinals
   *
   */
  public CategoryDocumentBuilder(TaxonomyWriter taxonomyWriter) {
    this(taxonomyWriter, new DefaultFacetIndexingParams());
  }

  /**
   * Creating a facets document builder with a given facet indexing parameters
   * object.<br>
   * 
   * @param taxonomyWriter
   *            to which new categories will be added, as well as translating
   *            known categories to ordinals
   * @param params
   *            holds all parameters the indexing process should use such as
   *            category-list parameters
   */
  public CategoryDocumentBuilder(TaxonomyWriter taxonomyWriter,
      FacetIndexingParams params) {
    this.taxonomyWriter = taxonomyWriter;
    this.indexingParams = params;
    this.categoriesMap = new HashMap<String, List<CategoryAttribute>>();
  }

  /**
   * Set the categories of the document builder from an {@link Iterable} of
   * {@link CategoryPath} objects.
   * 
   * @param categoryPaths
   *            An iterable of CategoryPath objects which holds the categories
   *            (facets) which will be added to the document at
   *            {@link #build(Document)}
   * @return This CategoryDocumentBuilder, to enable this one line call:
   *         {@code new} {@link #CategoryDocumentBuilder(TaxonomyWriter)}.
   *         {@link #setCategoryPaths(Iterable)}.{@link #build(Document)}.
   * @throws IOException If there is a low-level I/O error.
   */
  public CategoryDocumentBuilder setCategoryPaths(
      Iterable<CategoryPath> categoryPaths) throws IOException {
    if (categoryPaths == null) {
      fieldList.clear();
      return this;
    }
    return setCategories(new CategoryAttributesIterable(categoryPaths));
  }

  /**
   * Set the categories of the document builder from an {@link Iterable} of
   * {@link CategoryAttribute} objects.
   * 
   * @param categories
   *            An iterable of {@link CategoryAttribute} objects which holds
   *            the categories (facets) which will be added to the document at
   *            {@link #build(Document)}
   * @return This CategoryDocumentBuilder, to enable this one line call:
   *         {@code new} {@link #CategoryDocumentBuilder(TaxonomyWriter)}.
   *         {@link #setCategories(Iterable)}.{@link #build(Document)}.
   * @throws IOException If there is a low-level I/O error.
   */
  public CategoryDocumentBuilder setCategories(
      Iterable<CategoryAttribute> categories) throws IOException {
    fieldList.clear();
    if (categories == null) {
      return this;
    }

    // get field-name to a list of facets mapping as different facets could
    // be added to different category-lists on different fields
    fillCategoriesMap(categories);

    // creates a different stream for each different field
    for (Entry<String, List<CategoryAttribute>> e : categoriesMap
        .entrySet()) {
      // create a category attributes stream for the array of facets
      CategoryAttributesStream categoryAttributesStream = new CategoryAttributesStream(
          e.getValue());

      // Set a suitable {@link TokenStream} using
      // CategoryParentsStream, followed by CategoryListTokenizer and
      // CategoryTokenizer composition (the ordering of the last two is
      // not mandatory).
      CategoryParentsStream parentsStream = (CategoryParentsStream) getParentsStream(categoryAttributesStream);
      CategoryListTokenizer categoryListTokenizer = getCategoryListTokenizer(parentsStream);
      CategoryTokenizer stream = getCategoryTokenizer(categoryListTokenizer);

      // Finally creating a suitable field with stream and adding it to a
      // master field-list, used during the build process (see
      // super.build())
      FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
      ft.setOmitNorms(true);
      fieldList.add(new Field(e.getKey(), stream, ft));
    }

    return this;
  }

  /**
   * Get a stream of categories which includes the parents, according to
   * policies defined in indexing parameters.
   * 
   * @param categoryAttributesStream
   *            The input stream
   * @return The parents stream.
   * @see OrdinalPolicy OrdinalPolicy (for policy of adding category tokens for parents)
   * @see PathPolicy PathPolicy (for policy of adding category <b>list</b> tokens for parents)
   */
  protected TokenStream getParentsStream(
      CategoryAttributesStream categoryAttributesStream) {
    return new CategoryParentsStream(categoryAttributesStream,
        taxonomyWriter, indexingParams);
  }

  /**
   * Fills the categories mapping between a field name and a list of
   * categories that belongs to it according to this builder's
   * {@link FacetIndexingParams} object
   * 
   * @param categories
   *            Iterable over the category attributes
   */
  protected void fillCategoriesMap(Iterable<CategoryAttribute> categories)
      throws IOException {
    categoriesMap.clear();

    // for-each category
    for (CategoryAttribute category : categories) {
      // extracting the field-name to which this category belongs
      String fieldName = indexingParams.getCategoryListParams(
          category.getCategoryPath()).getTerm().field();

      // getting the list of categories which belongs to that field
      List<CategoryAttribute> list = categoriesMap.get(fieldName);

      // if no such list exists
      if (list == null) {
        // adding a new one to the map
        list = new ArrayList<CategoryAttribute>();
        categoriesMap.put(fieldName, list);
      }

      // adding the new category to the list
      list.add(category.clone());
    }
  }

  /**
   * Get a category list tokenizer (or a series of such tokenizers) to create
   * the <b>category list tokens</b>.
   * 
   * @param categoryStream
   *            A stream containing {@link CategoryAttribute} with the
   *            relevant data.
   * @return The category list tokenizer (or series of tokenizers) to be used
   *         in creating category list tokens.
   */
  protected CategoryListTokenizer getCategoryListTokenizer(
      TokenStream categoryStream) {
    return getCountingListTokenizer(categoryStream);
  }

  /**
   * Get a {@link CountingListTokenizer} for creating counting list token.
   * 
   * @param categoryStream
   *            A stream containing {@link CategoryAttribute}s with the
   *            relevant data.
   * @return A counting list tokenizer to be used in creating counting list
   *         token.
   */
  protected CountingListTokenizer getCountingListTokenizer(
      TokenStream categoryStream) {
    return new CountingListTokenizer(categoryStream, indexingParams);
  }

  /**
   * Get a {@link CategoryTokenizer} to create the <b>category tokens</b>.
   * This method can be overridden for adding more attributes to the category
   * tokens.
   * 
   * @param categoryStream
   *            A stream containing {@link CategoryAttribute} with the
   *            relevant data.
   * @return The {@link CategoryTokenizer} to be used in creating category
   *         tokens.
   * @throws IOException If there is a low-level I/O error.
   */
  protected CategoryTokenizer getCategoryTokenizer(TokenStream categoryStream)
      throws IOException {
    return new CategoryTokenizer(categoryStream, indexingParams);
  }

  /** Adds the fields created in one of the "set" methods to the document */
  public Document build(Document doc) {
    for (Field f : fieldList) {
      doc.add(f);
    }
    return doc;
  }

}
