package org.apache.lucene.facet.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

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
 * A utility class for adding facet fields to a document. Usually one field will
 * be added for all facets, however per the
 * {@link FacetIndexingParams#getCategoryListParams(CategoryPath)}, one field
 * may be added for every group of facets.
 * 
 * @lucene.experimental
 */
public class FacetFields {

  // The drill-down field is added with a TokenStream, hence why it's based on
  // TextField type. However in practice, it is added just like StringField.
  // Therefore we set its IndexOptions to DOCS_ONLY.
  private static final FieldType DRILL_DOWN_TYPE = new FieldType(TextField.TYPE_NOT_STORED);
  static {
    DRILL_DOWN_TYPE.setIndexOptions(IndexOptions.DOCS_ONLY);
    DRILL_DOWN_TYPE.setOmitNorms(true);
    DRILL_DOWN_TYPE.freeze();
  }
  
  protected final TaxonomyWriter taxonomyWriter;

  protected final FacetIndexingParams indexingParams;

  /**
   * Constructs a new instance with the {@link FacetIndexingParams#DEFAULT
   * default} facet indexing params.
   * 
   * @param taxonomyWriter
   *          used to resolve given categories to ordinals
   */
  public FacetFields(TaxonomyWriter taxonomyWriter) {
    this(taxonomyWriter, FacetIndexingParams.DEFAULT);
  }

  /**
   * Constructs a new instance with the given facet indexing params.
   * 
   * @param taxonomyWriter
   *          used to resolve given categories to ordinals
   * @param params
   *          determines under which fields the categories should be indexed
   */
  public FacetFields(TaxonomyWriter taxonomyWriter, FacetIndexingParams params) {
    this.taxonomyWriter = taxonomyWriter;
    this.indexingParams = params;
  }

  /**
   * Creates a mapping between a {@link CategoryListParams} and all
   * {@link CategoryPath categories} that are associated with it.
   */
  protected Map<CategoryListParams,Iterable<CategoryPath>> createCategoryListMapping(
      Iterable<CategoryPath> categories) {
    if (indexingParams.getAllCategoryListParams().size() == 1) {
      return Collections.singletonMap(indexingParams.getCategoryListParams(null), categories);
    }
    HashMap<CategoryListParams,Iterable<CategoryPath>> categoryLists = 
        new HashMap<CategoryListParams,Iterable<CategoryPath>>();
    for (CategoryPath cp : categories) {
      // each category may be indexed under a different field, so add it to the right list.
      CategoryListParams clp = indexingParams.getCategoryListParams(cp);
      List<CategoryPath> list = (List<CategoryPath>) categoryLists.get(clp);
      if (list == null) {
        list = new ArrayList<CategoryPath>();
        categoryLists.put(clp, list);
      }
      list.add(cp);
    }
    return categoryLists;
  }
  
  /**
   * Returns the category list data, as a mapping from key to {@link BytesRef}
   * which includes the encoded data. Every ordinal in {@code ordinals}
   * corrspond to a {@link CategoryPath} returned from {@code categories}.
   */
  protected Map<String,BytesRef> getCategoryListData(CategoryListParams categoryListParams, 
      IntsRef ordinals, Iterable<CategoryPath> categories /* needed for AssociationsFacetFields */) 
      throws IOException {
    return new CountingListBuilder(categoryListParams, indexingParams, taxonomyWriter).build(ordinals, categories);
  }
  
  /**
   * Returns a {@link DrillDownStream} for writing the categories drill-down
   * terms.
   */
  protected DrillDownStream getDrillDownStream(Iterable<CategoryPath> categories) {
    return new DrillDownStream(categories, indexingParams);
  }
  
  /**
   * Returns the {@link FieldType} with which the drill-down terms should be
   * indexed. The default is {@link IndexOptions#DOCS_ONLY}.
   */
  protected FieldType drillDownFieldType() {
    return DRILL_DOWN_TYPE;
  }

  /**
   * Add the counting list data to the document under the given field. Note that
   * the field is determined by the {@link CategoryListParams}.
   */
  protected void addCountingListData(Document doc, Map<String,BytesRef> categoriesData, String field) {
    for (Entry<String,BytesRef> entry : categoriesData.entrySet()) {
      doc.add(new BinaryDocValuesField(field + entry.getKey(), entry.getValue()));
    }
  }
  
  /** Adds the needed facet fields to the document. */
  public void addFields(Document doc, Iterable<CategoryPath> categories) throws IOException {
    if (categories == null) {
      throw new IllegalArgumentException("categories should not be null");
    }

    // TODO: add reuse capabilities to this class, per CLP objects:
    // - drill-down field
    // - counting list field
    // - DrillDownStream
    // - CountingListStream

    final Map<CategoryListParams,Iterable<CategoryPath>> categoryLists = createCategoryListMapping(categories);

    // for each CLP we add a different field for drill-down terms as well as for
    // counting list data.
    IntsRef ordinals = new IntsRef(32); // should be enough for most common applications
    for (Entry<CategoryListParams, Iterable<CategoryPath>> e : categoryLists.entrySet()) {
      final CategoryListParams clp = e.getKey();
      final String field = clp.field;

      // build category list data
      ordinals.length = 0; // reset
      int maxNumOrds = 0;
      for (CategoryPath cp : e.getValue()) {
        int ordinal = taxonomyWriter.addCategory(cp);
        maxNumOrds += cp.length; // ordinal and potentially all parents
        if (ordinals.ints.length < maxNumOrds) {
          ordinals.grow(maxNumOrds);
        }
        ordinals.ints[ordinals.length++] = ordinal;
      }
      Map<String,BytesRef> categoriesData = getCategoryListData(clp, ordinals, e.getValue());
      
      // add the counting list data
      addCountingListData(doc, categoriesData, field);
      
      // add the drill-down field
      DrillDownStream drillDownStream = getDrillDownStream(e.getValue());
      Field drillDown = new Field(field, drillDownStream, drillDownFieldType());
      doc.add(drillDown);
    }
  }

}
