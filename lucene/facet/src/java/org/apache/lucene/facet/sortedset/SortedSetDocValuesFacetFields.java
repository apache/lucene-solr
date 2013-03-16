package org.apache.lucene.facet.sortedset;

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

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.facet.index.DrillDownStream;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.util.BytesRef;

/** Add instances of this to your Document if you intend to
 *  use {@link SortedSetDocValuesAccumulator} to count facets
 *  at search time.  Note that this only supports flat
 *  facets (dimension + label).  Add multiple instances of
 *  this to your document, one per dimension + label, and
 *  it's fine if a given dimension is multi-valued. */

public class SortedSetDocValuesFacetFields extends FacetFields {

  /** Create a {@code SortedSetDocValuesFacetField} with the
   *  provided {@link CategoryPath}. */
  public SortedSetDocValuesFacetFields()  {
    this(FacetIndexingParams.DEFAULT);
  }

  /** Create a {@code SortedSetDocValuesFacetField} with the
   *  provided {@link CategoryPath}, and custom {@link
   *  FacetIndexingParams}. */
  public SortedSetDocValuesFacetFields(FacetIndexingParams fip)  {
    super(null, fip);
    if (fip.getPartitionSize() != Integer.MAX_VALUE) {
      throw new IllegalArgumentException("partitions are not supported");
    }
  }

  @Override
  public void addFields(Document doc, Iterable<CategoryPath> categories) throws IOException {
    if (categories == null) {
      throw new IllegalArgumentException("categories should not be null");
    }

    final Map<CategoryListParams,Iterable<CategoryPath>> categoryLists = createCategoryListMapping(categories);
    for (Entry<CategoryListParams, Iterable<CategoryPath>> e : categoryLists.entrySet()) {

      CategoryListParams clp = e.getKey();
      String dvField = clp.field + SortedSetDocValuesReaderState.FACET_FIELD_EXTENSION;

      // Add sorted-set DV fields, one per value:
      for(CategoryPath cp : e.getValue()) {
        if (cp.length != 2) {
          throw new IllegalArgumentException("only flat facets (dimension + label) are currently supported; got " + cp);
        }
        doc.add(new SortedSetDocValuesField(dvField, new BytesRef(cp.toString(indexingParams.getFacetDelimChar()))));
      }

      // add the drill-down field
      DrillDownStream drillDownStream = getDrillDownStream(e.getValue());
      Field drillDown = new Field(clp.field, drillDownStream, drillDownFieldType());
      doc.add(drillDown);
    }
  }
}

