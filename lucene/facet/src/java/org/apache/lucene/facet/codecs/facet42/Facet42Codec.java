package org.apache.lucene.facet.codecs.facet42;

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

import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;

/**
 * Same as {@link Lucene42Codec} except it uses {@link Facet42DocValuesFormat}
 * for facet fields (faster-but-more-RAM-consuming doc values).
 * 
 * <p>
 * <b>NOTE</b>: this codec does not support facet partitions (see
 * {@link FacetIndexingParams#getPartitionSize()}).
 *
 * <p>
 * <b>NOTE</b>: this format cannot handle more than 2 GB
 * of facet data in a single segment.  If your usage may hit
 * this limit, you can either use Lucene's default
 * DocValuesFormat, limit the maximum segment size in your
 * MergePolicy, or send us a patch fixing the limitation.
 * 
 * @lucene.experimental
 */
public class Facet42Codec extends Lucene42Codec {

  private final Set<String> facetFields;
  private final DocValuesFormat facetsDVFormat = DocValuesFormat.forName("Facet42");
  private final DocValuesFormat lucene42DVFormat = DocValuesFormat.forName("Lucene42");

  // must have that for SPI purposes
  /** Default constructor, uses {@link FacetIndexingParams#DEFAULT}. */
  public Facet42Codec() {
    this(FacetIndexingParams.DEFAULT);
  }

  /**
   * Initializes with the given {@link FacetIndexingParams}. Returns the proper
   * {@link DocValuesFormat} for the fields that are returned by
   * {@link FacetIndexingParams#getAllCategoryListParams()}.
   */
  public Facet42Codec(FacetIndexingParams fip) {
    if (fip.getPartitionSize() != Integer.MAX_VALUE) {
      throw new IllegalArgumentException("this Codec does not support partitions");
    }
    this.facetFields = new HashSet<String>();
    for (CategoryListParams clp : fip.getAllCategoryListParams()) {
      facetFields.add(clp.field);
    }
  }
  
  @Override
  public DocValuesFormat getDocValuesFormatForField(String field) {
    if (facetFields.contains(field)) {
      return facetsDVFormat;
    } else {
      return lucene42DVFormat;
    }
  }
  
}
