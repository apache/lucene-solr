package org.apache.lucene.facet.util;

import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

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
 * Utilities for partitions - sizes and such
 * 
 * @lucene.experimental
 */
public final class PartitionsUtils {

  /**
   * Get the offset for a given partition.  That is, what is the minimum number an
   * ordinal could be for a particular partition. 
   */
  public final static int partitionOffset (  FacetIndexingParams iParams, 
                        int partitionNumber, 
                        final TaxonomyReader taxonomyReader) {
    return partitionNumber * partitionSize(iParams, taxonomyReader);
  }

  /**
   * @see #partitionOffset(FacetIndexingParams, int, TaxonomyReader)
   */
  public final static int partitionOffset (  FacetSearchParams sParams, 
                        int partitionNumber, 
                        final TaxonomyReader taxonomyReader) {
    return partitionOffset(sParams.getFacetIndexingParams(), partitionNumber, taxonomyReader);
  }

  /**
   * Get the partition size in this parameter, or return the size of the taxonomy, which
   * is smaller.  (Guarantees usage of as little memory as possible at search time).
   */
  public final static int partitionSize(FacetIndexingParams indexingParams, final TaxonomyReader taxonomyReader) {
    return Math.min(indexingParams.getPartitionSize(), taxonomyReader.getSize());
  }

  /**
   * @see #partitionSize(FacetIndexingParams, TaxonomyReader)
   */
  public final static int partitionSize(FacetSearchParams sParams, final TaxonomyReader taxonomyReader) {
    return partitionSize(sParams.getFacetIndexingParams(), taxonomyReader);
  }

  /**
   * Partition number of an ordinal.
   * <p>
   * This allows to locate the partition containing a certain (facet) ordinal.
   * @see FacetIndexingParams#getPartitionSize()      
   */
  public final static int partitionNumber(FacetIndexingParams iParams, int ordinal) {
    return ordinal / iParams.getPartitionSize();
  }

  /**
   * @see #partitionNumber(FacetIndexingParams, int)
   */
  public final static int partitionNumber(FacetSearchParams sParams, int ordinal) {
    return partitionNumber(sParams.getFacetIndexingParams(), ordinal);
  }

  /**
   * Partition name by category ordinal
   */
  public final static String partitionNameByOrdinal(  FacetIndexingParams iParams, 
                            CategoryListParams clParams, 
                            int ordinal) {
    int partition = partitionNumber(iParams, ordinal); 
    return partitionName(clParams, partition);
  }

  /** 
   * Partition name by its number
   */
  public final static String partitionName(CategoryListParams clParams, int partition) {
    String term = clParams.getTerm().text();
    if (partition == 0) {
      return term; // for backwards compatibility we do not add a partition number in this case
    }
    return term + partition;
  }

}
