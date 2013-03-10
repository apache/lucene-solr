package org.apache.lucene.facet.util;

import org.apache.lucene.facet.params.FacetIndexingParams;
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

  /** The prefix that is added to the name of the partition. */
  public static final String PART_NAME_PREFIX = "$part";
  
  /**
   * Get the partition size in this parameter, or return the size of the taxonomy, which
   * is smaller.  (Guarantees usage of as little memory as possible at search time).
   */
  public final static int partitionSize(FacetIndexingParams indexingParams, final TaxonomyReader taxonomyReader) {
    return Math.min(indexingParams.getPartitionSize(), taxonomyReader.getSize());
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
   * Partition name by category ordinal
   */
  public final static String partitionNameByOrdinal(FacetIndexingParams iParams, int ordinal) {
    int partition = partitionNumber(iParams, ordinal);
    return partitionName(partition);
  }

  /** Partition name by its number */
  public final static String partitionName(int partition) {
    // TODO would be good if this method isn't called when partitions are not enabled.
    // perhaps through some specialization code.
    if (partition == 0) {
      // since regular faceted search code goes through this method too,
      // return the same value for partition 0 and when there are no partitions
      return "";
    }
    return PART_NAME_PREFIX + Integer.toString(partition);
  }

}
