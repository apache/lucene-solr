package org.apache.lucene.facet.index;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.facet.encoding.IntEncoder;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.CategoryListParams.OrdinalPolicy;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.util.PartitionsUtils;
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
 * A {@link CategoryListBuilder} which builds a counting list data by encoding
 * the category ordinals into one or more {@link BytesRef}. Each
 * {@link BytesRef} corresponds to a set of ordinals that belong to the same
 * partition. When partitions are not enabled (i.e.
 * {@link FacetIndexingParams#getPartitionSize()} returns
 * {@link Integer#MAX_VALUE}), only one {@link BytesRef} is returned by this
 * class.
 * <p>
 * Counting lists are used usually for computing the weight of categories by
 * summing their number of occurrences (hence counting) in a result set.
 */
public class CountingListBuilder implements CategoryListBuilder {
 
  /** Specializes encoding ordinals when partitions are enabled/disabled. */
  private static abstract class OrdinalsEncoder {
    OrdinalsEncoder() {}
    public abstract Map<String,BytesRef> encode(IntsRef ordinals);
  }
  
  private static final class NoPartitionsOrdinalsEncoder extends OrdinalsEncoder {
    
    private final IntEncoder encoder;
    private final String name = "";
    
    NoPartitionsOrdinalsEncoder(CategoryListParams categoryListParams) {
      encoder = categoryListParams.createEncoder();
    }
    
    @Override
    public Map<String,BytesRef> encode(IntsRef ordinals) {
      final BytesRef bytes = new BytesRef(128); // should be enough for most common applications
      encoder.encode(ordinals, bytes);
      return Collections.singletonMap(name, bytes);
    }
    
  }
  
  private static final class PerPartitionOrdinalsEncoder extends OrdinalsEncoder {

    private final FacetIndexingParams indexingParams;
    private final CategoryListParams categoryListParams;
    private final int partitionSize;
    private final HashMap<String,IntEncoder> partitionEncoder = new HashMap<String,IntEncoder>();

    PerPartitionOrdinalsEncoder(FacetIndexingParams indexingParams, CategoryListParams categoryListParams) {
      this.indexingParams = indexingParams;
      this.categoryListParams = categoryListParams;
      this.partitionSize = indexingParams.getPartitionSize();
    }

    @Override
    public HashMap<String,BytesRef> encode(IntsRef ordinals) {
      // build the partitionOrdinals map
      final HashMap<String,IntsRef> partitionOrdinals = new HashMap<String,IntsRef>();
      for (int i = 0; i < ordinals.length; i++) {
        int ordinal = ordinals.ints[i];
        final String name = PartitionsUtils.partitionNameByOrdinal(indexingParams, ordinal);
        IntsRef partitionOrds = partitionOrdinals.get(name);
        if (partitionOrds == null) {
          partitionOrds = new IntsRef(32);
          partitionOrdinals.put(name, partitionOrds);
          partitionEncoder.put(name, categoryListParams.createEncoder());
        }
        partitionOrds.ints[partitionOrds.length++] = ordinal % partitionSize;
      }
      
      HashMap<String,BytesRef> partitionBytes = new HashMap<String,BytesRef>();
      for (Entry<String,IntsRef> e : partitionOrdinals.entrySet()) {
        String name = e.getKey();
        final IntEncoder encoder = partitionEncoder.get(name);
        final BytesRef bytes = new BytesRef(128); // should be enough for most common applications        
        encoder.encode(e.getValue(), bytes);
        partitionBytes.put(name, bytes);
      }
      return partitionBytes;
    }
    
  }
  
  private final OrdinalsEncoder ordinalsEncoder;
  private final TaxonomyWriter taxoWriter;
  private final CategoryListParams clp;
  
  public CountingListBuilder(CategoryListParams categoryListParams, FacetIndexingParams indexingParams, 
      TaxonomyWriter taxoWriter) {
    this.taxoWriter = taxoWriter;
    this.clp = categoryListParams;
    if (indexingParams.getPartitionSize() == Integer.MAX_VALUE) {
      ordinalsEncoder = new NoPartitionsOrdinalsEncoder(categoryListParams);
    } else {
      ordinalsEncoder = new PerPartitionOrdinalsEncoder(indexingParams, categoryListParams);
    }
  }

  /**
   * Every returned {@link BytesRef} corresponds to a single partition (as
   * defined by {@link FacetIndexingParams#getPartitionSize()}) and the key
   * denotes the partition ID. When no partitions are defined, the returned map
   * contains only one value.
   * <p>
   * <b>NOTE:</b> the {@code ordinals} array is modified by adding parent
   * ordinals to it. Also, some encoders may sort the array and remove duplicate
   * ordinals. Therefore you may want to invoke this method after you finished
   * processing the array for other purposes.
   */
  @Override
  public Map<String,BytesRef> build(IntsRef ordinals, Iterable<CategoryPath> categories) throws IOException {
    int upto = ordinals.length; // since we may add ordinals to IntsRef, iterate upto original length

    Iterator<CategoryPath> iter = categories.iterator();
    for (int i = 0; i < upto; i++) {
      int ordinal = ordinals.ints[i];
      CategoryPath cp = iter.next();
      OrdinalPolicy op = clp.getOrdinalPolicy(cp.components[0]);
      if (op != OrdinalPolicy.NO_PARENTS) {
        // need to add parents too
        int parent = taxoWriter.getParent(ordinal);
        if (parent > 0) {
          // only do this if the category is not a dimension itself, otherwise, it was just discarded by the 'if' below
          while (parent > 0) {
            ordinals.ints[ordinals.length++] = parent;
            parent = taxoWriter.getParent(parent);
          }
          if (op == OrdinalPolicy.ALL_BUT_DIMENSION) { // discard the last added parent, which is the dimension
            ordinals.length--;
          }
        }
      }
    }
    return ordinalsEncoder.encode(ordinals);
  }
  
}
