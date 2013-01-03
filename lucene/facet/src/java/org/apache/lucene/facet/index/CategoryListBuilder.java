package org.apache.lucene.facet.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.lucene.facet.index.categorypolicy.OrdinalPolicy;
import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.util.PartitionsUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.UnsafeByteArrayOutputStream;
import org.apache.lucene.util.encoding.IntEncoder;

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
 * Builds a category list by encoding the category ordinals into one or more
 * {@link BytesRef}. Each {@link BytesRef} corresponds to a set of ordinals that
 * belong to the same partition. When partitions are not enabled (i.e.
 * {@link FacetIndexingParams#getPartitionSize()} returns
 * {@link Integer#MAX_VALUE}), only one {@link BytesRef} is returned by this
 * class.
 */
public class CategoryListBuilder {
 
  /** Specializes encoding ordinals when partitions are enabled/disabled. */
  private static abstract class OrdinalsEncoder {
    OrdinalsEncoder() {}
    public abstract void encode(int ordinal);
    public abstract HashMap<String,BytesRef> finish();
  }
  
  private static final class NoPartitionsOrdinalsEncoder extends OrdinalsEncoder {
    
    private final IntEncoder encoder;
    private final UnsafeByteArrayOutputStream ubaos;
    private final String name;
    
    NoPartitionsOrdinalsEncoder(CategoryListParams categoryListParams) {
      name = categoryListParams.getTerm().text();
      encoder = categoryListParams.createEncoder();
      ubaos = new UnsafeByteArrayOutputStream();
      encoder.reInit(ubaos);
    }
    
    @Override
    public void encode(int ordinal) {
      try {
        encoder.encode(ordinal);
      } catch (IOException e) {
        // shouldn't happen as we're writing to byte[]
        throw new RuntimeException("unexpected exception", e);
      }
    }
    
    @Override
    public HashMap<String,BytesRef> finish() {
      try {
        encoder.close();
      } catch (IOException e) {
        // shouldn't happen as we're writing to byte[]
        throw new RuntimeException("unexpected exception", e);
      }
      HashMap<String,BytesRef> result = new HashMap<String,BytesRef>();
      result.put(name, new BytesRef(ubaos.toByteArray(), ubaos.getStartPos(), ubaos.length()));
      return result;
    }
    
  }
  
  private static final class PerPartitionOrdinalsEncoder extends OrdinalsEncoder {

    private final FacetIndexingParams indexingParams;
    private final CategoryListParams categoryListParams;
    private final int partitionSize;
    private final HashMap<String,IntEncoder> partitionEncoder = new HashMap<String,IntEncoder>();
    private final HashMap<String,UnsafeByteArrayOutputStream> partitionBytes = new HashMap<String,UnsafeByteArrayOutputStream>();

    PerPartitionOrdinalsEncoder(FacetIndexingParams indexingParams, CategoryListParams categoryListParams) {
      this.indexingParams = indexingParams;
      this.categoryListParams = categoryListParams;
      this.partitionSize = indexingParams.getPartitionSize();
    }

    @Override
    public void encode(int ordinal) {
      final String name = PartitionsUtils.partitionNameByOrdinal(indexingParams, categoryListParams, ordinal);
      IntEncoder encoder = partitionEncoder.get(name);
      if (encoder == null) {
        encoder = categoryListParams.createEncoder();
        final UnsafeByteArrayOutputStream ubaos = new UnsafeByteArrayOutputStream();        
        encoder.reInit(ubaos);
        partitionEncoder.put(name, encoder);
        partitionBytes.put(name, ubaos);
      }
      try {
        encoder.encode(ordinal % partitionSize);
      } catch (IOException e) {
        // shouldn't happen as we're writing to byte[]
        throw new RuntimeException("unexpected exception", e);
      }
    }
    
    @Override
    public HashMap<String,BytesRef> finish() {
      // finish encoding
      IOUtils.closeWhileHandlingException(partitionEncoder.values());
      
      HashMap<String,BytesRef> bytes = new HashMap<String,BytesRef>();
      for (Entry<String,UnsafeByteArrayOutputStream> e : partitionBytes.entrySet()) {
        UnsafeByteArrayOutputStream ubaos = e.getValue();
        bytes.put(e.getKey(), new BytesRef(ubaos.toByteArray(), ubaos.getStartPos(), ubaos.length()));
      }
      return bytes;
    }
    
  }
  
  private final TaxonomyWriter taxoWriter;
  private final OrdinalsEncoder ordinalsEncoder;
  private final OrdinalPolicy ordinalPolicy;
  
  public CategoryListBuilder(CategoryListParams categoryListParams, FacetIndexingParams indexingParams, 
      TaxonomyWriter taxoWriter) {
    this.taxoWriter = taxoWriter;
    this.ordinalPolicy = indexingParams.getOrdinalPolicy();
    if (indexingParams.getPartitionSize() == Integer.MAX_VALUE) {
      ordinalsEncoder = new NoPartitionsOrdinalsEncoder(categoryListParams);
    } else {
      ordinalsEncoder = new PerPartitionOrdinalsEncoder(indexingParams, categoryListParams);
    }
  }

  /**
   * Encodes the given ordinal as well as any of its parent ordinals (per
   * {@link OrdinalPolicy}).
   */
  public void handle(int ordinal, CategoryPath cp) throws IOException {
    ordinalsEncoder.encode(ordinal);
    
    // add all parent ordinals, per OrdinalPolicy
    int parent = taxoWriter.getParent(ordinal);
    while (parent > 0) {
      if (ordinalPolicy.shouldAdd(parent)) {
        ordinalsEncoder.encode(parent);
      }
      parent = taxoWriter.getParent(parent);
    }
  }
  
  /**
   * Returns the encoded ordinals data. Every returned {@link BytesRef}
   * corresponds to a single partition (as defined by
   * {@link FacetIndexingParams#getPartitionSize()}) and the key denotes the
   * partition ID. When no partitions are defined, the returned map includes
   * only one value.
   */
  public HashMap<String,BytesRef> finish() {
    return ordinalsEncoder.finish();
  }
  
}
