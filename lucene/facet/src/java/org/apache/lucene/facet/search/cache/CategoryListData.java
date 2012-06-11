package org.apache.lucene.facet.search.cache;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.search.CategoryListIterator;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.util.collections.IntArray;

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
 * Category list data maintained in RAM.
 * <p>
 * Speeds up facets accumulation when more RAM is available.
 * <p>
 * Note that this will consume more memory: one int (4 bytes) for each category
 * of each document.
 * <p>
 * Note: at the moment this class is insensitive to updates of the index, and,
 * in particular, does not make use of Lucene's ability to refresh a single
 * segment.
 * <p>
 * See {@link CategoryListCache#register(CategoryListParams, CategoryListData)}
 * and
 * {@link CategoryListCache#loadAndRegister(CategoryListParams, IndexReader, TaxonomyReader, FacetIndexingParams)}.
 * 
 * @lucene.experimental
 */
public class CategoryListData {
  
  // TODO (Facet): experiment with different orders - p-d-c vs. current d-p-c.
  private transient volatile int[][][] docPartitionCategories;  
  
  /**
   * Empty constructor for extensions with modified computation of the data.
   */
  protected CategoryListData() {
  }
  
  /**
   * Compute category list data for caching for faster iteration.
   */
  CategoryListData(IndexReader reader, TaxonomyReader taxo, 
      FacetIndexingParams iparams, CategoryListParams clp) throws IOException {
  
    final int maxDoc = reader.maxDoc();
    int[][][]dpf  = new int[maxDoc][][];
    int numPartitions = (int)Math.ceil(taxo.getSize()/(double)iparams.getPartitionSize());
    IntArray docCategories = new IntArray(); 
    for (int part=0; part<numPartitions; part++) {
      CategoryListIterator cli = clp.createCategoryListIterator(reader, part);
      if (cli.init()) {
        for (int doc=0; doc<maxDoc; doc++) {
          if (cli.skipTo(doc)) {
            docCategories.clear(false);
            if (dpf[doc]==null) {
              dpf[doc] = new int[numPartitions][];
            }
            long category;
            while ((category = cli.nextCategory()) <= Integer.MAX_VALUE) {
              docCategories.addToArray((int)category);
            }
            final int size = docCategories.size();
            dpf[doc][part] = new int[size];
            for (int i=0; i<size; i++) {
              dpf[doc][part][i] = docCategories.get(i);
            }
          }
        }
      }
    }
    docPartitionCategories = dpf;
  }
  
  /**
   * Iterate on the category list data for the specified partition.
   */
  public CategoryListIterator iterator(int partition) throws IOException {
    return new RAMCategoryListIterator(partition, docPartitionCategories);
  }

  /**
   * Internal: category list iterator over uncompressed category info in RAM
   */
  private static class RAMCategoryListIterator implements CategoryListIterator {
    private final int part;
    private final int[][][] dpc;
    private int currDoc = -1;
    private int nextCategoryIndex = -1;  
    
    RAMCategoryListIterator(int part, int[][][] docPartitionCategories) {
      this.part = part;
      dpc = docPartitionCategories;
    }

    public boolean init() throws IOException {
      return dpc!=null && dpc.length>part;
    }

    public long nextCategory() throws IOException {
      if (nextCategoryIndex >= dpc[currDoc][part].length) {
        return 1L+Integer.MAX_VALUE;
      }
      return dpc[currDoc][part][nextCategoryIndex++]; 
    }

    public boolean skipTo(int docId) throws IOException {
      final boolean res = dpc.length>docId && dpc[docId]!=null && dpc[docId][part]!=null;
      if (res) {
        currDoc = docId;
        nextCategoryIndex = 0;
      }
      return res;
    }
  }
}