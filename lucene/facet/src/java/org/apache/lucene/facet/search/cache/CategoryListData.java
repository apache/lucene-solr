package org.apache.lucene.facet.search.cache;

import java.io.IOException;

import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.search.CategoryListIterator;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
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
  
  /** Compute category list data for caching for faster iteration. */
  CategoryListData(IndexReader reader, TaxonomyReader taxo, FacetIndexingParams iparams, CategoryListParams clp) 
      throws IOException {
  
    int[][][]dpf  = new int[reader.maxDoc()][][];
    int numPartitions = (int)Math.ceil(taxo.getSize()/(double)iparams.getPartitionSize());
    IntsRef ordinals = new IntsRef(32);
    for (int part = 0; part < numPartitions; part++) {
      for (AtomicReaderContext context : reader.leaves()) {
        CategoryListIterator cli = clp.createCategoryListIterator(part);
        if (cli.setNextReader(context)) {
          final int maxDoc = context.reader().maxDoc();
          for (int i = 0; i < maxDoc; i++) {
            cli.getOrdinals(i, ordinals);
            if (ordinals.length > 0) {
              int doc = i + context.docBase;
              if (dpf[doc] == null) {
                dpf[doc] = new int[numPartitions][];
              }
              if (dpf[doc][part] == null) {
                dpf[doc][part] = new int[ordinals.length];
              }
              for (int j = 0; j < ordinals.length; j++) {
                dpf[doc][part][j] = ordinals.ints[j];
              }
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

  /** Internal: category list iterator over uncompressed category info in RAM */
  private static class RAMCategoryListIterator implements CategoryListIterator {
    
    private int docBase;
    private final int part;
    private final int[][][] dpc;
    
    RAMCategoryListIterator(int part, int[][][] docPartitionCategories) {
      this.part = part;
      dpc = docPartitionCategories;
    }

    @Override
    public boolean setNextReader(AtomicReaderContext context) throws IOException {
      docBase = context.docBase;
      return dpc != null && dpc.length > part;
    }
    
    @Override
    public void getOrdinals(int docID, IntsRef ints) throws IOException {
      ints.length = 0;
      docID += docBase;
      if (dpc.length > docID && dpc[docID] != null && dpc[docID][part] != null) {
        if (ints.ints.length < dpc[docID][part].length) {
          ints.grow(dpc[docID][part].length);
        }
        ints.length = 0;
        for (int i = 0; i < dpc[docID][part].length; i++) {
          ints.ints[ints.length++] = dpc[docID][part][i];
        }
      }
    }
  }
  
}