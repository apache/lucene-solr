package org.apache.lucene.facet.util;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.OrdinalMap;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.store.Directory;

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
 * Utility methods for merging index and taxonomy directories.
 * @lucene.experimental
 */
public class TaxonomyMergeUtils {

  /**
   * Merges the given taxonomy and index directories and commits the changes to
   * the given writers.
   */
  public static void merge(Directory srcIndexDir, Directory srcTaxDir, OrdinalMap map, IndexWriter destIndexWriter, 
      DirectoryTaxonomyWriter destTaxWriter, FacetIndexingParams params) throws IOException {
    // merge the taxonomies
    destTaxWriter.addTaxonomy(srcTaxDir, map);
    int ordinalMap[] = map.getMap();
    DirectoryReader reader = DirectoryReader.open(srcIndexDir, -1);
    List<AtomicReaderContext> leaves = reader.leaves();
    int numReaders = leaves.size();
    AtomicReader wrappedLeaves[] = new AtomicReader[numReaders];
    for (int i = 0; i < numReaders; i++) {
      wrappedLeaves[i] = new OrdinalMappingAtomicReader(leaves.get(i).reader(), ordinalMap, params);
    }
    try {
      destIndexWriter.addIndexes(new MultiReader(wrappedLeaves));
      
      // commit changes to taxonomy and index respectively.
      destTaxWriter.commit();
      destIndexWriter.commit();
    } finally {
      reader.close();
    }
  }
  
}
