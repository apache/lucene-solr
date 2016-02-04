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
package org.apache.lucene.facet.taxonomy;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.OrdinalMap;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.store.Directory;

/**
 * Utility methods for merging index and taxonomy directories.
 * @lucene.experimental
 */
public abstract class TaxonomyMergeUtils {
  
  private TaxonomyMergeUtils() {}
  
  /**
   * Merges the given taxonomy and index directories and commits the changes to
   * the given writers.
   */
  public static void merge(Directory srcIndexDir, Directory srcTaxoDir, OrdinalMap map, IndexWriter destIndexWriter,
      DirectoryTaxonomyWriter destTaxoWriter, FacetsConfig srcConfig) throws IOException {
    
    // merge the taxonomies
    destTaxoWriter.addTaxonomy(srcTaxoDir, map);
    int ordinalMap[] = map.getMap();
    DirectoryReader reader = DirectoryReader.open(srcIndexDir);
    try {
      List<LeafReaderContext> leaves = reader.leaves();
      int numReaders = leaves.size();
      CodecReader wrappedLeaves[] = new CodecReader[numReaders];
      for (int i = 0; i < numReaders; i++) {
        wrappedLeaves[i] = SlowCodecReaderWrapper.wrap(new OrdinalMappingLeafReader(leaves.get(i).reader(), ordinalMap, srcConfig));
      }
      destIndexWriter.addIndexes(wrappedLeaves);
      
      // commit changes to taxonomy and index respectively.
      destTaxoWriter.commit();
      destIndexWriter.commit();
    } finally {
      reader.close();
    }
  }
  
}
