package org.apache.lucene.facet.example.merge;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.facet.example.ExampleUtils;
import org.apache.lucene.facet.index.OrdinalMappingAtomicReader;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.DiskOrdinalMap;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.MemoryOrdinalMap;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.OrdinalMap;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
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
 * @lucene.experimental
 */
public class TaxonomyMergeUtils {

  /**
   * Merges the given taxonomy and index directories. Note that this method
   * opens {@link DirectoryTaxonomyWriter} and {@link IndexWriter} on the
   * respective destination indexes. Therefore if you have a writer open on any
   * of them, it should be closed, or you should use
   * {@link #merge(Directory, Directory, IndexWriter, DirectoryTaxonomyWriter)}
   * instead.
   * 
   * @see #merge(Directory, Directory, IndexWriter, DirectoryTaxonomyWriter)
   */
  public static void merge(Directory srcIndexDir, Directory srcTaxDir,
                            Directory destIndexDir, Directory destTaxDir) throws IOException {
    IndexWriter destIndexWriter = new IndexWriter(destIndexDir,
        new IndexWriterConfig(ExampleUtils.EXAMPLE_VER, null));
    DirectoryTaxonomyWriter destTaxWriter = new DirectoryTaxonomyWriter(destTaxDir);
    merge(srcIndexDir, srcTaxDir, new MemoryOrdinalMap(), destIndexWriter, destTaxWriter);
    destTaxWriter.close();
    destIndexWriter.close();
  }

  /**
   * Merges the given taxonomy and index directories and commits the changes to
   * the given writers. This method uses {@link MemoryOrdinalMap} to store the
   * mapped ordinals. If you cannot afford the memory, you can use
   * {@link #merge(Directory, Directory, DirectoryTaxonomyWriter.OrdinalMap, IndexWriter, DirectoryTaxonomyWriter)}
   * by passing {@link DiskOrdinalMap}.
   * 
   * @see #merge(Directory, Directory, DirectoryTaxonomyWriter.OrdinalMap, IndexWriter, DirectoryTaxonomyWriter)
   */
  public static void merge(Directory srcIndexDir, Directory srcTaxDir,
                            IndexWriter destIndexWriter, 
                            DirectoryTaxonomyWriter destTaxWriter) throws IOException {
    merge(srcIndexDir, srcTaxDir, new MemoryOrdinalMap(), destIndexWriter, destTaxWriter);
  }
  
  /**
   * Merges the given taxonomy and index directories and commits the changes to
   * the given writers.
   */
  public static void merge(Directory srcIndexDir, Directory srcTaxDir,
                            OrdinalMap map, IndexWriter destIndexWriter,
                            DirectoryTaxonomyWriter destTaxWriter) throws IOException {
    // merge the taxonomies
    destTaxWriter.addTaxonomy(srcTaxDir, map);

    int ordinalMap[] = map.getMap();
    FacetIndexingParams params = FacetIndexingParams.ALL_PARENTS;

    DirectoryReader reader = DirectoryReader.open(srcIndexDir, -1);
    List<AtomicReaderContext> leaves = reader.leaves();
    AtomicReader wrappedLeaves[] = new AtomicReader[leaves.size()];
    for (int i = 0; i < leaves.size(); i++) {
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
