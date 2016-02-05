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
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.FacetsConfig.DimConfig;
import org.apache.lucene.facet.taxonomy.OrdinalsReader.OrdinalsSegmentReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.OrdinalMap;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

/**
 * A {@link org.apache.lucene.index.FilterLeafReader} for updating facets ordinal references,
 * based on an ordinal map. You should use this code in conjunction with merging
 * taxonomies - after you merge taxonomies, you receive an {@link OrdinalMap}
 * which maps the 'old' ordinals to the 'new' ones. You can use that map to
 * re-map the doc values which contain the facets information (ordinals) either
 * before or while merging the indexes.
 * <p>
 * For re-mapping the ordinals during index merge, do the following:
 * 
 * <pre class="prettyprint">
 * // merge the old taxonomy with the new one.
 * OrdinalMap map = new MemoryOrdinalMap();
 * DirectoryTaxonomyWriter.addTaxonomy(srcTaxoDir, map);
 * int[] ordmap = map.getMap();
 * 
 * // Add the index and re-map ordinals on the go
 * DirectoryReader reader = DirectoryReader.open(oldDir);
 * IndexWriterConfig conf = new IndexWriterConfig(VER, ANALYZER);
 * IndexWriter writer = new IndexWriter(newDir, conf);
 * List&lt;LeafReaderContext&gt; leaves = reader.leaves();
 * LeafReader wrappedLeaves[] = new LeafReader[leaves.size()];
 * for (int i = 0; i &lt; leaves.size(); i++) {
 *   wrappedLeaves[i] = new OrdinalMappingLeafReader(leaves.get(i).reader(), ordmap);
 * }
 * writer.addIndexes(new MultiReader(wrappedLeaves));
 * writer.commit();
 * </pre>
 * 
 * @lucene.experimental
 */
public class OrdinalMappingLeafReader extends FilterLeafReader {
  
  // silly way, but we need to use dedupAndEncode and it's protected on FacetsConfig.
  private static class InnerFacetsConfig extends FacetsConfig {
    
    InnerFacetsConfig() {}
    
    @Override
    public BytesRef dedupAndEncode(IntsRef ordinals) {
      return super.dedupAndEncode(ordinals);
    }
    
  }
  
  private class OrdinalMappingBinaryDocValues extends BinaryDocValues {
    
    private final IntsRef ordinals = new IntsRef(32);
    private final OrdinalsSegmentReader ordsReader;
    
    OrdinalMappingBinaryDocValues(OrdinalsSegmentReader ordsReader) throws IOException {
      this.ordsReader = ordsReader;
    }
    
    @SuppressWarnings("synthetic-access")
    @Override
    public BytesRef get(int docID) {
      try {
        // NOTE: this isn't quite koscher, because in general
        // multiple threads can call BinaryDV.get which would
        // then conflict on the single ordinals instance, but
        // because this impl is only used for merging, we know
        // only 1 thread calls us:
        ordsReader.get(docID, ordinals);
        
        // map the ordinals
        for (int i = 0; i < ordinals.length; i++) {
          ordinals.ints[i] = ordinalMap[ordinals.ints[i]];
        }
        
        return encode(ordinals);
      } catch (IOException e) {
        throw new RuntimeException("error reading category ordinals for doc " + docID, e);
      }
    }
  }
  
  private final int[] ordinalMap;
  private final InnerFacetsConfig facetsConfig;
  private final Set<String> facetFields;
  
  /**
   * Wraps an LeafReader, mapping ordinals according to the ordinalMap, using
   * the provided {@link FacetsConfig} which was used to build the wrapped
   * reader.
   */
  public OrdinalMappingLeafReader(LeafReader in, int[] ordinalMap, FacetsConfig srcConfig) {
    super(in);
    this.ordinalMap = ordinalMap;
    facetsConfig = new InnerFacetsConfig();
    facetFields = new HashSet<>();
    for (DimConfig dc : srcConfig.getDimConfigs().values()) {
      facetFields.add(dc.indexFieldName);
    }
    // always add the default indexFieldName. This is because FacetsConfig does
    // not explicitly record dimensions that were indexed under the default
    // DimConfig, unless they have a custome DimConfig.
    facetFields.add(FacetsConfig.DEFAULT_DIM_CONFIG.indexFieldName);
  }
  
  /**
   * Expert: encodes category ordinals into a BytesRef. Override in case you use
   * custom encoding, other than the default done by FacetsConfig.
   */
  protected BytesRef encode(IntsRef ordinals) {
    return facetsConfig.dedupAndEncode(ordinals);
  }
  
  /**
   * Expert: override in case you used custom encoding for the categories under
   * this field.
   */
  protected OrdinalsReader getOrdinalsReader(String field) {
    return new DocValuesOrdinalsReader(field);
  }
  
  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    if (facetFields.contains(field)) {
      final OrdinalsReader ordsReader = getOrdinalsReader(field);
      return new OrdinalMappingBinaryDocValues(ordsReader.getReader(in.getContext()));
    } else {
      return in.getBinaryDocValues(field);
    }
  }
  
}
