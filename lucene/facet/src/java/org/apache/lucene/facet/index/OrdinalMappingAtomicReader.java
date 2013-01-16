package org.apache.lucene.facet.index;

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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.OrdinalMap;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.encoding.IntDecoder;
import org.apache.lucene.util.encoding.IntEncoder;

/**
 * A {@link FilterAtomicReader} for updating facets ordinal references,
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
 * OrdinalMap map = DirectoryTaxonomyWriter.addTaxonomies();
 * int[] ordmap = map.getMap();
 * 
 * // Add the index and re-map ordinals on the go
 * DirectoryReader reader = DirectoryReader.open(oldDir);
 * IndexWriterConfig conf = new IndexWriterConfig(VER, ANALYZER);
 * IndexWriter writer = new IndexWriter(newDir, conf);
 * List&lt;AtomicReaderContext&gt; leaves = reader.leaves();
 *   AtomicReader wrappedLeaves[] = new AtomicReader[leaves.size()];
 *   for (int i = 0; i < leaves.size(); i++) {
 *     wrappedLeaves[i] = new OrdinalMappingAtomicReader(leaves.get(i).reader(), ordmap);
 *   }
 * writer.addIndexes(new MultiReader(wrappedLeaves));
 * writer.commit();
 * </pre>
 * 
 * @lucene.experimental
 */
public class OrdinalMappingAtomicReader extends FilterAtomicReader {
  
  private final int[] ordinalMap;
  
  private final Map<String,CategoryListParams> dvFieldMap = new HashMap<String,CategoryListParams>();
  
  /**
   * Wraps an AtomicReader, mapping ordinals according to the ordinalMap.
   * Calls {@link #OrdinalMappingAtomicReader(AtomicReader, int[], FacetIndexingParams)
   * OrdinalMappingAtomicReader(in, ordinalMap, new DefaultFacetIndexingParams())}
   */
  public OrdinalMappingAtomicReader(AtomicReader in, int[] ordinalMap) {
    this(in, ordinalMap, FacetIndexingParams.ALL_PARENTS);
  }
  
  /**
   * Wraps an AtomicReader, mapping ordinals according to the ordinalMap,
   * using the provided indexingParams.
   */
  public OrdinalMappingAtomicReader(AtomicReader in, int[] ordinalMap, FacetIndexingParams indexingParams) {
    super(in);
    this.ordinalMap = ordinalMap;
    for (CategoryListParams params: indexingParams.getAllCategoryListParams()) {
      dvFieldMap.put(params.field, params);
    }
  }

  @Override
  public DocValues docValues(String field) throws IOException {
    DocValues inner = super.docValues(field);
    if (inner == null) {
      return inner;
    }
    
    CategoryListParams clp = dvFieldMap.get(field);
    if (clp == null) {
      return inner;
    } else {
      return new OrdinalMappingDocValues(inner, clp);
    }
  }
  
  private class OrdinalMappingDocValues extends DocValues {

    private final CategoryListParams clp;
    private final DocValues delegate;
    
    public OrdinalMappingDocValues(DocValues delegate, CategoryListParams clp) {
      this.delegate = delegate;
      this.clp = clp;
    }

    @Override
    protected Source loadSource() throws IOException {
      return new OrdinalMappingSource(getType(), clp, delegate.getSource());
    }

    @Override
    protected Source loadDirectSource() throws IOException {
      return new OrdinalMappingSource(getType(), clp, delegate.getDirectSource());
    }

    @Override
    public Type getType() {
      return Type.BYTES_VAR_STRAIGHT;
    }
    
  }
  
  private class OrdinalMappingSource extends Source {

    private final IntEncoder encoder;
    private final IntDecoder decoder;
    private final IntsRef ordinals = new IntsRef(32);
    private final Source delegate;
    
    protected OrdinalMappingSource(Type type, CategoryListParams clp, Source delegate) {
      super(type);
      this.delegate = delegate;
      encoder = clp.createEncoder();
      decoder = encoder.createMatchingDecoder();
    }
    
    @SuppressWarnings("synthetic-access")
    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      ref = delegate.getBytes(docID, ref);
      if (ref == null || ref.length == 0) {
        return ref;
      } else {
        decoder.decode(ref, ordinals);
        
        // map the ordinals
        for (int i = 0; i < ordinals.length; i++) {
          ordinals.ints[i] = ordinalMap[ordinals.ints[i]];
        }
        
        encoder.encode(ordinals, ref);
        return ref;
      }
    }
    
  }
  
}
