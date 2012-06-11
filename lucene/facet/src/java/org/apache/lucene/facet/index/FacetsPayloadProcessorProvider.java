package org.apache.lucene.facet.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.PayloadProcessorProvider;
import org.apache.lucene.index.PayloadProcessorProvider.ReaderPayloadProcessor; // javadocs
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;

import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.OrdinalMap;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.encoding.IntDecoder;
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
 * A {@link PayloadProcessorProvider} for updating facets ordinal references,
 * based on an ordinal map. You should use this code in conjunction with merging
 * taxonomies - after you merge taxonomies, you receive an {@link OrdinalMap}
 * which maps the 'old' payloads to the 'new' ones. You can use that map to
 * re-map the payloads which contain the facets information (ordinals) either
 * before or while merging the indexes.
 * <p>
 * For re-mapping the ordinals before you merge the indexes, do the following:
 * 
 * <pre>
 * // merge the old taxonomy with the new one.
 * OrdinalMap map = LuceneTaxonomyWriter.addTaxonomies();
 * int[] ordmap = map.getMap();
 * 
 * // re-map the ordinals on the old directory.
 * Directory oldDir;
 * FacetsPayloadProcessorProvider fppp = new FacetsPayloadProcessorProvider(
 *     oldDir, ordmap);
 * IndexWriterConfig conf = new IndexWriterConfig(VER, ANALYZER);
 * conf.setMergePolicy(new ForceOptimizeMergePolicy());
 * IndexWriter writer = new IndexWriter(oldDir, conf);
 * writer.setPayloadProcessorProvider(fppp);
 * writer.forceMerge(1);
 * writer.close();
 * 
 * // merge that directory with the new index.
 * IndexWriter newWriter; // opened on the 'new' Directory
 * newWriter.addIndexes(oldDir);
 * newWriter.commit();
 * </pre>
 * 
 * For re-mapping the ordinals during index merge, do the following:
 * 
 * <pre>
 * // merge the old taxonomy with the new one.
 * OrdinalMap map = LuceneTaxonomyWriter.addTaxonomies();
 * int[] ordmap = map.getMap();
 * 
 * // Add the index and re-map ordinals on the go
 * IndexReader r = IndexReader.open(oldDir);
 * IndexWriterConfig conf = new IndexWriterConfig(VER, ANALYZER);
 * IndexWriter writer = new IndexWriter(newDir, conf);
 * writer.setPayloadProcessorProvider(fppp);
 * writer.addIndexes(r);
 * writer.commit();
 * </pre>
 * <p>
 * <b>NOTE:</b> while the second example looks simpler, IndexWriter may trigger
 * a long merge due to addIndexes. The first example avoids this perhaps
 * unneeded merge, as well as can be done separately (e.g. on another node)
 * before the index is merged.
 * 
 * @lucene.experimental
 */
public class FacetsPayloadProcessorProvider extends PayloadProcessorProvider {
  
  private final Directory workDir;
  
  private final ReaderPayloadProcessor dirProcessor;

  /**
   * Construct FacetsPayloadProcessorProvider with FacetIndexingParams
   * 
   * @param dir the {@link Directory} containing the segments to update
   * @param ordinalMap an array mapping previous facets ordinals to new ones
   * @param indexingParams the facets indexing parameters
   */
  public FacetsPayloadProcessorProvider(Directory dir, int[] ordinalMap,
                                        FacetIndexingParams indexingParams) {
    workDir = dir;
    dirProcessor = new FacetsDirPayloadProcessor(indexingParams, ordinalMap);
  }
  
  @Override
  public ReaderPayloadProcessor getReaderProcessor(AtomicReader reader) throws IOException {
    if (reader instanceof SegmentReader) {
      if (workDir == ((SegmentReader) reader).directory()) {
        return dirProcessor;
      }
    }
    return null;
  }
  
  /**
   * {@link ReaderPayloadProcessor} that processes 
   * facet ordinals according to the passed in {@link FacetIndexingParams}.
   */
  public static class FacetsDirPayloadProcessor extends ReaderPayloadProcessor {
    
    private final Map<Term, CategoryListParams> termMap = new HashMap<Term, CategoryListParams>(1);
    
    private final int[] ordinalMap;
    
    /**
     * Construct FacetsDirPayloadProcessor with custom FacetIndexingParams
     * @param ordinalMap an array mapping previous facets ordinals to new ones
     * @param indexingParams the facets indexing parameters
     */
    protected FacetsDirPayloadProcessor(FacetIndexingParams indexingParams, int[] ordinalMap) {
      this.ordinalMap = ordinalMap;
      for (CategoryListParams params: indexingParams.getAllCategoryListParams()) {
        termMap.put(params.getTerm(), params);
      }
    }
    
    @Override
    public PayloadProcessor getProcessor(String field, BytesRef bytes) throws IOException {
      // TODO (Facet): don't create terms
      CategoryListParams params = termMap.get(new Term(field, bytes));
      if (params == null) {
        return null;
      }
      return new FacetsPayloadProcessor(params, ordinalMap);
    }

  }
  
  /** A PayloadProcessor for updating facets ordinal references, based on an ordinal map */
  public static class FacetsPayloadProcessor extends PayloadProcessor {
    
    private final IntEncoder encoder;
    private final IntDecoder decoder;
    private final int[] ordinalMap;
    private final ByteArrayOutputStream os = new ByteArrayOutputStream();
    
    /**
     * @param params defines the encoding of facet ordinals as payload
     * @param ordinalMap an array mapping previous facets ordinals to new ones
     */
    protected FacetsPayloadProcessor(CategoryListParams params, int[] ordinalMap) {
      encoder = params.createEncoder();
      decoder = encoder.createMatchingDecoder();
      this.ordinalMap = ordinalMap;
    }

    @Override
    public void processPayload(BytesRef payload) throws IOException {
      InputStream is = new ByteArrayInputStream(payload.bytes, payload.offset, payload.length);
      decoder.reInit(is);
      os.reset();
      encoder.reInit(os);
      long ordinal;
      while ((ordinal = decoder.decode()) != IntDecoder.EOS) {
        int newOrdinal = ordinalMap[(int)ordinal];
        encoder.encode(newOrdinal);      
      }
      encoder.close();
      // TODO (Facet): avoid copy?
      byte out[] = os.toByteArray();
      payload.bytes = out;
      payload.offset = 0;
      payload.length = out.length;
    }
  }
  
}
