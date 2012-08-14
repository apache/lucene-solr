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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.DefaultFacetIndexingParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.OrdinalMap;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.encoding.IntDecoder;
import org.apache.lucene.util.encoding.IntEncoder;

/**
 * A {@link FilterAtomicReader} for updating facets ordinal references,
 * based on an ordinal map. You should use this code in conjunction with merging
 * taxonomies - after you merge taxonomies, you receive an {@link OrdinalMap}
 * which maps the 'old' payloads to the 'new' ones. You can use that map to
 * re-map the payloads which contain the facets information (ordinals) either
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
 * List<AtomicReaderContext> leaves = reader.leaves();
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
  // a little obtuse: but we dont need to create Term objects this way
  private final Map<String,Map<BytesRef,CategoryListParams>> termMap = 
      new HashMap<String,Map<BytesRef,CategoryListParams>>(1);
  
  /**
   * Wraps an AtomicReader, mapping ordinals according to the ordinalMap.
   * Calls {@link #OrdinalMappingAtomicReader(AtomicReader, int[], FacetIndexingParams)
   * OrdinalMappingAtomicReader(in, ordinalMap, new DefaultFacetIndexingParams())}
   */
  public OrdinalMappingAtomicReader(AtomicReader in, int[] ordinalMap) {
    this(in, ordinalMap, new DefaultFacetIndexingParams());
  }
  
  /**
   * Wraps an AtomicReader, mapping ordinals according to the ordinalMap,
   * using the provided indexingParams.
   */
  public OrdinalMappingAtomicReader(AtomicReader in, int[] ordinalMap, FacetIndexingParams indexingParams) {
    super(in);
    this.ordinalMap = ordinalMap;
    for (CategoryListParams params: indexingParams.getAllCategoryListParams()) {
      Term term = params.getTerm();
      Map<BytesRef,CategoryListParams> fieldMap = termMap.get(term.field());
      if (fieldMap == null) {
        fieldMap = new HashMap<BytesRef,CategoryListParams>(1);
        termMap.put(term.field(), fieldMap);
      }
      fieldMap.put(term.bytes(), params);
    }
  }

  @Override
  public Fields getTermVectors(int docID) throws IOException {
    Fields fields = super.getTermVectors(docID);
    if (fields == null) {
      return null;
    } else {
      return new OrdinalMappingFields(fields);
    }
  }

  @Override
  public Fields fields() throws IOException {
    Fields fields = super.fields();
    if (fields == null) {
      return null;
    } else {
      return new OrdinalMappingFields(fields);
    }
  }
  
  private class OrdinalMappingFields extends FilterFields {

    public OrdinalMappingFields(Fields in) {
      super(in);
    }

    @Override
    public Terms terms(String field) throws IOException {
      Terms terms = super.terms(field);
      if (terms == null) {
        return terms;
      }
      Map<BytesRef,CategoryListParams> termsMap = termMap.get(field);
      if (termsMap == null) {
        return terms;
      } else {
        return new OrdinalMappingTerms(terms, termsMap);
      }
    }
  }
  
  private class OrdinalMappingTerms extends FilterTerms {
    private final Map<BytesRef,CategoryListParams> termsMap;
    
    public OrdinalMappingTerms(Terms in, Map<BytesRef,CategoryListParams> termsMap) {
      super(in);
      this.termsMap = termsMap;
    }

    @Override
    public TermsEnum iterator(TermsEnum reuse) throws IOException {
      // TODO: should we reuse the inner termsenum?
      return new OrdinalMappingTermsEnum(super.iterator(reuse), termsMap);
    }
  }
  
  private class OrdinalMappingTermsEnum extends FilterTermsEnum {
    private final Map<BytesRef,CategoryListParams> termsMap;
    
    public OrdinalMappingTermsEnum(TermsEnum in, Map<BytesRef,CategoryListParams> termsMap) {
      super(in);
      this.termsMap = termsMap;
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
      // TODO: we could reuse our D&P enum if we need
      DocsAndPositionsEnum inner = super.docsAndPositions(liveDocs, reuse, flags);
      if (inner == null) {
        return inner;
      }
      
      CategoryListParams params = termsMap.get(term());
      if (params == null) {
        return inner;
      }
      
      return new OrdinalMappingDocsAndPositionsEnum(inner, params);
    }
  }
  
  private class OrdinalMappingDocsAndPositionsEnum extends FilterDocsAndPositionsEnum {
    private final IntEncoder encoder;
    private final IntDecoder decoder;
    private final ByteArrayOutputStream os = new ByteArrayOutputStream();
    private final BytesRef payloadOut = new BytesRef();

    public OrdinalMappingDocsAndPositionsEnum(DocsAndPositionsEnum in, CategoryListParams params) {
      super(in);
      encoder = params.createEncoder();
      decoder = encoder.createMatchingDecoder();
    }

    @Override
    public BytesRef getPayload() throws IOException {
      BytesRef payload = super.getPayload();
      if (payload == null) {
        return payload;
      } else {
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
        payloadOut.bytes = out;
        payloadOut.offset = 0;
        payloadOut.length = out.length;
        return payloadOut;
      }
    }
  }
}
