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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

/**
 * A {@link FilterAtomicReader} for migrating a facets index which encodes
 * category ordinals in a payload to {@link BinaryDocValues}. To migrate the index,
 * you should build a mapping from a field (String) to term ({@link Term}),
 * which denotes under which BinaryDocValues field to put the data encoded in the
 * matching term's payload. You can follow the code example below to migrate an
 * existing index:
 * 
 * <pre class="prettyprint">
 * // Add the index and migrate payload to DocValues on the go
 * DirectoryReader reader = DirectoryReader.open(oldDir);
 * IndexWriterConfig conf = new IndexWriterConfig(VER, ANALYZER);
 * IndexWriter writer = new IndexWriter(newDir, conf);
 * List&lt;AtomicReaderContext&gt; leaves = reader.leaves();
 * AtomicReader wrappedLeaves[] = new AtomicReader[leaves.size()];
 * for (int i = 0; i &lt; leaves.size(); i++) {
 *   wrappedLeaves[i] = new FacetPayloadMigrationReader(leaves.get(i).reader(),
 *       fieldTerms);
 * }
 * writer.addIndexes(new MultiReader(wrappedLeaves));
 * writer.commit();
 * </pre>
 * 
 * <p>
 * <b>NOTE:</b> to build the field-to-term map you can use
 * {@link #buildFieldTermsMap(Directory, FacetIndexingParams)}, as long as the
 * index to migrate contains the ordinals payload under
 * {@link #PAYLOAD_TERM_TEXT}.
 * 
 * @lucene.experimental
 */
public class FacetsPayloadMigrationReader extends FilterAtomicReader {  

  private class PayloadMigratingDocValues extends DocValues {

    private final DocsAndPositionsEnum dpe;
    
    public PayloadMigratingDocValues(DocsAndPositionsEnum dpe) {
      this.dpe = dpe;
    }

    @Override
    protected Source loadDirectSource() throws IOException {
      return new PayloadMigratingSource(getType(), dpe);
    }

    @Override
    protected Source loadSource() throws IOException {
      throw new UnsupportedOperationException("in-memory Source is not supported by this reader");
    }

    @Override
    public Type getType() {
      return Type.BYTES_VAR_STRAIGHT;
    }
    
  }
  
  private class PayloadMigratingSource extends Source {

    private final DocsAndPositionsEnum dpe;
    private int curDocID;
    
    protected PayloadMigratingSource(Type type, DocsAndPositionsEnum dpe) {
      super(type);
      this.dpe = dpe;
      if (dpe == null) {
        curDocID = DocIdSetIterator.NO_MORE_DOCS;
      } else {
        try {
          curDocID = dpe.nextDoc();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
    
    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      if (curDocID > docID) {
        // document does not exist
        ref.length = 0;
        return ref;
      }
      
      try {
        if (curDocID < docID) {
          curDocID = dpe.advance(docID);
          if (curDocID != docID) { // requested document does not have a payload
            ref.length = 0;
            return ref;
          }
        }
        
        // we're on the document
        dpe.nextPosition();
        ref.copyBytes(dpe.getPayload());
        return ref;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
  }
  
  /** The {@link Term} text of the ordinals payload. */
  public static final String PAYLOAD_TERM_TEXT = "$fulltree$";

  /**
   * A utility method for building the field-to-Term map, given the
   * {@link FacetIndexingParams} and the directory of the index to migrate. The
   * map that will be built will correspond to partitions as well as multiple
   * {@link CategoryListParams}.
   * <p>
   * <b>NOTE:</b> since {@link CategoryListParams} no longer define a
   * {@link Term}, this method assumes that the term used by the different
   * {@link CategoryListParams} is {@link #PAYLOAD_TERM_TEXT}. If this is not
   * the case, then you should build the map yourself, using the terms in your
   * index.
   */
  public static Map<String,Term> buildFieldTermsMap(Directory dir, FacetIndexingParams fip) throws IOException {
    // only add field-Term mapping that will actually have DocValues in the end.
    // therefore traverse the index terms and add what exists. this pertains to
    // multiple CLPs, as well as partitions
    DirectoryReader reader = DirectoryReader.open(dir);
    final Map<String,Term> fieldTerms = new HashMap<String,Term>();
    for (AtomicReaderContext context : reader.leaves()) {
      for (CategoryListParams clp : fip.getAllCategoryListParams()) {
        Terms terms = context.reader().terms(clp.field);
        if (terms != null) {
          TermsEnum te = terms.iterator(null);
          BytesRef termBytes = null;
          while ((termBytes = te.next()) != null) {
            String term = termBytes.utf8ToString();
            if (term.startsWith(PAYLOAD_TERM_TEXT )) {
              if (term.equals(PAYLOAD_TERM_TEXT)) {
                fieldTerms.put(clp.field, new Term(clp.field, term));
              } else {
                fieldTerms.put(clp.field + term.substring(PAYLOAD_TERM_TEXT.length()), new Term(clp.field, term));
              }
            }
          }
        }        
      }
    }
    reader.close();
    return fieldTerms;
  }
  
  private final Map<String,Term> fieldTerms;
  
  /**
   * Wraps an {@link AtomicReader} and migrates the payload to {@link DocValues}
   * fields by using the given mapping.
   */
  public FacetsPayloadMigrationReader(AtomicReader in, Map<String,Term> fieldTerms) {
    super(in);
    this.fieldTerms = fieldTerms;
  }
  
  @Override
  public DocValues docValues(String field) throws IOException {
    Term term = fieldTerms.get(field);
    if (term == null) {
      return super.docValues(field);
    } else {
      DocsAndPositionsEnum dpe = null;
      Fields fields = fields();
      if (fields != null) {
        Terms terms = fields.terms(term.field());
        if (terms != null) {
          TermsEnum te = terms.iterator(null); // no use for reusing
          if (te.seekExact(term.bytes(), true)) {
            // we're not expected to be called for deleted documents
            dpe = te.docsAndPositions(null, null, DocsAndPositionsEnum.FLAG_PAYLOADS);
          }
        }
      }
      // we shouldn't return null, even if the term does not exist or has no
      // payloads, since we already marked the field as having DocValues.
      return new PayloadMigratingDocValues(dpe);
    }
  }

  @Override
  public FieldInfos getFieldInfos() {
    FieldInfos innerInfos = super.getFieldInfos();
    ArrayList<FieldInfo> infos = new ArrayList<FieldInfo>(innerInfos.size());
    // if there are partitions, then the source index contains one field for all their terms
    // while with DocValues, we simulate that by multiple fields.
    HashSet<String> leftoverFields = new HashSet<String>(fieldTerms.keySet());
    int number = -1;
    for (FieldInfo info : innerInfos) {
      if (fieldTerms.containsKey(info.name)) {
        // mark this field as having a DocValues
        infos.add(new FieldInfo(info.name, true, info.number,
            info.hasVectors(), info.omitsNorms(), info.hasPayloads(),
            info.getIndexOptions(), Type.BYTES_VAR_STRAIGHT,
            info.getNormType(), info.attributes()));
        leftoverFields.remove(info.name);
      } else {
        infos.add(info);
      }
      number = Math.max(number, info.number);
    }
    for (String field : leftoverFields) {
      infos.add(new FieldInfo(field, false, ++number, false, false, false,
          null, Type.BYTES_VAR_STRAIGHT, null, null));
    }
    return new FieldInfos(infos.toArray(new FieldInfo[infos.size()]));
  }
  
}
