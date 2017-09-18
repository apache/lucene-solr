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
package org.apache.solr.response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.schema.BinaryField;
import org.apache.solr.schema.BoolField;
import org.apache.solr.schema.DatePointField;
import org.apache.solr.schema.DoublePointField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.FloatPointField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IntPointField;
import org.apache.solr.schema.LongPointField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.TextField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.schema.TrieDoubleField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.schema.TrieFloatField;
import org.apache.solr.schema.TrieIntField;
import org.apache.solr.schema.TrieLongField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrDocumentFetcher;

/**
 * This streams SolrDocuments from a DocList and applies transformer
 */
public class DocsStreamer implements Iterator<SolrDocument> {
  public static final Set<Class> KNOWN_TYPES = new HashSet<>();

  private final org.apache.solr.response.ResultContext rctx;
  private final SolrDocumentFetcher docFetcher; // a collaborator of SolrIndexSearcher
  private final DocList docs;

  private final DocTransformer transformer;
  private final DocIterator docIterator;

  private final RetrieveFieldsOptimizer retrieveFieldsOptimizer;

  private int idx = -1;

  public DocsStreamer(ResultContext rctx) {
    this.rctx = rctx;
    this.docs = rctx.getDocList();
    transformer = rctx.getReturnFields().getTransformer();
    docIterator = this.docs.iterator();
    docFetcher = rctx.getSearcher().getDocFetcher();

    retrieveFieldsOptimizer = RetrieveFieldsOptimizer.create(docFetcher, rctx.getReturnFields());
    retrieveFieldsOptimizer.optimize(docFetcher);
    if (transformer != null) transformer.setContext(rctx);
  }

  public int currentIndex() {
    return idx;
  }

  public boolean hasNext() {
    return docIterator.hasNext();
  }

  public SolrDocument next() {
    int id = docIterator.nextDoc();
    idx++;
    SolrDocument sdoc = null;

    try {
      if (retrieveFieldsOptimizer.returnStoredFields()) {
        Document doc = docFetcher.doc(id, retrieveFieldsOptimizer.getStoredFields());
        // make sure to use the schema from the searcher and not the request (cross-core)
        sdoc = convertLuceneDocToSolrDoc(doc, rctx.getSearcher().getSchema());
      } else {
        // no need to get stored fields of the document, see SOLR-5968
        sdoc = new SolrDocument();
      }

      // decorate the document with non-stored docValues fields
      if (retrieveFieldsOptimizer.returnDVFields()) {
        docFetcher.decorateDocValueFields(sdoc, id, retrieveFieldsOptimizer.getDvFields());
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error reading document with docId " + id, e);
    }

    if (transformer != null) {
      boolean doScore = rctx.wantsScores();
      try {
        if (doScore) {
          transformer.transform(sdoc, id, docIterator.score());
        } else {
          transformer.transform(sdoc, id);
        }
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error applying transformer", e);
      }
    }
    return sdoc;

  }

  // TODO move to SolrDocumentFetcher ?  Refactor to also call docFetcher.decorateDocValueFields(...) ?
  public static SolrDocument convertLuceneDocToSolrDoc(Document doc, final IndexSchema schema) {
    SolrDocument out = new SolrDocument();
    for (IndexableField f : doc.getFields()) {
      // Make sure multivalued fields are represented as lists
      Object existing = out.get(f.name());
      if (existing == null) {
        SchemaField sf = schema.getFieldOrNull(f.name());
        if (sf != null && sf.multiValued()) {
          List<Object> vals = new ArrayList<>();
          vals.add(f);
          out.setField(f.name(), vals);
        } else {
          out.setField(f.name(), f);
        }
      } else {
        out.addField(f.name(), f);
      }
    }
    return out;
  }

  @Override
  public void remove() { //do nothing
  }

  public static Object getValue(SchemaField sf, IndexableField f) {
    FieldType ft = null;
    if (sf != null) ft = sf.getType();

    if (ft == null) {  // handle fields not in the schema
      BytesRef bytesRef = f.binaryValue();
      if (bytesRef != null) {
        if (bytesRef.offset == 0 && bytesRef.length == bytesRef.bytes.length) {
          return bytesRef.bytes;
        } else {
          final byte[] bytes = new byte[bytesRef.length];
          System.arraycopy(bytesRef.bytes, bytesRef.offset, bytes, 0, bytesRef.length);
          return bytes;
        }
      } else return f.stringValue();
    } else {
      if (KNOWN_TYPES.contains(ft.getClass())) {
        return ft.toObject(f);
      } else {
        return ft.toExternal(f);
      }
    }
  }


  static {
    KNOWN_TYPES.add(BoolField.class);
    KNOWN_TYPES.add(StrField.class);
    KNOWN_TYPES.add(TextField.class);
    KNOWN_TYPES.add(TrieField.class);
    KNOWN_TYPES.add(TrieIntField.class);
    KNOWN_TYPES.add(TrieLongField.class);
    KNOWN_TYPES.add(TrieFloatField.class);
    KNOWN_TYPES.add(TrieDoubleField.class);
    KNOWN_TYPES.add(TrieDateField.class);
    KNOWN_TYPES.add(BinaryField.class);
    KNOWN_TYPES.add(IntPointField.class);
    KNOWN_TYPES.add(LongPointField.class);
    KNOWN_TYPES.add(DoublePointField.class);
    KNOWN_TYPES.add(FloatPointField.class);
    KNOWN_TYPES.add(DatePointField.class);
    // We do not add UUIDField because UUID object is not a supported type in JavaBinCodec
    // and if we write UUIDField.toObject, we wouldn't know how to handle it in the client side
  }

}
