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
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.TransformContext;
import org.apache.solr.schema.BinaryField;
import org.apache.solr.schema.BoolField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
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
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrReturnFields;

/**
 * This streams SolrDocuments from a DocList and applies transformer
 */
public class DocsStreamer implements Iterator<SolrDocument> {
  private static final Set<Class> KNOWN_TYPES = new HashSet<>();
  private final DocList docs;

  private SolrIndexSearcher searcher;
  private final IndexSchema schema;
  private DocTransformer transformer;
  private DocIterator docIterator;
  private boolean onlyPseudoFields;
  private Set<String> fnames;
  private TransformContext context;
  private Set<String> dvFieldsToReturn;
  private int idx = -1;

  public DocsStreamer(DocList docList, Query query, SolrQueryRequest req, ReturnFields returnFields) {
    this.docs = docList;
    this.schema = req.getSchema();
    searcher = req.getSearcher();
    transformer = returnFields.getTransformer();
    docIterator = docList.iterator();
    context = new TransformContext();
    context.query = query;
    context.wantsScores = returnFields.wantsScore() && docList.hasScores();
    context.req = req;
    context.searcher = searcher;
    context.iterator = docIterator;
    fnames = returnFields.getLuceneFieldNames();
    onlyPseudoFields = (fnames == null && !returnFields.wantsAllFields() && !returnFields.hasPatternMatching())
        || (fnames != null && fnames.size() == 1 && SolrReturnFields.SCORE.equals(fnames.iterator().next()));

    // add non-stored DV fields that may have been requested
    if (returnFields.wantsAllFields()) {
      // check whether there are no additional fields
      Set<String> fieldNames = returnFields.getLuceneFieldNames(true);
      if (fieldNames == null || fieldNames.isEmpty()) {
        dvFieldsToReturn = searcher.getNonStoredDVs(true);
      } else {
        dvFieldsToReturn = new HashSet<>(searcher.getNonStoredDVs(true)); // copy
        // add all requested fields that may be useDocValuesAsStored=false
        for (String fl : fieldNames) {
          if (searcher.getNonStoredDVs(false).contains(fl)) {
            dvFieldsToReturn.add(fl);
          }
        }
      }
    } else {
      if (returnFields.hasPatternMatching()) {
        for (String s : searcher.getNonStoredDVs(true)) {
          if (returnFields.wantsField(s)) {
            if (null == dvFieldsToReturn) {
              dvFieldsToReturn = new HashSet<>();
            }
            dvFieldsToReturn.add(s);
          }
        }
      } else if (fnames != null) {
        dvFieldsToReturn = new HashSet<>(fnames); // copy
        // here we get all non-stored dv fields because even if a user has set
        // useDocValuesAsStored=false in schema, he may have requested a field
        // explicitly using the fl parameter
        dvFieldsToReturn.retainAll(searcher.getNonStoredDVs(false));
      }
    }

    if (transformer != null) transformer.setContext(context);
  }

  public boolean hasScores() {
    return context.wantsScores;
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

    if (onlyPseudoFields) {
      // no need to get stored fields of the document, see SOLR-5968
      sdoc = new SolrDocument();
    } else {
      try {
        Document doc = searcher.doc(id, fnames);
        sdoc = getDoc(doc, schema);

        // decorate the document with non-stored docValues fields
        if (dvFieldsToReturn != null) {
          searcher.decorateDocValueFields(sdoc, id, dvFieldsToReturn);
        }
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error reading document with docId " + id, e);
      }
    }

    if (transformer != null) {
      try {
        transformer.transform(sdoc, id);
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error applying transformer", e);
      }
    }
    return sdoc;

  }

  public static SolrDocument getDoc(Document doc, final IndexSchema schema) {
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
    // We do not add UUIDField because UUID object is not a supported type in JavaBinCodec
    // and if we write UUIDField.toObject, we wouldn't know how to handle it in the client side
  }

}
