/**
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
package org.apache.solr.request;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.schema.*;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class BinaryResponseWriter implements BinaryQueryResponseWriter {
  private static final Logger LOG = LoggerFactory.getLogger(BinaryResponseWriter.class);
  private static final Set<Class> KNOWN_TYPES = new HashSet<Class>();

  public void write(OutputStream out, SolrQueryRequest req, SolrQueryResponse response) throws IOException {
    Resolver resolver = new Resolver(req, response.getReturnFields());
    Boolean omitHeader = req.getParams().getBool(CommonParams.OMIT_HEADER);
    if (omitHeader != null && omitHeader) response.getValues().remove("responseHeader");
    JavaBinCodec codec = new JavaBinCodec(resolver);
    codec.marshal(response.getValues(), out);
  }

  public void write(Writer writer, SolrQueryRequest request, SolrQueryResponse response) throws IOException {
    throw new RuntimeException("This is a binary writer , Cannot write to a characterstream");
  }

  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return "application/octet-stream";
  }

  public void init(NamedList args) {
    /* NOOP */
  }

  private static class Resolver implements JavaBinCodec.ObjectResolver {
    private final SolrQueryRequest solrQueryRequest;
    private IndexSchema schema;
    private SolrIndexSearcher searcher;
    private final Set<String> returnFields;
    private final boolean includeScore;

    // transmit field values using FieldType.toObject()
    // rather than the String from FieldType.toExternal()
    boolean useFieldObjects = true;

    public Resolver(SolrQueryRequest req, Set<String> returnFields) {
      solrQueryRequest = req;
      this.includeScore = returnFields != null && returnFields.contains("score");

      if (returnFields != null) {
        if (returnFields.size() == 0 || (returnFields.size() == 1 && includeScore) || returnFields.contains("*")) {
          returnFields = null;  // null means return all stored fields
        }
      }
      this.returnFields = returnFields;
    }

    public Object resolve(Object o, JavaBinCodec codec) throws IOException {
      if (o instanceof DocList) {
        writeDocList((DocList) o, codec);
        return null; // null means we completely handled it
      }
      if (o instanceof SolrDocument) {
        SolrDocument solrDocument = (SolrDocument) o;
        codec.writeSolrDocument(solrDocument, returnFields);
        return null;
      }
      if (o instanceof Document) {
        return getDoc((Document) o);
      }

      return o;
    }

    public void writeDocList(DocList ids, JavaBinCodec codec) throws IOException {
      codec.writeTag(JavaBinCodec.SOLRDOCLST);
      List l = new ArrayList(3);
      l.add((long) ids.matches());
      l.add((long) ids.offset());
      Float maxScore = null;
      if (includeScore && ids.hasScores()) {
        maxScore = ids.maxScore();
      }
      l.add(maxScore);
      codec.writeArray(l);

      int sz = ids.size();
      codec.writeTag(JavaBinCodec.ARR, sz);
      if(searcher == null) searcher = solrQueryRequest.getSearcher();
      if(schema == null) schema = solrQueryRequest.getSchema(); 
      DocIterator iterator = ids.iterator();
      for (int i = 0; i < sz; i++) {
        int id = iterator.nextDoc();
        Document doc = searcher.doc(id, returnFields);

        SolrDocument sdoc = getDoc(doc);

        if (includeScore && ids.hasScores()) {
          sdoc.addField("score", iterator.score());
        }

        codec.writeSolrDocument(sdoc);
      }
    }


    public SolrDocument getDoc(Document doc) {
      SolrDocument solrDoc = new SolrDocument();
      for (Fieldable f : (List<Fieldable>) doc.getFields()) {
        String fieldName = f.name();
        if (returnFields != null && !returnFields.contains(fieldName)) continue;
        FieldType ft = schema.getFieldTypeNoEx(fieldName);
        Object val;
        if (ft == null) {  // handle fields not in the schema
          if (f.isBinary()) val = f.binaryValue();
          else val = f.stringValue();
        } else {
          try {
            if (useFieldObjects && KNOWN_TYPES.contains(ft.getClass())) {
              val = ft.toObject(f);
            } else {
              val = ft.toExternal(f);
            }
          } catch (Exception e) {
            // There is a chance of the underlying field not really matching the
            // actual field type . So ,it can throw exception
            LOG.warn("Error reading a field from document : " + solrDoc, e);
            //if it happens log it and continue
            continue;
          }
        }
        solrDoc.addField(fieldName, val);
      }
      return solrDoc;
    }

  }


  /**
   * TODO -- there may be a way to do this without marshal at all...
   *
   * @param req
   * @param rsp
   *
   * @return a response object equivalent to what you get from the XML/JSON/javabin parser. Documents become
   *         SolrDocuments, DocList becomes SolrDocumentList etc.
   *
   * @since solr 1.4
   */
  @SuppressWarnings("unchecked")
  public static NamedList<Object> getParsedResponse(SolrQueryRequest req, SolrQueryResponse rsp) {
    try {
      Resolver resolver = new Resolver(req, rsp.getReturnFields());

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      new JavaBinCodec(resolver).marshal(rsp.getValues(), out);

      InputStream in = new ByteArrayInputStream(out.toByteArray());
      return (NamedList<Object>) new JavaBinCodec(resolver).unmarshal(in);
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  static {
    KNOWN_TYPES.add(BoolField.class);
    KNOWN_TYPES.add(BCDIntField.class);
    KNOWN_TYPES.add(BCDLongField.class);
    KNOWN_TYPES.add(BCDStrField.class);
    KNOWN_TYPES.add(ByteField.class);
    KNOWN_TYPES.add(DateField.class);
    KNOWN_TYPES.add(DoubleField.class);
    KNOWN_TYPES.add(FloatField.class);
    KNOWN_TYPES.add(ShortField.class);
    KNOWN_TYPES.add(IntField.class);
    KNOWN_TYPES.add(LongField.class);
    KNOWN_TYPES.add(SortableLongField.class);
    KNOWN_TYPES.add(SortableIntField.class);
    KNOWN_TYPES.add(SortableFloatField.class);
    KNOWN_TYPES.add(SortableDoubleField.class);
    KNOWN_TYPES.add(StrField.class);
    KNOWN_TYPES.add(TextField.class);
    KNOWN_TYPES.add(TrieField.class);
    KNOWN_TYPES.add(BinaryField.class);
    // We do not add UUIDField because UUID object is not a supported type in JavaBinCodec
    // and if we write UUIDField.toObject, we wouldn't know how to handle it in the client side
  }
}
