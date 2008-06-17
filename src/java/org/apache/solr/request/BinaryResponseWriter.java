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
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.NamedListCodec;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TextField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.*;


public class BinaryResponseWriter implements BinaryQueryResponseWriter {
  public void write(OutputStream out, SolrQueryRequest req, SolrQueryResponse response) throws IOException {
    Resolver resolver = new Resolver(req, response.getReturnFields());
    NamedListCodec codec = new NamedListCodec(resolver);
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

  private static class Resolver implements NamedListCodec.ObjectResolver {
    private final IndexSchema schema;
    private final SolrIndexSearcher searcher;
    private final Set<String> returnFields;
    private final boolean includeScore;

    // transmit field values using FieldType.toObject()
    // rather than the String from FieldType.toExternal()
    boolean useFieldObjects = true;

    public Resolver(SolrQueryRequest req, Set<String> returnFields) {
      this.schema = req.getSchema();
      this.searcher = req.getSearcher();
      this.includeScore = returnFields!=null && returnFields.contains("score");

      if (returnFields != null) {
       if (returnFields.size() == 0 || (returnFields.size() == 1 && includeScore) || returnFields.contains("*")) {
          returnFields = null;  // null means return all stored fields
        }
      }
      this.returnFields = returnFields;
    }

    public Object resolve(Object o, NamedListCodec codec) throws IOException {
      if (o instanceof DocList) {
        writeDocList((DocList) o, codec);
        return null; // null means we completely handled it
      }
      if (o instanceof SolrDocument) {
        SolrDocument solrDocument = (SolrDocument) o;
        codec.writeSolrDocument(solrDocument,returnFields);
        return null;
      }
      if (o instanceof Document) {
        return getDoc((Document) o);
      }

      return o;
    }

    public void writeDocList(DocList ids, NamedListCodec codec) throws IOException {
      codec.writeTag(NamedListCodec.SOLRDOCLST);
      List l = new ArrayList(3);
      l.add((long)ids.matches());
      l.add((long)ids.offset());
      Float maxScore = null;
      if (includeScore && ids.hasScores()) {
        maxScore = ids.maxScore();
      }
      l.add(maxScore);
      codec.writeArray(l);

      int sz = ids.size();
      codec.writeTag(NamedListCodec.ARR, sz);

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
      for (Fieldable f : (List<Fieldable>)doc.getFields()) {
        String fieldName = f.name();
        if (returnFields!=null && !returnFields.contains(fieldName)) continue;
        FieldType ft = schema.getFieldTypeNoEx(fieldName);
        Object val;
        if (ft==null) {  // handle fields not in the schema
          if (f.isBinary()) val = f.binaryValue();
          else val = f.stringValue();
        } else {
          val = useFieldObjects ? ft.toObject(f) : ft.toExternal(f);
        }
        solrDoc.addField(fieldName, val);
      }
      return solrDoc;
    }

  }
  
}
