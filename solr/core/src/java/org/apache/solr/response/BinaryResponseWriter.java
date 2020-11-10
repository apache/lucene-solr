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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.TotalHits;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utf8CharSequence;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocList;
import org.apache.solr.search.ReturnFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.util.ByteArrayUtf8CharSequence.convertCharSeq;


public class BinaryResponseWriter implements BinaryQueryResponseWriter {
//  public static boolean useUtf8CharSeq = true;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void write(OutputStream out, SolrQueryRequest req, SolrQueryResponse response) throws IOException {
    Resolver resolver = new Resolver(req, response.getReturnFields());
    if (req.getParams().getBool(CommonParams.OMIT_HEADER, false)) response.removeResponseHeader();
    try (JavaBinCodec jbc = new JavaBinCodec(resolver)) {
      jbc.setWritableDocFields(resolver).marshal(response.getValues(), out);
    }
  }

  private static void serialize(SolrQueryResponse response,Resolver resolver, String f) throws IOException {
    try (JavaBinCodec jbc = new JavaBinCodec(resolver); FileOutputStream fos = new FileOutputStream(f)) {
      jbc.setWritableDocFields(resolver).marshal(response.getValues(), fos);
      fos.flush();
    }

  }

  @Override
  public void write(Writer writer, SolrQueryRequest request, SolrQueryResponse response) throws IOException {
    throw new RuntimeException("This is a binary writer , Cannot write to a characterstream");
  }

  @Override
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return BinaryResponseParser.BINARY_CONTENT_TYPE;
  }

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    /* NOOP */
  }

  public static class Resolver implements JavaBinCodec.ObjectResolver , JavaBinCodec.WritableDocFields {
    protected final SolrQueryRequest solrQueryRequest;
    protected IndexSchema schema;
    protected ReturnFields returnFields;

    public Resolver(SolrQueryRequest req, ReturnFields returnFields) {
      solrQueryRequest = req;
      this.returnFields = returnFields;
    }

    @Override
    public Object resolve(Object o, JavaBinCodec codec) throws IOException {
      if (o instanceof StoredField) {
        CharSequence val = ((StoredField) o).getCharSequenceValue();
        if (val instanceof Utf8CharSequence) {
          codec.writeUTF8Str((Utf8CharSequence) val);
          return null;
        }
      }
      if (o instanceof ResultContext) {
        ReturnFields orig = returnFields;
        ResultContext res = (ResultContext)o;
        if(res.getReturnFields()!=null) {
          returnFields = res.getReturnFields();
        }
//        if (useUtf8CharSeq) {
        ResultContext.READASBYTES.set(fieldName -> {
          SchemaField fld = res.getRequest().getSchema().getFieldOrNull(fieldName);
          return fld != null && fld.getType().isUtf8Field();
        });

        try {
          writeResults(res, codec);
        } finally {
          ResultContext.READASBYTES.remove();
        }
        returnFields = orig;

        return null; // null means we completely handled it
      }
      if (o instanceof DocList) {
        ResultContext ctx = new BasicResultContext((DocList)o, returnFields, null, null, solrQueryRequest);
        writeResults(ctx, codec);
        return null; // null means we completely handled it
      }
      if( o instanceof IndexableField ) {
        if(schema == null) schema = solrQueryRequest.getSchema();

        IndexableField f = (IndexableField)o;
        SchemaField sf = schema.getFieldOrNull(f.name());
        try {
          o = DocsStreamer.getValue(sf, f);
        } catch (Exception e) {
          log.warn("Error reading a field : {}", o, e);
        }
      }
      return o;
    }

    @Override
    public boolean isWritable(String name) {
      return returnFields.wantsField(name);
    }

    @Override
    public boolean wantsAllFields() {
      return returnFields.wantsAllFields();
    }

    protected void writeResultsBody( ResultContext res, JavaBinCodec codec ) throws IOException {
      codec.writeTag(JavaBinCodec.ARR, res.getDocList().size());
      Iterator<SolrDocument> docStreamer = res.getProcessedDocuments();
      while (docStreamer.hasNext()) {
        SolrDocument doc = docStreamer.next();
        codec.writeSolrDocument(doc);
      }
    }

    public void writeResults(ResultContext ctx, JavaBinCodec codec) throws IOException {
      codec.writeTag(JavaBinCodec.SOLRDOCLST);
      List<Object> l = new ArrayList<>(4);
      l.add( ctx.getDocList().matches());
      l.add((long) ctx.getDocList().offset());
      
      Float maxScore = null;
      if (ctx.wantsScores()) {
        maxScore = ctx.getDocList().maxScore();
      }
      l.add(maxScore);
      l.add(ctx.getDocList().hitCountRelation() == TotalHits.Relation.EQUAL_TO);
      codec.writeArray(l);
      
      // this is a seprate function so that streaming responses can use just that part
      writeResultsBody( ctx, codec );
    }

  }


  /**
   * TODO -- there may be a way to do this without marshal at all...
   *
   * @return a response object equivalent to what you get from the XML/JSON/javabin parser. Documents become
   *         SolrDocuments, DocList becomes SolrDocumentList etc.
   *
   * @since solr 1.4
   */
  @SuppressWarnings("unchecked")
  public static NamedList<Object> getParsedResponse(SolrQueryRequest req, SolrQueryResponse rsp) {
    try {
      if (req.getParams().getBool(CommonParams.OMIT_HEADER, false)) {
        rsp.removeResponseHeader();
      }
      Resolver resolver = new Resolver(req, rsp.getReturnFields());

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (JavaBinCodec jbc = new JavaBinCodec(resolver)) {
        jbc.setWritableDocFields(resolver).marshal(rsp.getValues(), out);
      }

      InputStream in = out.toInputStream();
      try (JavaBinCodec jbc = new JavaBinCodec(resolver)) {
        return (NamedList<Object>) jbc.unmarshal(in);
      }
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  static class MaskCharSeqSolrDocument extends SolrDocument {
    /**
     * Get the value or collection of values for a given field.
     */
    @Override
    public Object getFieldValue(String name) {
      return convertCharSeq(_fields.get(name));
    }

    /**
     * Get a collection of values for a given field name
     */
    @SuppressWarnings("unchecked")
    @Override
    public Collection<Object> getFieldValues(String name) {
      Object v = _fields.get(name);
      if (v instanceof Collection) {
        return convertCharSeq((Collection<Object>) v);
      }
      if (v != null) {
        ArrayList<Object> arr = new ArrayList<>(1);
        arr.add(convertCharSeq(v));
        return arr;
      }
      return null;
    }

    @SuppressWarnings({"unchecked"})
    public Collection<Object> getRawFieldValues(String name) {
      Object v = _fields.get(name);
      if (v instanceof Collection) {
        return (Collection<Object>) v;
      }
      if (v != null) {
        ArrayList<Object> arr = new ArrayList<>(1);
        arr.add(v);
        return arr;
      }
      return null;
    }


    /**
     * Iterate of String-&gt;Object keys
     */
    @Override
    public Iterator<Entry<String, Object>> iterator() {
      Iterator<Entry<String, Object>> it = _fields.entrySet().iterator();
      return new Iterator<Entry<String, Object>>() {
        @Override
        public boolean hasNext() {
          return it.hasNext();
        }

        @Override
        @SuppressWarnings({"unchecked"})
        public Entry<String, Object> next() {
          return convertCharSeq(it.next());
        }
      };
    }


    ///////////////////////////////////////////////////////////////////
    // Get the field values
    ///////////////////////////////////////////////////////////////////

    /**
     * returns the first value for a field
     */
    @Override
    public Object getFirstValue(String name) {
      Object v = _fields.get(name);
      if (v == null || !(v instanceof Collection)) return convertCharSeq(v);
      @SuppressWarnings({"rawtypes"})
      Collection c = (Collection) v;
      if (c.size() > 0) {
        return convertCharSeq(c.iterator().next());
      }
      return null;
    }

    @Override
    public Object get(Object key) {
      return convertCharSeq(_fields.get(key));
    }

    public Object getRaw(Object key) {
      return _fields.get(key);
    }

    @Override
    public void forEach(Consumer<? super Entry<String, Object>> action) {
      super.forEach(action);
    }
  }

}
