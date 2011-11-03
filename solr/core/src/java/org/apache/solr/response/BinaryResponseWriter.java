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
package org.apache.solr.response;

import java.io.*;
import java.util.*;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.TransformContext;
import org.apache.solr.schema.*;
import org.apache.solr.search.DocList;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BinaryResponseWriter implements BinaryQueryResponseWriter {
  private static final Logger LOG = LoggerFactory.getLogger(BinaryResponseWriter.class);
  public static final Set<Class> KNOWN_TYPES = new HashSet<Class>();

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

  public static class Resolver implements JavaBinCodec.ObjectResolver {
    protected final SolrQueryRequest solrQueryRequest;
    protected IndexSchema schema;
    protected SolrIndexSearcher searcher;
    protected final ReturnFields returnFields;

    // transmit field values using FieldType.toObject()
    // rather than the String from FieldType.toExternal()
    boolean useFieldObjects = true;

    public Resolver(SolrQueryRequest req, ReturnFields returnFields) {
      solrQueryRequest = req;
      this.returnFields = returnFields;
    }

    public Object resolve(Object o, JavaBinCodec codec) throws IOException {
      if (o instanceof ResultContext) {
        writeResults((ResultContext) o, codec);
        return null; // null means we completely handled it
      }
      if (o instanceof DocList) {
        ResultContext ctx = new ResultContext();
        ctx.docs = (DocList) o;
        writeResults(ctx, codec);
        return null; // null means we completely handled it
      }
      if( o instanceof IndexableField ) {
        if(schema == null) schema = solrQueryRequest.getSchema(); 
        
        IndexableField f = (IndexableField)o;
        SchemaField sf = schema.getFieldOrNull(f.name());
        try {
          o = getValue(sf, f);
        } 
        catch (Exception e) {
          LOG.warn("Error reading a field : " + o, e);
        }
      }
      if (o instanceof SolrDocument) {
        // Remove any fields that were not requested.
        // This typically happens when distributed search adds 
        // extra fields to an internal request
        SolrDocument doc = (SolrDocument)o;
        Iterator<Map.Entry<String, Object>> i = doc.iterator();
        while ( i.hasNext() ) {
          String fname = i.next().getKey();
          if ( !returnFields.wantsField( fname ) ) {
            i.remove();
          }
        }
        return doc;
      }
      return o;
    }

    protected void writeResultsBody( ResultContext res, JavaBinCodec codec ) throws IOException 
    {
      DocList ids = res.docs;
      TransformContext context = new TransformContext();
      context.query = res.query;
      context.wantsScores = returnFields.wantsScore() && ids.hasScores();
      
      int sz = ids.size();
      codec.writeTag(JavaBinCodec.ARR, sz);
      if(searcher == null) searcher = solrQueryRequest.getSearcher();
      if(schema == null) schema = solrQueryRequest.getSchema(); 
      
      context.searcher = searcher;
      DocTransformer transformer = returnFields.getTransformer();
      if( transformer != null ) {
        transformer.setContext( context );
      }
      
      Set<String> fnames = returnFields.getLuceneFieldNames();
      context.iterator = ids.iterator();
      for (int i = 0; i < sz; i++) {
        int id = context.iterator.nextDoc();
        Document doc = searcher.doc(id, fnames);
        SolrDocument sdoc = getDoc(doc);
        if( transformer != null ) {
          transformer.transform(sdoc, id );
        }
        codec.writeSolrDocument(sdoc);
      }
      if( transformer != null ) {
        transformer.setContext( null );
      }
    }
    
    public void writeResults(ResultContext ctx, JavaBinCodec codec) throws IOException {
      codec.writeTag(JavaBinCodec.SOLRDOCLST);
      boolean wantsScores = returnFields.wantsScore() && ctx.docs.hasScores();
      List l = new ArrayList(3);
      l.add((long) ctx.docs.matches());
      l.add((long) ctx.docs.offset());
      
      Float maxScore = null;
      if (wantsScores) {
        maxScore = ctx.docs.maxScore();
      }
      l.add(maxScore);
      codec.writeArray(l);
      
      // this is a seprate function so that streaming responses can use just that part
      writeResultsBody( ctx, codec );
    }

    public SolrDocument getDoc(Document doc) {
      SolrDocument solrDoc = new SolrDocument();
      for (IndexableField f : doc) {
        String fieldName = f.name();
        if( !returnFields.wantsField(fieldName) ) 
          continue;
        
        SchemaField sf = schema.getFieldOrNull(fieldName);
        Object val = null;
        try {
          val = getValue(sf,f);
        } catch (Exception e) {
          // There is a chance of the underlying field not really matching the
          // actual field type . So ,it can throw exception
          LOG.warn("Error reading a field from document : " + solrDoc, e);
          //if it happens log it and continue
          continue;
        }
          
        if(sf != null && sf.multiValued() && !solrDoc.containsKey(fieldName)){
          ArrayList l = new ArrayList();
          l.add(val);
          solrDoc.addField(fieldName, l);
        } else {
          solrDoc.addField(fieldName, val);
        }
      }
      return solrDoc;
    }
    
    public Object getValue(SchemaField sf, IndexableField f) throws Exception {
      FieldType ft = null;
      if(sf != null) ft =sf.getType();
      
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
        if (useFieldObjects && KNOWN_TYPES.contains(ft.getClass())) {
          return ft.toObject(f);
        } else {
          return ft.toExternal(f);
        }
      }
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
