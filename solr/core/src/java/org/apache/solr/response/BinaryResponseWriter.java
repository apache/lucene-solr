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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocList;
import org.apache.solr.search.ReturnFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BinaryResponseWriter implements BinaryQueryResponseWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void write(OutputStream out, SolrQueryRequest req, SolrQueryResponse response) throws IOException {
    Resolver resolver = new Resolver(req, response.getReturnFields());
    if (req.getParams().getBool(CommonParams.OMIT_HEADER, false)) response.removeResponseHeader();
    new JavaBinCodec(resolver).setWritableDocFields(resolver).marshal(response.getValues(), out);
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
  public void init(NamedList args) {
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
      if (o instanceof ResultContext) {
        ReturnFields orig = returnFields;
        ResultContext res = (ResultContext)o;
        if(res.getReturnFields()!=null) {
          returnFields = res.getReturnFields();
        }
        writeResults(res, codec);
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
          LOG.warn("Error reading a field : " + o, e);
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
      List l = new ArrayList(3);
      l.add((long) ctx.getDocList().matches());
      l.add((long) ctx.getDocList().offset());
      
      Float maxScore = null;
      if (ctx.wantsScores()) {
        maxScore = ctx.getDocList().maxScore();
      }
      l.add(maxScore);
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
      Resolver resolver = new Resolver(req, rsp.getReturnFields());

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      new JavaBinCodec(resolver).setWritableDocFields(resolver).marshal(rsp.getValues(), out);

      InputStream in = out.toInputStream();
      return (NamedList<Object>) new JavaBinCodec(resolver).unmarshal(in);
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}
