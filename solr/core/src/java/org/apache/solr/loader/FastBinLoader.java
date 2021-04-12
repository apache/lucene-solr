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

package org.apache.solr.loader;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.common.util.DataEntry;
import org.apache.solr.common.util.FastJavaBinDecoder;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.RefCounted;

import static org.apache.solr.common.util.DataEntry.Type.ENTRY_ITER;
import static org.apache.solr.common.util.DataEntry.Type.KEYVAL_ITER;

public class FastBinLoader extends RequestHandlerBase {
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    RefCounted<IndexWriter> ref = req.getCore().getSolrCoreState().getIndexWriter(req.getCore());
    FastFieldReader.Ctx ctx= new FastFieldReader.Ctx();
    ctx.iw = ref.get();
    ctx.document = new Document();
    ctx.schema = req.getSchema();
    FieldsListener fieldsListener = new FieldsListener(ctx);
    try {
      DataEntry.FastDecoder decoder = new FastJavaBinDecoder();
      decoder
          .withInputStream(req.getContentStreams().iterator().next().getStream())
          .decode(e -> {
            if(e.type() == ENTRY_ITER) {// a list of docs
              e.listenContainer(null, de -> {
                if(de.type() == KEYVAL_ITER) {
                  ctx.document.clear();
                  de.listenContainer(null, fieldsListener);
                }
              });
            }// we ignore everything else
          });

    } finally {
      ref.decref();
    }

  }

  @Override
  public String getDescription() {
    return "FastBinLoader";
  }

  private static class FieldsListener implements DataEntry.EntryListener {
   FastFieldReader.Ctx ctx;

    public FieldsListener(FastFieldReader.Ctx ctx) {
      this.ctx = ctx;
    }

    @Override
    public void end(DataEntry e) {
      try {
        ctx.iw.addDocument(ctx.document);
        ctx.document.clear();

        ctx.packedBytes.clear();
      } catch (IOException ioException) {
        throw new RuntimeException(ioException);
      }
    }

    @Override
    public void entry(DataEntry e) {
      CharSequence name = e.name();
      SchemaField sf = null;
      if (name != null &&
          (sf = ctx.schema.getField(name.toString())) != null) {
        FastFieldReader ffr = sf.getFastFieldReader();
        if (ffr != null) {
          //this type is optimized to be indexed live
          ffr.addFields(ctx);
        } else {
          for (IndexableField f : sf.getType().createFields(sf, e.val())) {
            if (f != null) { // null fields are not added
              // HACK: workaround for SOLR-9809
              // even though at this point in the code we know the field is single valued and DV only
              // TrieField.createFields() may still return (usless) IndexableField instances that are not
              // NumericDocValuesField instances.
              //
              // once SOLR-9809 is resolved, we should be able to replace this conditional with...
              //    assert f instanceof NumericDocValuesField
             /* if (forInPlaceUpdate) {
                if (f instanceof NumericDocValuesField) {
                  doc.add(f);
                }
              } else {*/
                ctx.document.add(f);
              }/**/
            }
          }
        }
      }
      //else , not present in schema, ignore
  }
}
