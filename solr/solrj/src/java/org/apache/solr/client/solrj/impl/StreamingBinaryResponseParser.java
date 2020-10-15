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
package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.solr.client.solrj.FastStreamingDocsCallback;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.DataEntry;
import org.apache.solr.common.util.DataEntry.EntryListener;
import org.apache.solr.common.util.DataInputInputStream;
import org.apache.solr.common.util.FastJavaBinDecoder;
import org.apache.solr.common.util.FastJavaBinDecoder.EntryImpl;
import org.apache.solr.common.util.FastJavaBinDecoder.Tag;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;

/**
 * A BinaryResponseParser that sends callback events rather then build
 * a large response 
 * 
 *
 * @since solr 4.0
 */
public class StreamingBinaryResponseParser extends BinaryResponseParser {
  public final StreamingResponseCallback callback;
  public final FastStreamingDocsCallback fastCallback;

  public StreamingBinaryResponseParser(StreamingResponseCallback cb) {
    this.callback = cb;
    fastCallback = null;
  }

  public StreamingBinaryResponseParser(FastStreamingDocsCallback cb) {
    this.fastCallback = cb;
    this.callback = null;

  }
  
  @Override
  public NamedList<Object> processResponse(InputStream body, String encoding) {
    if (callback != null) {
      return streamDocs(body);
    } else {
      try {
        return fastStreamDocs(body, fastCallback);
      } catch (IOException e) {
        throw new RuntimeException("Unable to parse", e);
      }
    }

  }

  private NamedList<Object> fastStreamDocs(InputStream body, FastStreamingDocsCallback fastCallback) throws IOException {

    fieldListener = new EntryListener() {
      @Override
      public void entry(DataEntry field) {
        if (((EntryImpl) field).getTag() == Tag._SOLRDOC) {
          field.listenContainer(fastCallback.startChildDoc(field.ctx()), fieldListener);
        } else {
          fastCallback.field(field,  field.ctx());
        }
      }

      @Override
      public void end(DataEntry e) {
        fastCallback.endDoc(((EntryImpl) e).ctx);
      }
    };
    docListener = e -> {
      EntryImpl entry = (EntryImpl) e;
      if (entry.getTag() == Tag._SOLRDOC) {//this is a doc
        entry.listenContainer(fastCallback.startDoc(entry.ctx()), fieldListener);
      }
    };
    new FastJavaBinDecoder()
        .withInputStream(body)
        .decode(new EntryListener() {
          @Override
          public void entry(DataEntry e) {
            EntryImpl entry = (EntryImpl) e;
            if( !entry.type().isContainer) return;
            if (e.isKeyValEntry() && entry.getTag() == Tag._SOLRDOCLST) {
              @SuppressWarnings({"rawtypes"})
              List l = (List) e.metadata();
              e.listenContainer(fastCallback.initDocList(
                  (Long) l.get(0),
                  (Long) l.get(1),
                  (Float) l.get(2)),
                  docListener);
            } else {
              e.listenContainer(null, this);
            }
          }
        });
    return null;
  }


  private EntryListener fieldListener;
  private EntryListener docListener;


  @SuppressWarnings({"unchecked"})
  private NamedList<Object> streamDocs(InputStream body) {
    try (JavaBinCodec codec = new JavaBinCodec() {

      private int nestedLevel;

      @Override
      public SolrDocument readSolrDocument(DataInputInputStream dis) throws IOException {
        nestedLevel++;
        SolrDocument doc = super.readSolrDocument(dis);
        nestedLevel--;
        if (nestedLevel == 0) {
          // parent document
          callback.streamSolrDocument(doc);
          return null;
        } else {
          // child document
          return doc;
        }
      }

      @Override
      public SolrDocumentList readSolrDocumentList(DataInputInputStream dis) throws IOException {
        SolrDocumentList solrDocs = new SolrDocumentList();
        @SuppressWarnings({"rawtypes"})
        List list = (List) readVal(dis);
        solrDocs.setNumFound((Long) list.get(0));
        solrDocs.setStart((Long) list.get(1));
        solrDocs.setMaxScore((Float) list.get(2));

        callback.streamDocListInfo(
            solrDocs.getNumFound(),
            solrDocs.getStart(),
            solrDocs.getMaxScore());

        // Read the Array
        tagByte = dis.readByte();
        if ((tagByte >>> 5) != (ARR >>> 5)) {
          throw new RuntimeException("doclist must have an array");
        }
        int sz = readSize(dis);
        for (int i = 0; i < sz; i++) {
          // must be a SolrDocument
          readVal(dis);
        }
        return solrDocs;
      }
    };) {

      return (NamedList<Object>) codec.unmarshal(body);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "parsing error", e);
    }
  }
}
