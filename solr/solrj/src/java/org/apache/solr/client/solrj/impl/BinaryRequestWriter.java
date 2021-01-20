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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.util.ContentStream;

import static org.apache.solr.common.params.CommonParams.JAVABIN_MIME;

/**
 * A RequestWriter which writes requests in the javabin format
 *
 *
 * @see org.apache.solr.client.solrj.request.RequestWriter
 * @since solr 1.4
 */
public class BinaryRequestWriter extends RequestWriter {

  @Override
  public ContentWriter getContentWriter(@SuppressWarnings({"rawtypes"})SolrRequest req) {
    if (req instanceof UpdateRequest) {
      UpdateRequest updateRequest = (UpdateRequest) req;
      if (isEmpty(updateRequest)) return null;
      return new ContentWriter() {
        @Override
        public void write(OutputStream os) throws IOException {
          new JavaBinUpdateRequestCodec().marshal(updateRequest, os);
        }

        @Override
        public String getContentType() {
          return JAVABIN_MIME;
        }
      };
    } else {
      return req.getContentWriter(JAVABIN_MIME);
    }
  }

  @Override
  public Collection<ContentStream> getContentStreams(@SuppressWarnings({"rawtypes"})SolrRequest req) throws IOException {
    if (req instanceof UpdateRequest) {
      UpdateRequest updateRequest = (UpdateRequest) req;
      if (isEmpty(updateRequest) ) return null;
      throw new RuntimeException("This Should not happen");
    } else {
      return super.getContentStreams(req);
    }
  }


  @Override
  public String getUpdateContentType() {
    return JAVABIN_MIME;
  }

  @Override
  public void write(@SuppressWarnings({"rawtypes"})SolrRequest request, OutputStream os) throws IOException {
    if (request instanceof UpdateRequest) {
      UpdateRequest updateRequest = (UpdateRequest) request;
      new JavaBinUpdateRequestCodec().marshal(updateRequest, os);
    }
  }
  
  /*
   * A hack to get access to the protected internal buffer and avoid an additional copy
   */
  public static class BAOS extends ByteArrayOutputStream {
    public byte[] getbuf() {
      return super.buf;
    }
  }
}
