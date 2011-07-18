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
package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.util.ContentStream;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A RequestWriter which writes requests in the javabin format
 *
 *
 * @see org.apache.solr.client.solrj.request.RequestWriter
 * @since solr 1.4
 */
public class BinaryRequestWriter extends RequestWriter {

  @Override
  public Collection<ContentStream> getContentStreams(SolrRequest req) throws IOException {
    if (req instanceof UpdateRequest) {
      UpdateRequest updateRequest = (UpdateRequest) req;
      if (isNull(updateRequest.getDocuments()) &&
              isNull(updateRequest.getDeleteById()) &&
              isNull(updateRequest.getDeleteQuery())
              && (updateRequest.getDocIterator() == null) ) {
        return null;
      }
      List<ContentStream> l = new ArrayList<ContentStream>();
      l.add(new LazyContentStream(updateRequest));
      return l;
    } else {
      return super.getContentStreams(req);
    }

  }


  @Override
  public String getUpdateContentType() {
    return "application/octet-stream";
  }

  @Override
  public ContentStream getContentStream(final UpdateRequest request) throws IOException {
    final BAOS baos = new BAOS();
      new JavaBinUpdateRequestCodec().marshal(request, baos);
    return new ContentStream() {
      public String getName() {
        return null;
      }

      public String getSourceInfo() {
        return "javabin";
      }

      public String getContentType() {
        return "application/octet-stream";
      }

      public Long getSize() // size if we know it, otherwise null
      {
        return new Long(baos.size());
      }

      public InputStream getStream() throws IOException {
        return new ByteArrayInputStream(baos.getbuf(), 0, baos.size());
      }

      public Reader getReader() throws IOException {
        throw new RuntimeException("No reader available . this is a binarystream");
      }
    };
  }


  @Override
  public void write(SolrRequest request, OutputStream os) throws IOException {
    if (request instanceof UpdateRequest) {
      UpdateRequest updateRequest = (UpdateRequest) request;
      new JavaBinUpdateRequestCodec().marshal(updateRequest, os);
    } 

  }/*
   * A hack to get access to the protected internal buffer and avoid an additional copy 
   */
  class BAOS extends ByteArrayOutputStream {
    byte[] getbuf() {
      return super.buf;
    }
  }

  @Override
  public String getPath(SolrRequest req) {
    if (req instanceof UpdateRequest) {
      return "/update/javabin";
    } else {
      return req.getPath();
    }
  }
}
