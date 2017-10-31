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
package org.apache.solr.client.solrj.request;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;

/**
 * A RequestWriter is used to write requests to Solr.
 * <p>
 * A subclass can override the methods in this class to supply a custom format in which a request can be sent.
 *
 *
 * @since solr 1.4
 */
public class RequestWriter {
  public static final Charset UTF_8 = StandardCharsets.UTF_8;


  public interface ContentWriter {

    void write(OutputStream os) throws IOException;

    String getContentType();
  }

  /**
   * Use this to do a push writing instead of pull. If this method returns null
   * {@link org.apache.solr.client.solrj.request.RequestWriter#getContentStream(UpdateRequest)} is
   * invoked to do a pull write.
   */
  public ContentWriter getContentWriter(SolrRequest req) {
    return null;
  }

  public Collection<ContentStream> getContentStreams(SolrRequest req) throws IOException {
    if (req instanceof UpdateRequest) {
      UpdateRequest updateRequest = (UpdateRequest) req;
      if (isEmpty(updateRequest)) return null;
      List<ContentStream> l = new ArrayList<>();
      l.add(new LazyContentStream(updateRequest));
      return l;
    }
    return req.getContentStreams();
  }

  protected boolean isEmpty(UpdateRequest updateRequest) {
    return isNull(updateRequest.getDocuments()) &&
            isNull(updateRequest.getDeleteByIdMap()) &&
            isNull(updateRequest.getDeleteQuery()) &&
            updateRequest.getDocIterator() == null;
  }

  public String getPath(SolrRequest req) {
    return req.getPath();
  }

  public ContentStream getContentStream(UpdateRequest req) throws IOException {
    return new ContentStreamBase.StringStream(req.getXML());
  }

  public void write(SolrRequest request, OutputStream os) throws IOException {
    if (request instanceof UpdateRequest) {
      UpdateRequest updateRequest = (UpdateRequest) request;
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, UTF_8));
      updateRequest.writeXML(writer);
      writer.flush();
    }
  }

  public String getUpdateContentType() {
    return ClientUtils.TEXT_XML;

  }

  public class LazyContentStream implements ContentStream {
    ContentStream contentStream = null;
    UpdateRequest req = null;

    public LazyContentStream(UpdateRequest req) {
      this.req = req;
    }

    private ContentStream getDelegate() {
      if (contentStream == null) {
        try {
          contentStream = getContentStream(req);
        } catch (IOException e) {
          throw new RuntimeException("Unable to write xml into a stream", e);
        }
      }
      return contentStream;
    }

    @Override
    public String getName() {
      return getDelegate().getName();
    }

    @Override
    public String getSourceInfo() {
      return getDelegate().getSourceInfo();
    }

    @Override
    public String getContentType() {
      return getUpdateContentType();
    }

    @Override
    public Long getSize() {
      return getDelegate().getSize();
    }

    @Override
    public InputStream getStream() throws IOException {
      return getDelegate().getStream();
    }

    @Override
    public Reader getReader() throws IOException {
      return getDelegate().getReader();
    }

    public void writeTo(OutputStream os) throws IOException {
      write(req, os);

    }
  }

  protected boolean isNull(List l) {
    return l == null || l.isEmpty();
  }
  
  protected boolean isNull(Map l) {
    return l == null || l.isEmpty();
  }
}
