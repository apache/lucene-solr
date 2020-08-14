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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.util.ContentStream;

/**
 * A RequestWriter is used to write requests to Solr.
 * <p>
 * A subclass can override the methods in this class to supply a custom format in which a request can be sent.
 *
 *
 * @since solr 1.4
 */
public class RequestWriter {


  public interface ContentWriter {

    void write(OutputStream os) throws IOException;

    String getContentType();
  }

  /**
   * Use this to do a push writing instead of pull. If this method returns null
   * {@link org.apache.solr.client.solrj.request.RequestWriter#getContentStreams(SolrRequest)} is
   * invoked to do a pull write.
   */
  public ContentWriter getContentWriter(@SuppressWarnings({"rawtypes"})SolrRequest req) {
    if (req instanceof UpdateRequest) {
      UpdateRequest updateRequest = (UpdateRequest) req;
      if (isEmpty(updateRequest)) return null;
      return new ContentWriter() {
        @Override
        public void write(OutputStream os) throws IOException {
          OutputStreamWriter writer = new OutputStreamWriter(os, StandardCharsets.UTF_8);
          updateRequest.writeXML(writer);
          writer.flush();
        }

        @Override
        public String getContentType() {
          return ClientUtils.TEXT_XML;
        }
      };
    }
    return req.getContentWriter(ClientUtils.TEXT_XML);
  }

  /**
   * @deprecated Use {@link #getContentWriter(SolrRequest)}.
   */
  @Deprecated
  @SuppressWarnings({"unchecked"})
  public Collection<ContentStream> getContentStreams(@SuppressWarnings({"rawtypes"})SolrRequest req) throws IOException {
    if (req instanceof UpdateRequest) {
      return null;
    }
    return req.getContentStreams();
  }

  protected boolean isEmpty(UpdateRequest updateRequest) {
    return isNull(updateRequest.getDocuments()) &&
            isNull(updateRequest.getDeleteByIdMap()) &&
            isNull(updateRequest.getDeleteQuery()) &&
            updateRequest.getDocIterator() == null;
  }

  public String getPath(@SuppressWarnings({"rawtypes"})SolrRequest req) {
    return req.getPath();
  }

  public void write(@SuppressWarnings({"rawtypes"})SolrRequest request, OutputStream os) throws IOException {
    if (request instanceof UpdateRequest) {
      UpdateRequest updateRequest = (UpdateRequest) request;
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
      updateRequest.writeXML(writer);
      writer.flush();
    }
  }

  public String getUpdateContentType() {
    return ClientUtils.TEXT_XML;
  }

  public static class StringPayloadContentWriter implements ContentWriter {
    public final String payload;
    public final String type;

    public StringPayloadContentWriter(String payload, String type) {
      this.payload = payload;
      this.type = type;
    }

    @Override
    public void write(OutputStream os) throws IOException {
      if (payload == null) return;
      os.write(payload.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String getContentType() {
      return type;
    }
  }

  protected boolean isNull(@SuppressWarnings({"rawtypes"})List l) {
    return l == null || l.isEmpty();
  }
  
  protected boolean isNull(@SuppressWarnings({"rawtypes"})Map l) {
    return l == null || l.isEmpty();
  }
}
