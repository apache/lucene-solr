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

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.response.DocumentAnalysisResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.AnalysisParams;
import org.apache.solr.common.params.ModifiableSolrParams;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A request for the org.apache.solr.handler.DocumentAnalysisRequestHandler.
 *
 *
 * @since solr 1.4
 */
public class DocumentAnalysisRequest extends SolrRequest<DocumentAnalysisResponse> {

  private List<SolrInputDocument> documents = new ArrayList<>();
  private String query;
  private boolean showMatch = false;

  /**
   * Constructs a new request with a default uri of "/documentanalysis".
   */
  public DocumentAnalysisRequest() {
    super(METHOD.POST, "/analysis/document");
  }

  /**
   * Constructs a new request with the given request handler uri.
   *
   * @param uri The of the request handler.
   */
  public DocumentAnalysisRequest(String uri) {
    super(METHOD.POST, uri);
  }

  @Override
  public RequestWriter.ContentWriter getContentWriter(String expectedType) {

    return new RequestWriter.ContentWriter() {
      @Override
      public void write(OutputStream os) throws IOException {
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(os, UTF_8);
        try {
          getXML(outputStreamWriter);
        } finally {
          outputStreamWriter.flush();
        }
      }

      @Override
      public String getContentType() {
        return ClientUtils.TEXT_XML;
      }
    };

  }

  @Override
  protected DocumentAnalysisResponse createResponse(SolrClient client) {
    return new DocumentAnalysisResponse();
  }

  @Override
  public ModifiableSolrParams getParams() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    if (query != null) {
      params.add(AnalysisParams.QUERY, query);
      params.add(AnalysisParams.SHOW_MATCH, String.valueOf(showMatch));
    }
    return params;
  }

  //================================================ Helper Methods ==================================================

  /**
   * Returns the xml be be set as the request body.
   *
   * @return The xml be be set as the request body.
   *
   * @throws IOException When constructing the xml fails
   */
  String getXML(Writer writer) throws IOException {
//    StringWriter writer = new StringWriter();
    writer.write("<docs>");
    for (SolrInputDocument document : documents) {
      ClientUtils.writeXML(document, writer);
    }
    writer.write("</docs>");
    writer.flush();

    String xml = writer.toString();
    return (xml.length() > 0) ? xml : null;
  }


  //============================================ Setter/Getter Methods ===============================================

  /**
   * Adds a document to be analyzed.
   *
   * @param doc The document to be analyzed.
   *
   * @return This DocumentAnalysisRequest (fluent interface support).
   */
  public DocumentAnalysisRequest addDocument(SolrInputDocument doc) {
    documents.add(doc);
    return this;
  }

  /**
   * Adds a collection of documents to be analyzed.
   *
   * @param docs The documents to be analyzed.
   *
   * @return This DocumentAnalysisRequest (fluent interface support).
   *
   * @see #addDocument(org.apache.solr.common.SolrInputDocument)
   */
  public DocumentAnalysisRequest addDocuments(Collection<SolrInputDocument> docs) {
    documents.addAll(docs);
    return this;
  }

  /**
   * Sets the query to be analyzed. By default the query is set to null, meaning no query analysis will be performed.
   *
   * @param query The query to be analyzed.
   *
   * @return This DocumentAnalysisRequest (fluent interface support).
   */
  public DocumentAnalysisRequest setQuery(String query) {
    this.query = query;
    return this;
  }

  /**
   * Sets whether index time tokens that match query time tokens should be marked as a "match". By default this is set
   * to {@code false}. Obviously, this flag is ignored if when the query is set to {@code null}.
   *
   * @param showMatch Sets whether index time tokens that match query time tokens should be marked as a "match".
   *
   * @return This DocumentAnalysisRequest (fluent interface support).
   */
  public DocumentAnalysisRequest setShowMatch(boolean showMatch) {
    this.showMatch = showMatch;
    return this;
  }

  /**
   * Returns all documents that will be analyzed when processing the request.
   *
   * @return All documents that will be analyzed when processing the request.
   *
   * @see #addDocument(org.apache.solr.common.SolrInputDocument)
   */
  public List<SolrInputDocument> getDocuments() {
    return documents;
  }

  /**
   * Returns the query that will be analyzed when processing the request. May return {@code null} indicating that no
   * query time analysis is taking place.
   *
   * @return The query that will be analyzed when processing the request.
   *
   * @see #setQuery(String)
   */
  public String getQuery() {
    return query;
  }

  /**
   * Returns whether index time tokens that match query time tokens will be marked as a "match".
   *
   * @return Whether index time tokens that match query time tokens will be marked as a "match".
   *
   * @see #setShowMatch(boolean)
   */
  public boolean isShowMatch() {
    return showMatch;
  }

}
