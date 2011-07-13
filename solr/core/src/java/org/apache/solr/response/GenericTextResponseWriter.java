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

import java.io.Writer;
import java.io.IOException;

import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;

import org.apache.solr.response.BaseResponseWriter.SingleResponseWriter; // javadocs

/**
 * 
 * 
 * A generic {@link QueryResponseWriter} implementation that requires a user to
 * implement the
 * {@link #getSingleResponseWriter(Writer, SolrQueryRequest, SolrQueryResponse)}
 * that defines a {@link SingleResponseWriter} to handle plain ol' text output.
 * 
 * @since 1.5
 * @version $Id$
 * 
 * @deprecated use {@link TextResponseWriter} see SOLR-2485
 */
public abstract class GenericTextResponseWriter extends BaseResponseWriter
    implements QueryResponseWriter {

  /**
   * 
   * Writes text output using the {@link SingleResponseWriter} provided by a
   * call to
   * {@link #getSingleResponseWriter(Writer, SolrQueryRequest, SolrQueryResponse)}
   * .
   * 
   * @param writer
   *          The {@link Writer} to write the text output to.
   * @param request
   *          The provided {@link SolrQueryRequest}.
   * @param response
   *          The provided {@link SolrQueryResponse}.
   */
  public void write(Writer writer, SolrQueryRequest request,
      SolrQueryResponse response) throws IOException {
    super.write(getSingleResponseWriter(writer, request, response), request,
        response);
  }

  /**
   * Users of this class should implement this method to define a
   * {@link SingleResponseWriter} responsible for writing text output given a
   * {@link SolrDocumentList} or doc-by-doc, given a {@link SolrInputDocument}.
   * 
   * @param writer
   *          The {@link Writer} to write the text data response to.
   * @param request
   *          The provided {@link SolrQueryRequest}.
   * @param response
   *          The provided {@link SolrQueryResponse}.
   * @return A {@link SingleResponseWriter} that will be used to generate the
   *         response output from this {@link QueryResponseWriter}.
   */
  protected abstract SingleResponseWriter getSingleResponseWriter(
      Writer writer, SolrQueryRequest request, SolrQueryResponse response);
}
