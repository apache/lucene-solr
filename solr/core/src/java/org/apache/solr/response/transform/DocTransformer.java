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

package org.apache.solr.response.transform;

import java.io.IOException;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.ResultContext;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * A DocTransformer can add, remove or alter a Document before it is written out to the Response.  For instance, there are implementations
 * that can put explanations inline with a document, add constant values and mark items as being artificially boosted (see {@link org.apache.solr.handler.component.QueryElevationComponent})
 *
 * <p>
 * New instance for each request
 *
 * @see TransformerFactory
 *
 */
public abstract class DocTransformer {
  protected ResultContext context;
  /**
   *
   * @return The name of the transformer
   */
  public abstract String getName();

  /**
   * This is called before transform and sets
   * @param context The {@link ResultContext} stores information about how the documents were produced.
   */
  public void setContext( ResultContext context ) {
    this.context = context;

  }

  /**
   * This is where implementations do the actual work
   *
   *
   * @param doc The document to alter
   * @param docid The Lucene internal doc id
   * @param score the score for this document
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void transform(SolrDocument doc, int docid, float score) throws IOException;

  /**
   * When a transformer needs access to fields that are not automatically derived from the
   * input fields names, this option lets us explicitly say the field names that we hope
   * will be in the SolrDocument.  These fields will be requested from the
   * {@link SolrIndexSearcher} but may or may not be returned in the final
   * {@link QueryResponseWriter}
   * 
   * @return a list of extra lucene fields
   */
  public String[] getExtraRequestFields() {
    return null;
  }
  
  @Override
  public String toString() {
    return getName();
  }
}
