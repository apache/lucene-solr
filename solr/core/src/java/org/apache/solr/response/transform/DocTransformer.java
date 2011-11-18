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

package org.apache.solr.response.transform;

import java.io.IOException;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.request.SolrQueryRequest;

/**
 * A DocTransformer can add, remove or alter a Document before it is written out to the Response.  For instance, there are implementations
 * that can put explanations inline with a document, add constant values and mark items as being artificially boosted (see {@link org.apache.solr.handler.component.QueryElevationComponent})
 *
 * <p/>
 * New instance for each request
 *
 * @see TransformerFactory
 *
 */
public abstract class DocTransformer
{
  /**
   *
   * @return The name of the transformer
   */
  public abstract String getName();

  /**
   * This is called before transform and sets
   * @param context The {@link org.apache.solr.response.transform.TransformContext} stores information about the current state of things in Solr that may be
   * useful for doing transformations.
   */
  public void setContext( TransformContext context ) {}

  /**
   * This is where implementations do the actual work
   *
   *
   * @param doc The document to alter
   * @param docid The Lucene internal doc id
   * @throws IOException
   */
  public abstract void transform(SolrDocument doc, int docid) throws IOException;

  @Override
  public String toString() {
    return getName();
  }
}
