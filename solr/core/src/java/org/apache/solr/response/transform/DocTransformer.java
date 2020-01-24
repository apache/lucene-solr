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
   * This is called before {@link #transform} to provide context for any subsequent transformations.
   *
   * @param context The {@link ResultContext} stores information about how the documents were produced.
   * @see #needsSolrIndexSearcher
   */
  public void setContext( ResultContext context ) {
    this.context = context;

  }

  /**
   * Indicates if this transformer requires access to the underlying index to perform it's functions.
   *
   * In some situations (notably RealTimeGet) this method <i>may</i> be called before {@link #setContext} 
   * to determine if the transformer must be given a "full" ResultContext and accurate docIds 
   * that can be looked up using {@link ResultContext#getSearcher}, or if optimizations can be taken 
   * advantage of such that {@link ResultContext#getSearcher} <i>may</i> return null, and docIds passed to 
   * {@link #transform} <i>may</i> be negative.
   *
   * The default implementation always returns <code>false</code>.
   * 
   * @see ResultContext#getSearcher
   * @see #transform
   */
  public boolean needsSolrIndexSearcher() { return false; }
  
  /**
   * This is where implementations do the actual work.
   * If implementations require a valid docId and index access, the {@link #needsSolrIndexSearcher} 
   * method must return true
   *
   * Default implementation calls {@link #transform(SolrDocument, int)}.
   *
   * @param doc The document to alter
   * @param docid The Lucene internal doc id, or -1 in cases where the <code>doc</code> did not come from the index
   * @param score the score for this document
   * @throws IOException If there is a low-level I/O error.
   * @see #needsSolrIndexSearcher
   */
  public void transform(SolrDocument doc, int docid, float score) throws IOException {
    transform(doc, docid);
  }

  /**
   * This is where implementations do the actual work.
   * If implementations require a valid docId and index access, the {@link #needsSolrIndexSearcher} 
   * method must return true
   *
   * @param doc The document to alter
   * @param docid The Lucene internal doc id, or -1 in cases where the <code>doc</code> did not come from the index
   * @throws IOException If there is a low-level I/O error.
   * @see #needsSolrIndexSearcher
   */
  public abstract void transform(SolrDocument doc, int docid) throws IOException;

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

  /**
   * Trivial Impl that ensure that the specified field is requested as an "extra" field,
   * but then does nothing during the transformation phase.
   */
  public static final class NoopFieldTransformer extends DocTransformer {
    final String field;

    public NoopFieldTransformer() {
      this.field = null;
    }

    public NoopFieldTransformer(String field ) {
      this.field = field;
    }
    public String getName() { return "noop"; }
    public String[] getExtraRequestFields() {
      return this.field==null? null: new String[] { field };
    }
    public void transform(SolrDocument doc, int docid) {
      // No-Op
    }
  }
}
