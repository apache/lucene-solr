package org.apache.solr.schema;
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

import org.apache.lucene.search.similarities.Similarity;
import org.apache.solr.schema.SchemaAware; // javadocs
import org.apache.solr.schema.FieldType; // javadocs
import org.apache.solr.common.params.SolrParams;


/**
 * A factory interface for configuring a {@link Similarity} in the Solr 
 * schema.xml.  
 * 
 * <p>
 * Subclasses of <code>SimilarityFactory</code> which are {@link SchemaAware} 
 * must take responsibility for either consulting the similarities configured 
 * on individual field types, or generating appropriate error/warning messages 
 * if field type specific similarities exist but are being ignored.  The 
 * <code>IndexSchema</code> will provide such error checking if a 
 * non-<code>SchemaAware</code> instance of <code>SimilarityFactory</code> 
 * is used.
 * 
 * @see FieldType#getSimilarity
 */
public abstract class SimilarityFactory {
  protected SolrParams params;

  public void init(SolrParams params) { this.params = params; }
  public SolrParams getParams() { return params; }

  public abstract Similarity getSimilarity();
}
