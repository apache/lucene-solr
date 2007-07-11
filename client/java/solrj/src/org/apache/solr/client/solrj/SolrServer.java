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

package org.apache.solr.client.solrj;

import java.io.IOException;
import java.util.Collection;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/**
 * 
 * @version $Id$
 * @since solr 1.3
 */
public interface SolrServer 
{
  // A general method to allow various methods 
  NamedList<Object> request( final SolrRequest request ) throws SolrServerException, IOException;
  
  // Standard methods
  UpdateResponse add( SolrInputDocument doc ) throws SolrServerException, IOException;
  UpdateResponse add( Collection<SolrInputDocument> docs ) throws SolrServerException, IOException;
  UpdateResponse add( SolrInputDocument doc, boolean overwrite ) throws SolrServerException, IOException;
  UpdateResponse add( Collection<SolrInputDocument> docs, boolean overwrite ) throws SolrServerException, IOException;
  UpdateResponse deleteById( String id ) throws SolrServerException, IOException;
  UpdateResponse deleteByQuery( String query ) throws SolrServerException, IOException;
  UpdateResponse commit( boolean waitFlush, boolean waitSearcher ) throws SolrServerException, IOException;
  UpdateResponse optimize( boolean waitFlush, boolean waitSearcher ) throws SolrServerException, IOException;
  UpdateResponse commit( ) throws SolrServerException, IOException;
  UpdateResponse optimize( ) throws SolrServerException, IOException;
  QueryResponse query( SolrParams params ) throws SolrServerException, IOException;
  SolrPingResponse ping() throws SolrServerException, IOException;
}