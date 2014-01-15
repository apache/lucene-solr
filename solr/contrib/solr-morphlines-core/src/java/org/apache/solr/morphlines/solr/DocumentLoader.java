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
package org.apache.solr.morphlines.solr;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

/**
 * A vehicle to load a list of Solr documents into some kind of destination,
 * such as a SolrServer or MapReduce RecordWriter.
 */
public interface DocumentLoader {

  /** Begins a transaction */
  public void beginTransaction() throws IOException, SolrServerException;

  /** Loads the given document into the destination */
  public void load(SolrInputDocument doc) throws IOException, SolrServerException;

  /**
   * Sends any outstanding documents to the destination and waits for a positive
   * or negative ack (i.e. exception). Depending on the outcome the caller
   * should then commit or rollback the current flume transaction
   * correspondingly.
   * 
   * @throws IOException
   *           If there is a low-level I/O error.
   */
  public void commitTransaction() throws IOException, SolrServerException;

  /**
   * Performs a rollback of all non-committed documents pending.
   * <p>
   * Note that this is not a true rollback as in databases. Content you have
   * previously added may have already been committed due to autoCommit, buffer
   * full, other client performing a commit etc. So this is only a best-effort
   * rollback.
   * 
   * @throws IOException
   *           If there is a low-level I/O error.
   */
  public UpdateResponse rollbackTransaction() throws IOException, SolrServerException;

  /** Releases allocated resources */
  public void shutdown() throws IOException, SolrServerException;

  /**
   * Issues a ping request to check if the server is alive
   * 
   * @throws IOException
   *           If there is a low-level I/O error.
   */
  public SolrPingResponse ping() throws IOException, SolrServerException;

}
