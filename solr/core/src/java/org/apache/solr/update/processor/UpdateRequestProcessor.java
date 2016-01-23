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

package org.apache.solr.update.processor;

import java.io.IOException;

import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;


/**
 * This is a good place for subclassed update handlers to process the document before it is 
 * indexed.  You may wish to add/remove fields or check if the requested user is allowed to 
 * update the given document...
 * 
 * Perhaps you continue adding an error message (without indexing the document)...
 * perhaps you throw an error and halt indexing (remove anything already indexed??)
 * 
 * By default, this just passes the request to the next processor in the chain.
 * 
 * @since solr 1.3
 */
public abstract class UpdateRequestProcessor {
  protected final UpdateRequestProcessor next;

  public UpdateRequestProcessor( UpdateRequestProcessor next) {
    this.next = next;
  }

  public void processAdd(AddUpdateCommand cmd) throws IOException {
    if (next != null) next.processAdd(cmd);
  }

  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    if (next != null) next.processDelete(cmd);
  }

  public void processMergeIndexes(MergeIndexesCommand cmd) throws IOException {
    if (next != null) next.processMergeIndexes(cmd);
  }

  public void processCommit(CommitUpdateCommand cmd) throws IOException
  {
    if (next != null) next.processCommit(cmd);
  }

  /**
   * @since Solr 1.4
   */
  public void processRollback(RollbackUpdateCommand cmd) throws IOException
  {
    if (next != null) next.processRollback(cmd);
  }

  public void finish() throws IOException {
    if (next != null) next.finish();    
  }
}

