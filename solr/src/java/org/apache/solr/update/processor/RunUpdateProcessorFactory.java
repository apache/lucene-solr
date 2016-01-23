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

package org.apache.solr.update.processor;

import java.io.IOException;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.DocumentBuilder;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.UpdateHandler;


/**
 * Pass the command to the UpdateHandler without any modifications
 * 
 * @since solr 1.3
 */
public class RunUpdateProcessorFactory extends UpdateRequestProcessorFactory 
{
  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) 
  {
    return new RunUpdateProcessor(req, next);
  }
}

class RunUpdateProcessor extends UpdateRequestProcessor 
{
  private final SolrQueryRequest req;
  private final UpdateHandler updateHandler;

  public RunUpdateProcessor(SolrQueryRequest req, UpdateRequestProcessor next) {
    super( next );
    this.req = req;
    this.updateHandler = req.getCore().getUpdateHandler();
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    cmd.doc = DocumentBuilder.toDocument(cmd.getSolrInputDocument(), req.getSchema());
    updateHandler.addDoc(cmd);
    super.processAdd(cmd);
  }

  @Override
  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    if( cmd.id != null ) {
      updateHandler.delete(cmd);
    }
    else {
      updateHandler.deleteByQuery(cmd);
    }
    super.processDelete(cmd);
  }

  @Override
  public void processMergeIndexes(MergeIndexesCommand cmd) throws IOException {
    updateHandler.mergeIndexes(cmd);
    super.processMergeIndexes(cmd);
  }

  @Override
  public void processCommit(CommitUpdateCommand cmd) throws IOException
  {
    updateHandler.commit(cmd);
    super.processCommit(cmd);
  }

  /**
   * @since Solr 1.4
   */
  @Override
  public void processRollback(RollbackUpdateCommand cmd) throws IOException
  {
    updateHandler.rollback(cmd);
    super.processRollback(cmd);
  }
}


