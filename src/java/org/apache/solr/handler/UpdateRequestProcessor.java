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

package org.apache.solr.handler;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.DocumentBuilder;
import org.apache.solr.update.UpdateHandler;


/**
 * This is a good place for subclassed update handlers to process the document before it is 
 * indexed.  You may wish to add/remove fields or check if the requested user is allowed to 
 * update the given document...
 * 
 * Perhaps you continue adding an error message (without indexing the document)...
 * perhaps you throw an error and halt indexing (remove anything already indexed??)
 * 
 * This implementation (the default) passes the request command (as is) to the updateHandler
 * and adds debug info to the response.
 * 
 * @author ryan 
 * @since solr 1.3
 */
public class UpdateRequestProcessor
{
  public static Logger log = Logger.getLogger(UpdateRequestProcessor.class.getName());
  
  protected final SolrQueryRequest req;
  protected final SolrCore core;
  protected final IndexSchema schema;
  protected final UpdateHandler updateHandler;
  protected final SchemaField uniqueKeyField;
  protected final long startTime;
  protected final NamedList<Object> response;
  
  public UpdateRequestProcessor( SolrQueryRequest req )
  {
    this.req = req;
    
    core = req.getCore();
    schema = core.getSchema();
    updateHandler = core.getUpdateHandler();
    uniqueKeyField = schema.getUniqueKeyField();
    startTime = System.currentTimeMillis();
    
    // A place to put our output
    response = new NamedList<Object>();
  }
  
  /**
   * @return The response information
   */
  public NamedList<Object> getResponse()
  {
    return response;
  }
  
  public void processDelete( DeleteUpdateCommand cmd ) throws IOException
  {
    long start = System.currentTimeMillis();
    if( cmd.id != null ) {
      updateHandler.delete( cmd );
      long now = System.currentTimeMillis();
      log.info("delete(id " + cmd.id + ") 0 " + (now - start) + " ["+(now-startTime)+"]");
      
      response.add( "delete", cmd.id );
    }
    else {
      // TODO? if cmd.query == "*:* it should do something special
      
      updateHandler.deleteByQuery( cmd );
      long now = System.currentTimeMillis();
      log.info("deleteByQuery(id " + cmd.query + ") 0 " + (now - start) + " ["+(now-startTime)+"]");

      response.add( "deleteByQuery", cmd.id );
    }
  }
  
  public void processCommit( CommitUpdateCommand cmd ) throws IOException
  {
    updateHandler.commit(cmd);
    response.add(cmd.optimize ? "optimize" : "commit", "");
    long now = System.currentTimeMillis();
    
    if (cmd.optimize) {
      log.info("optimize 0 " + (now - startTime)+ " ["+(now-startTime)+"]");
    } 
    else {
      log.info("commit 0 " + (now - startTime)+ " ["+(now-startTime)+"]");
    }
  }

  // TODO -- in the future, the update command should just hold onto a SolrDocument
  public void processAdd( AddUpdateCommand cmd, SolrInputDocument doc ) throws IOException
  {
    long start = System.currentTimeMillis();
    Object id = null;
    if (uniqueKeyField != null) {
      id = doc.getFieldValue( uniqueKeyField.getName() );
    }
    cmd.doc = DocumentBuilder.toDocument( doc, schema );
    updateHandler.addDoc(cmd);
    response.add( "added", id );

    long now = System.currentTimeMillis();
    log.info("added id={" + id  + "} in " + (now - start) + "ms  ["+(now-startTime)+"]");
  }
}
