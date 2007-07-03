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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
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
 * @since solr 1.3
 */
public class UpdateRequestProcessor
{
  public static Logger log = Logger.getLogger(UpdateRequestProcessor.class.getName());
  
  protected final SolrQueryRequest req;
  protected final UpdateHandler updateHandler;
  protected final long startTime;
  protected final NamedList<Object> response;
  
  // hold on to the added list for logging and the response
  protected List<Object> addedIds;
  
  public UpdateRequestProcessor( SolrQueryRequest req )
  {
    this.req = req;
    this.updateHandler = req.getCore().getUpdateHandler();
    this.startTime = System.currentTimeMillis();
    this.response = new NamedList<Object>();
  }
  
  /**
   * @return The response information
   */
  public NamedList<Object> finish()
  {
    long elapsed = System.currentTimeMillis() - startTime;
    log.info( "update"+response+" 0 " + (elapsed) );
    return response;
  }
  
  public void processDelete( DeleteUpdateCommand cmd ) throws IOException
  {
    if( cmd.id != null ) {
      updateHandler.delete( cmd );
      response.add( "delete", cmd.id );
    }
    else {
      updateHandler.deleteByQuery( cmd );
      response.add( "deleteByQuery", cmd.query );
    }
  }
  
  public void processCommit( CommitUpdateCommand cmd ) throws IOException
  {
    updateHandler.commit(cmd);
    response.add(cmd.optimize ? "optimize" : "commit", "");
  }

  public void processAdd( AddUpdateCommand cmd, SolrInputDocument doc ) throws IOException
  {
    // Add a list of added id's to the response
    if( addedIds == null ) {
      addedIds = new ArrayList<Object>();
      response.add( "added", addedIds );
    }
    
    IndexSchema schema = req.getSchema();
    SchemaField uniqueKeyField = schema.getUniqueKeyField();
    Object id = null;
    if (uniqueKeyField != null) {
      SolrInputField f = doc.getField( uniqueKeyField.getName() );
      if( f != null ) {
        id = f.getFirstValue();
      }
    }
    addedIds.add( id );
    
    cmd.doc = DocumentBuilder.toDocument( doc, schema );
    updateHandler.addDoc(cmd);
  }
}
