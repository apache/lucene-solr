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
import java.util.logging.Logger;

import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;


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
public abstract class UpdateRequestProcessor {
  protected static Logger log = Logger.getLogger(UpdateRequestProcessor.class.getName());

  public abstract void processAdd(AddUpdateCommand cmd) throws IOException;
  public abstract void processDelete(DeleteUpdateCommand cmd) throws IOException;
  public abstract void processCommit(CommitUpdateCommand cmd) throws IOException;
  public abstract void finish() throws IOException;
}

