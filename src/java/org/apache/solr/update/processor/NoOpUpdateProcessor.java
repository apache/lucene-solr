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

import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;


/**
 * A simple processor that does nothing but passes on the command to the next
 * processor in the chain.  
 * 
 * @since solr 1.3
 */
public abstract class NoOpUpdateProcessor extends UpdateRequestProcessor 
{
  protected final UpdateRequestProcessor next;

  public NoOpUpdateProcessor( UpdateRequestProcessor next) {
    this.next = next;
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    if (next != null) next.processAdd(cmd);
  }

  @Override
  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    if (next != null) next.processDelete(cmd);
  }

  @Override
  public void processCommit(CommitUpdateCommand cmd) throws IOException
  {
    if (next != null) next.processCommit(cmd);
  }

  @Override
  public void finish() throws IOException {
    if (next != null) next.finish();    
  }
}


