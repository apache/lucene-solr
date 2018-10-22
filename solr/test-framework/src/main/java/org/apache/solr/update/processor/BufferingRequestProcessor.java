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
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.RollbackUpdateCommand;

public class BufferingRequestProcessor extends UpdateRequestProcessor
{
  public List<AddUpdateCommand> addCommands = new ArrayList<>();
  public List<DeleteUpdateCommand> deleteCommands = new ArrayList<>();
  public List<CommitUpdateCommand> commitCommands = new ArrayList<>();
  public List<RollbackUpdateCommand> rollbackCommands = new ArrayList<>();
  
  public BufferingRequestProcessor(UpdateRequestProcessor next) {
    super(next);
  }
  
  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    addCommands.add( cmd );
  }

  @Override
  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    deleteCommands.add( cmd );
  }

  @Override
  public void processCommit(CommitUpdateCommand cmd) throws IOException {
    commitCommands.add( cmd );
  }
  
  @Override
  public void processRollback(RollbackUpdateCommand cmd) throws IOException
  {
    rollbackCommands.add( cmd );
  }

  @Override
  public void finish() throws IOException {
    // nothing?    
  }
}
