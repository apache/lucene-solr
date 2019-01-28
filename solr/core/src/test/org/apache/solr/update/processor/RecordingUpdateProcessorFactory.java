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

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This Factory can optionally save references to the commands it receives in 
 * BlockingQueues that tests can poll from to observe that the expected commands 
 * are executed.  By default, this factory does nothing except return the "next" 
 * processor from the chain unless it's told to {@link #startRecording()}
 */
public final class RecordingUpdateProcessorFactory 
  extends UpdateRequestProcessorFactory {

  private boolean recording = false;

  /** The queue containing commands that were recorded
   * @see #startRecording
   */
  public final BlockingQueue<UpdateCommand> commandQueue 
    = new LinkedBlockingQueue<UpdateCommand>();

  /** 
   * @see #stopRecording 
   * @see #commandQueue
   */
  public synchronized void startRecording() {
    recording = true;
  }

  /** @see #startRecording */
  public synchronized void stopRecording() {
    recording = false;
  }

  @Override
  @SuppressWarnings("resource")
  public synchronized UpdateRequestProcessor getInstance(SolrQueryRequest req, 
                                                         SolrQueryResponse rsp, 
                                                         UpdateRequestProcessor next ) {
    return recording ? new RecordingUpdateRequestProcessor(commandQueue, next) : next;
  }

  private static final class RecordingUpdateRequestProcessor 
    extends UpdateRequestProcessor {

    private final BlockingQueue<UpdateCommand> commandQueue;

    public RecordingUpdateRequestProcessor(BlockingQueue<UpdateCommand> commandQueue, 
                                           UpdateRequestProcessor next) {
      super(next);
      this.commandQueue = commandQueue;
    }

    private void record(UpdateCommand cmd) {
      if (! commandQueue.offer(cmd) ) {
        throw new RuntimeException
          ("WTF: commandQueue should be unbounded but offer failed: " + cmd.toString());
      }
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      record(cmd);
      super.processAdd(cmd);
    }
    @Override
    public void processDelete(DeleteUpdateCommand cmd) throws IOException {
      record(cmd);
      super.processDelete(cmd);
    }
    @Override
    public void processMergeIndexes(MergeIndexesCommand cmd) throws IOException {
      record(cmd);
      super.processMergeIndexes(cmd);
    }
    @Override
    public void processCommit(CommitUpdateCommand cmd) throws IOException {
      record(cmd);
      super.processCommit(cmd);
    }
    @Override
    public void processRollback(RollbackUpdateCommand cmd) throws IOException {
      record(cmd);
      super.processRollback(cmd);
    }
  }
}



