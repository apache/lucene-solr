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
package org.apache.solr.handler;

import org.apache.solr.core.SolrCore;
import org.apache.solr.update.CdcrUpdateLog;

/**
 * This manager is responsible in enabling or disabling the buffering of the update logs. Currently, buffer
 * is always activated for non-leader nodes. For leader nodes, it is enabled only if the user explicitly
 * enabled it with the action {@link org.apache.solr.handler.CdcrParams.CdcrAction#ENABLEBUFFER}.
 */
class CdcrBufferManager implements CdcrStateManager.CdcrStateObserver {

  private CdcrLeaderStateManager leaderStateManager;
  private CdcrBufferStateManager bufferStateManager;

  private final SolrCore core;

  CdcrBufferManager(SolrCore core) {
    this.core = core;
  }

  void setLeaderStateManager(final CdcrLeaderStateManager leaderStateManager) {
    this.leaderStateManager = leaderStateManager;
    this.leaderStateManager.register(this);
  }

  void setBufferStateManager(final CdcrBufferStateManager bufferStateManager) {
    this.bufferStateManager = bufferStateManager;
    this.bufferStateManager.register(this);
  }

  /**
   * This method is synchronised as it can both be called by the leaderStateManager and the bufferStateManager.
   */
  @Override
  public synchronized void stateUpdate() {
    CdcrUpdateLog ulog = (CdcrUpdateLog) core.getUpdateHandler().getUpdateLog();

    // If I am not the leader, I should always buffer my updates
    if (!leaderStateManager.amILeader()) {
      ulog.enableBuffer();
      return;
    }
    // If I am the leader, I should buffer my updates only if buffer is enabled
    else if (bufferStateManager.getState().equals(CdcrParams.BufferState.ENABLED)) {
      ulog.enableBuffer();
      return;
    }

    // otherwise, disable the buffer
    ulog.disableBuffer();
  }

}

