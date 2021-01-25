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
package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.CoreDescriptor;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ElectionContext {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected final String electionPath;
  protected final Replica leaderProps;
  protected final CoreDescriptor cd;
  protected final String id;
  protected final String leaderPath;
  protected volatile String leaderSeqPath;
  protected volatile String watchedSeqPath;


  public ElectionContext(final String id, final String electionPath, final String leaderPath, final Replica leaderProps, CoreDescriptor cd) {
    this.id = id;
    this.electionPath = electionPath;
    this.leaderPath = leaderPath;
    this.leaderProps = leaderProps;
    this.cd = cd;
  }

  protected void cancelElection() throws InterruptedException, KeeperException {
  }

  abstract boolean runLeaderProcess(ElectionContext context, boolean weAreReplacement, int pauseBeforeStartMs) throws KeeperException, InterruptedException, IOException;

  public void checkIfIamLeaderFired() {}

  public void joinedElectionFired() {}

  public ElectionContext copy(){
    throw new UnsupportedOperationException("copy");
  }

  public abstract boolean isClosed();

}



