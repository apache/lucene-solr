package org.apache.lucene.server.handlers;

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

import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.PreHandle;
import org.apache.lucene.server.params.*;

// nocommit needs ChannelHandlerContext too?

public abstract class Handler {

  /** Processes request into a FinishRequest, which is then
   *  invoked to actually make changes.  We do this two-step
   *  process so that we can fail if there are unhandled
   *  params, without having made any changes to the index. */
  public abstract FinishRequest handle(IndexState state, Request request, Map<String,List<String>> params) throws Exception;
  public abstract StructType getType();
  public abstract String getTopDoc();

  protected final GlobalState globalState;
  
  public boolean requiresIndexName = true;

  protected Handler(GlobalState globalState) {
    this.globalState = globalState;
  }

  public boolean doStream() {
    return false;
  }

  public String handleStreamed(Reader reader, Map<String,List<String>> params) throws Exception {
    throw new UnsupportedOperationException();
  }

  public final List<PreHandle> preHandlers = new CopyOnWriteArrayList<PreHandle>();

  public synchronized void addPreHandle(PreHandle h) {
    preHandlers.add(h);
  }
}
