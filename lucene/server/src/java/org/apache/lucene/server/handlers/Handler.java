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

/** Handles one method. */

public abstract class Handler {

  /** Processes request into a {@link FinishRequest}, which is then
   *  invoked to actually make changes.  We do this two-step
   *  process so that we can fail if there are unhandled
   *  params, without having made any changes to the
   *  index.  When this returns, it must have retrieved all
   *  parameters it needs to use from the provided {@link
   *  Request}. */
  public abstract FinishRequest handle(IndexState state, Request request, Map<String,List<String>> params) throws Exception;

  /** Returns the {@link StructType} describing the
   *  parameters this method accepts. */
  public abstract StructType getType();

  /** Returns the brief summary documentation for this
   *  method (English). */
  public abstract String getTopDoc();

  /** The {@link GlobalState} instance. */
  protected final GlobalState globalState;

  /** True if this handler requires indexName. */
  public boolean requiresIndexName = true;

  // nocommit nuke this; the handlers can get globalState
  // via IndexState?  hmm, except for those handlers that
  // don't take an indexName (e.g. createIndex).

  /** Sole constructor. */
  protected Handler(GlobalState globalState) {
    this.globalState = globalState;
  }

  /** True if this handler uses streaming (e.g. the bulk
   *  indexing APIs). */
  public boolean doStream() {
    return false;
  }

  /** Invoked for handlers that use streaming. */
  public String handleStreamed(Reader reader, Map<String,List<String>> params) throws Exception {
    throw new UnsupportedOperationException();
  }

  /** For plugins: list of {@link PreHandle}rs, so a plugin
   *  can modify the request before the default handler
   *  sees it. */
  public final List<PreHandle> preHandlers = new CopyOnWriteArrayList<PreHandle>();

  /** Add a pre-handler for this method. */
  public synchronized void addPreHandle(PreHandle h) {
    preHandlers.add(h);
  }
}
