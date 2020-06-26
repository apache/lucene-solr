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

package org.apache.lucene.luke.app;

import java.lang.invoke.MethodHandles;
import java.util.Objects;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.models.util.IndexUtils;
import org.apache.lucene.luke.util.LoggerFactory;

/** Index open/close handler */
public final class IndexHandler extends AbstractHandler<IndexObserver> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final IndexHandler instance = new IndexHandler();

  private LukeStateImpl state;

  public static IndexHandler getInstance() {
    return instance;
  }

  @Override
  protected void notifyOne(IndexObserver observer) {
    if (state.closed) {
      observer.closeIndex();
    } else {
      observer.openIndex(state);
    }
  }

  public boolean indexOpened() {
    return state != null && !state.closed;
  }

  public void open(String indexPath, String dirImpl) {
    open(indexPath, dirImpl, false, false, false);
  }

  public void open(String indexPath, String dirImpl, boolean readOnly, boolean useCompound, boolean keepAllCommits) {
    Objects.requireNonNull(indexPath);

    if (indexOpened()) {
      close();
    }

    IndexReader reader;
    try {
      reader = IndexUtils.openIndex(indexPath, dirImpl);
    } catch (Exception e) {
      log.error("Error opening index", e);
      throw new LukeException(MessageUtils.getLocalizedMessage("openindex.message.index_path_invalid", indexPath), e);
    }

    state = new LukeStateImpl();
    state.indexPath = indexPath;
    state.reader = reader;
    state.dirImpl = dirImpl;
    state.readOnly = readOnly;
    state.useCompound = useCompound;
    state.keepAllCommits = keepAllCommits;

    notifyObservers();
  }

  public void close() {
    if (state == null) {
      return;
    }

    IndexUtils.close(state.reader);

    state.closed = true;
    notifyObservers();
  }

  public void reOpen() {
    close();
    open(state.getIndexPath(), state.getDirImpl(), state.readOnly(), state.useCompound(), state.keepAllCommits());
  }

  public LukeState getState() {
    return state;
  }

  private static class LukeStateImpl implements LukeState {

    private boolean closed = false;

    private String indexPath;
    private IndexReader reader;
    private String dirImpl;
    private boolean readOnly;
    private boolean useCompound;
    private boolean keepAllCommits;

    @Override
    public String getIndexPath() {
      return indexPath;
    }

    @Override
    public IndexReader getIndexReader() {
      return reader;
    }

    @Override
    public String getDirImpl() {
      return dirImpl;
    }

    @Override
    public boolean readOnly() {
      return readOnly;
    }

    @Override
    public boolean useCompound() {
      return useCompound;
    }

    @Override
    public boolean keepAllCommits() {
      return keepAllCommits;
    }
  }
}
