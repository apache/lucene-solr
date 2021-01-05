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

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.models.util.IndexUtils;
import org.apache.lucene.store.Directory;

/** Directory open/close handler */
public final class DirectoryHandler extends AbstractHandler<DirectoryObserver> {

  private static final DirectoryHandler instance = new DirectoryHandler();

  private LukeStateImpl state;

  public static DirectoryHandler getInstance() {
    return instance;
  }

  @Override
  protected void notifyOne(DirectoryObserver observer) {
    if (state.closed) {
      observer.closeDirectory();
    } else {
      observer.openDirectory(state);
    }
  }

  public boolean directoryOpened() {
    return state != null && !state.closed;
  }

  public void open(String indexPath, String dirImpl) {
    Objects.requireNonNull(indexPath);

    if (directoryOpened()) {
      close();
    }

    Directory dir;
    try {
      dir = IndexUtils.openDirectory(indexPath, dirImpl);
    } catch (IOException e) {
      throw new LukeException(
          MessageUtils.getLocalizedMessage("openindex.message.index_path_invalid", indexPath), e);
    }

    state = new LukeStateImpl();
    state.indexPath = indexPath;
    state.dirImpl = dirImpl;
    state.dir = dir;

    notifyObservers();
  }

  public void close() {
    if (state == null) {
      return;
    }

    IndexUtils.close(state.dir);

    state.closed = true;
    notifyObservers();
  }

  public LukeState getState() {
    return state;
  }

  private static class LukeStateImpl implements LukeState {
    private boolean closed = false;

    private String indexPath;
    private String dirImpl;
    private Directory dir;

    @Override
    public String getIndexPath() {
      return indexPath;
    }

    @Override
    public String getDirImpl() {
      return dirImpl;
    }

    @Override
    public Directory getDirectory() {
      return dir;
    }
  }
}
