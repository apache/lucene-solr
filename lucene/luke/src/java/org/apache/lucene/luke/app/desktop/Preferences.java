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

package org.apache.lucene.luke.app.desktop;

import org.apache.lucene.luke.app.controllers.LukeController;

import java.io.IOException;
import java.util.List;

public interface Preferences {

  List<String> getHistory();

  void addHistory(String indexPath) throws IOException;

  LukeController.ColorTheme getTheme();

  void setTheme(LukeController.ColorTheme theme) throws IOException;

  boolean isReadOnly();

  String getDirImpl();

  boolean isNoReader();

  boolean isUseCompound();

  boolean isKeepAllCommits();

  void setIndexOpenerPrefs(boolean readOnly, String dirImpl, boolean noReader, boolean useCompound, boolean keepAllCommits) throws IOException;
}
