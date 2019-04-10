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

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.luke.app.desktop.util.inifile.IniFile;
import org.apache.lucene.luke.app.desktop.util.inifile.SimpleIniFile;
import org.apache.lucene.store.FSDirectory;

/** Default implementation of {@link Preferences} */
public final class PreferencesImpl implements Preferences {

  private static final String CONFIG_DIR = System.getProperty("user.home") + FileSystems.getDefault().getSeparator() + ".luke.d";
  private static final String INIT_FILE = "luke.ini";
  private static final String HISTORY_FILE = "history";
  private static final int MAX_HISTORY = 10;

  private final IniFile ini = new SimpleIniFile();


  private final List<String> history = new ArrayList<>();

  public PreferencesImpl() throws IOException {
    // create config dir if not exists
    Path confDir = FileSystems.getDefault().getPath(CONFIG_DIR);
    if (!Files.exists(confDir)) {
      Files.createDirectory(confDir);
    }

    // load configs
    if (Files.exists(iniFile())) {
      ini.load(iniFile());
    } else {
      ini.store(iniFile());
    }

    // load history
    Path histFile = historyFile();
    if (Files.exists(histFile)) {
      List<String> allHistory = Files.readAllLines(histFile);
      history.addAll(allHistory.subList(0, Math.min(MAX_HISTORY, allHistory.size())));
    }

  }

  public List<String> getHistory() {
    return history;
  }

  @Override
  public void addHistory(String indexPath) throws IOException {
    if (history.indexOf(indexPath) >= 0) {
      history.remove(indexPath);
    }
    history.add(0, indexPath);
    saveHistory();
  }

  private void saveHistory() throws IOException {
    Files.write(historyFile(), history);
  }

  private Path historyFile() {
    return FileSystems.getDefault().getPath(CONFIG_DIR, HISTORY_FILE);
  }

  @Override
  public ColorTheme getColorTheme() {
    String theme = ini.getString("settings", "theme");
    return (theme == null) ? ColorTheme.GRAY : ColorTheme.valueOf(theme);
  }

  @Override
  public void setColorTheme(ColorTheme theme) throws IOException {
    ini.put("settings", "theme", theme.name());
    ini.store(iniFile());
  }

  @Override
  public boolean isReadOnly() {
    Boolean readOnly = ini.getBoolean("opener", "readOnly");
    return (readOnly == null) ? false : readOnly;
  }

  @Override
  public String getDirImpl() {
    String dirImpl = ini.getString("opener", "dirImpl");
    return (dirImpl == null) ? FSDirectory.class.getName() : dirImpl;
  }

  @Override
  public boolean isNoReader() {
    Boolean noReader = ini.getBoolean("opener", "noReader");
    return (noReader == null) ? false : noReader;
  }

  @Override
  public boolean isUseCompound() {
    Boolean useCompound = ini.getBoolean("opener", "useCompound");
    return (useCompound == null) ? false : useCompound;
  }

  @Override
  public boolean isKeepAllCommits() {
    Boolean keepAllCommits = ini.getBoolean("opener", "keepAllCommits");
    return (keepAllCommits == null) ? false : keepAllCommits;
  }

  @Override
  public void setIndexOpenerPrefs(boolean readOnly, String dirImpl, boolean noReader, boolean useCompound, boolean keepAllCommits) throws IOException {
    ini.put("opener", "readOnly", readOnly);
    ini.put("opener", "dirImpl", dirImpl);
    ini.put("opener", "noReader", noReader);
    ini.put("opener", "useCompound", useCompound);
    ini.put("opener", "keepAllCommits", keepAllCommits);
    ini.store(iniFile());
  }

  private Path iniFile() {
    return FileSystems.getDefault().getPath(CONFIG_DIR, INIT_FILE);
  }
}
