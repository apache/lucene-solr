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

package org.apache.lucene.luke.app.desktop.components;

import javax.swing.JPanel;
import javax.swing.JTabbedPane;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import org.apache.lucene.luke.app.DirectoryHandler;
import org.apache.lucene.luke.app.DirectoryObserver;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.app.IndexObserver;
import org.apache.lucene.luke.app.LukeState;
import org.apache.lucene.luke.app.desktop.MessageBroker;
import org.apache.lucene.luke.app.desktop.util.FontUtils;

public final class TabbedPaneProvider implements Provider<JTabbedPane>, TabSwitcherProxy.TabSwitcher {

  private final MessageBroker messageBroker;

  private final JTabbedPane tabbedPane = new JTabbedPane();

  private final JPanel overviewPanel;

  private final JPanel documentsPanel;

  private final JPanel searchPanel;

  private final JPanel analysisPanel;

  private final JPanel commitsPanel;

  private final JPanel logsPanel;

  @Inject
  public TabbedPaneProvider(@Named("overview") JPanel overviewPanel,
                            @Named("documents") JPanel documentsPanel,
                            @Named("search") JPanel searchPanel,
                            @Named("analysis") JPanel analysisPanel,
                            @Named("commits") JPanel commitsPanel,
                            @Named("logs") JPanel logsPanel,
                            IndexHandler indexHandler,
                            DirectoryHandler directoryHandler,
                            TabSwitcherProxy tabSwitcher,
                            MessageBroker messageBroker) {
    this.overviewPanel = overviewPanel;
    this.documentsPanel = documentsPanel;
    this.searchPanel = searchPanel;
    this.analysisPanel = analysisPanel;
    this.commitsPanel = commitsPanel;
    this.logsPanel = logsPanel;

    this.messageBroker = messageBroker;

    tabSwitcher.set(this);

    Observer observer = new Observer();
    indexHandler.addObserver(observer);
    directoryHandler.addObserver(observer);
  }

  @Override
  public JTabbedPane get() {
    tabbedPane.addTab(FontUtils.elegantIconHtml("&#xe009;", "Overview"), overviewPanel);
    tabbedPane.addTab(FontUtils.elegantIconHtml("&#x69;", "Documents"), documentsPanel);
    tabbedPane.addTab(FontUtils.elegantIconHtml("&#xe101;", "Search"), searchPanel);
    tabbedPane.addTab(FontUtils.elegantIconHtml("&#xe104;", "Analysis"), analysisPanel);
    tabbedPane.addTab(FontUtils.elegantIconHtml("&#xe0ea;", "Commits"), commitsPanel);
    tabbedPane.addTab(FontUtils.elegantIconHtml("&#xe058;", "Logs"), logsPanel);

    tabbedPane.setOpaque(false);
    return tabbedPane;
  }

  public void switchTab(Tab tab) {
    tabbedPane.setSelectedIndex(tab.index());
    tabbedPane.setVisible(false);
    tabbedPane.setVisible(true);
    messageBroker.clearStatusMessage();
  }

  private class Observer implements IndexObserver, DirectoryObserver {

    @Override
    public void openDirectory(LukeState state) {
      tabbedPane.setEnabledAt(Tab.COMMITS.index(), true);
    }

    @Override
    public void closeDirectory() {
      tabbedPane.setEnabledAt(Tab.OVERVIEW.index(), false);
      tabbedPane.setEnabledAt(Tab.DOCUMENTS.index(), false);
      tabbedPane.setEnabledAt(Tab.SEARCH.index(), false);
      tabbedPane.setEnabledAt(Tab.COMMITS.index(), false);
    }

    @Override
    public void openIndex(LukeState state) {
      tabbedPane.setEnabledAt(Tab.OVERVIEW.index(), true);
      tabbedPane.setEnabledAt(Tab.DOCUMENTS.index(), true);
      tabbedPane.setEnabledAt(Tab.SEARCH.index(), true);
      tabbedPane.setEnabledAt(Tab.COMMITS.index(), true);
    }

    @Override
    public void closeIndex() {
      tabbedPane.setEnabledAt(Tab.OVERVIEW.index(), false);
      tabbedPane.setEnabledAt(Tab.DOCUMENTS.index(), false);
      tabbedPane.setEnabledAt(Tab.SEARCH.index(), false);
      tabbedPane.setEnabledAt(Tab.COMMITS.index(), false);
    }
  }

  public enum Tab {
    OVERVIEW(0), DOCUMENTS(1), SEARCH(2), ANALYZER(3), COMMITS(4);

    private int tabIdx;

    Tab(int tabIdx) {
      this.tabIdx = tabIdx;
    }

    int index() {
      return tabIdx;
    }
  }

}
