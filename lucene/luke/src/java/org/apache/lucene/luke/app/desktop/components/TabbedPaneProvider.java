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
import javax.swing.JTextArea;
import java.io.IOException;

import org.apache.lucene.luke.app.DirectoryHandler;
import org.apache.lucene.luke.app.DirectoryObserver;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.app.IndexObserver;
import org.apache.lucene.luke.app.LukeState;
import org.apache.lucene.luke.app.desktop.MessageBroker;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.TabUtils;

/** Provider of the Tabbed pane */
public final class TabbedPaneProvider implements TabSwitcherProxy.TabSwitcher {

  private final MessageBroker messageBroker;

  private final JTabbedPane tabbedPane = new JTabbedPane();

  private final JPanel overviewPanel;

  private final JPanel documentsPanel;

  private final JPanel searchPanel;

  private final JPanel analysisPanel;

  private final JPanel commitsPanel;

  private final JPanel logsPanel;

  public TabbedPaneProvider(JTextArea logTextArea) throws IOException {
    this.overviewPanel = new OverviewPanelProvider().get();
    this.documentsPanel = new DocumentsPanelProvider().get();
    this.searchPanel = new SearchPanelProvider().get();
    this.analysisPanel = new AnalysisPanelProvider().get();
    this.commitsPanel = new CommitsPanelProvider().get();
    this.logsPanel = new LogsPanelProvider(logTextArea).get();

    this.messageBroker = MessageBroker.getInstance();

    TabSwitcherProxy.getInstance().set(this);

    Observer observer = new Observer();
    IndexHandler.getInstance().addObserver(observer);
    DirectoryHandler.getInstance().addObserver(observer);
  }

  public JTabbedPane get() {
    tabbedPane.addTab(FontUtils.elegantIconHtml("&#xe009;", "Overview"), overviewPanel);
    tabbedPane.addTab(FontUtils.elegantIconHtml("&#x69;", "Documents"), documentsPanel);
    tabbedPane.addTab(FontUtils.elegantIconHtml("&#xe101;", "Search"), searchPanel);
    tabbedPane.addTab(FontUtils.elegantIconHtml("&#xe104;", "Analysis"), analysisPanel);
    tabbedPane.addTab(FontUtils.elegantIconHtml("&#xe0ea;", "Commits"), commitsPanel);
    tabbedPane.addTab(FontUtils.elegantIconHtml("&#xe058;", "Logs"), logsPanel);

    TabUtils.forceTransparent(tabbedPane);

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

  /** tabs in the main frame */
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
