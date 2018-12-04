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

import javax.swing.JFrame;
import javax.swing.JMenuBar;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import org.apache.lucene.luke.app.LukeModule;
import org.apache.lucene.luke.app.desktop.components.AnalysisPanelProvider;
import org.apache.lucene.luke.app.desktop.components.CommitsPanelProvider;
import org.apache.lucene.luke.app.desktop.components.ComponentOperatorRegistry;
import org.apache.lucene.luke.app.desktop.components.DocumentsPanelProvider;
import org.apache.lucene.luke.app.desktop.components.LogsPanelProvider;
import org.apache.lucene.luke.app.desktop.components.LukeWindowProvider;
import org.apache.lucene.luke.app.desktop.components.MenuBarProvider;
import org.apache.lucene.luke.app.desktop.components.OverviewPanelProvider;
import org.apache.lucene.luke.app.desktop.components.SearchPanelProvider;
import org.apache.lucene.luke.app.desktop.components.TabSwitcherProxy;
import org.apache.lucene.luke.app.desktop.components.TabbedPaneProvider;
import org.apache.lucene.luke.app.desktop.components.dialog.ConfirmDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.ConfirmDialogFactoryImpl;
import org.apache.lucene.luke.app.desktop.components.dialog.HelpDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.HelpDialogFactoryImpl;
import org.apache.lucene.luke.app.desktop.components.dialog.analysis.AnalysisChainDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.analysis.AnalysisChainDialogFactoryImpl;
import org.apache.lucene.luke.app.desktop.components.dialog.analysis.EditFiltersDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.analysis.EditFiltersDialogFactoryImpl;
import org.apache.lucene.luke.app.desktop.components.dialog.analysis.EditParamsDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.analysis.EditParamsDialogFactoryImpl;
import org.apache.lucene.luke.app.desktop.components.dialog.analysis.TokenAttributeDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.analysis.TokenAttributeDialogFactoryImpl;
import org.apache.lucene.luke.app.desktop.components.dialog.documents.AddDocumentDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.documents.AddDocumentDialogFactoryImpl;
import org.apache.lucene.luke.app.desktop.components.dialog.documents.DocValuesDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.documents.DocValuesDialogFactoryImpl;
import org.apache.lucene.luke.app.desktop.components.dialog.documents.IndexOptionsDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.documents.IndexOptionsDialogFactoryImpl;
import org.apache.lucene.luke.app.desktop.components.dialog.documents.StoredValueDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.documents.StoredValueDialogFactoryImpl;
import org.apache.lucene.luke.app.desktop.components.dialog.documents.TermVectorDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.documents.TermVectorDialogFactoryImpl;
import org.apache.lucene.luke.app.desktop.components.dialog.menubar.AboutDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.menubar.AboutDialogFactoryImpl;
import org.apache.lucene.luke.app.desktop.components.dialog.menubar.CheckIndexDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.menubar.CheckIndexDialogFactoryImpl;
import org.apache.lucene.luke.app.desktop.components.dialog.menubar.OpenIndexDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.menubar.OpenIndexDialogFactoryImpl;
import org.apache.lucene.luke.app.desktop.components.dialog.menubar.OptimizeIndexDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.menubar.OptimizeIndexDialogFactoryImpl;
import org.apache.lucene.luke.app.desktop.components.dialog.search.ExplainDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.search.ExplainDialogFactoryImpl;
import org.apache.lucene.luke.app.desktop.components.fragments.analysis.CustomAnalyzerPanelProvider;
import org.apache.lucene.luke.app.desktop.components.fragments.analysis.PresetAnalyzerPanelProvider;
import org.apache.lucene.luke.app.desktop.components.fragments.search.AnalyzerPaneProvider;
import org.apache.lucene.luke.app.desktop.components.fragments.search.FieldValuesPaneProvider;
import org.apache.lucene.luke.app.desktop.components.fragments.search.MLTPaneProvider;
import org.apache.lucene.luke.app.desktop.components.fragments.search.QueryParserPaneProvider;
import org.apache.lucene.luke.app.desktop.components.fragments.search.SimilarityPaneProvider;
import org.apache.lucene.luke.app.desktop.components.fragments.search.SortPaneProvider;

/** Guice configuration */
public final class DesktopModule extends AbstractModule {

  private static final Injector injector = Guice.createInjector(new DesktopModule());

  public static Injector getIngector() {
    return injector;
  }

  @Override
  protected void configure() {
    // luke core module
    install(new LukeModule());

    // UI components and fragments
    bind(ComponentOperatorRegistry.class).toInstance(new ComponentOperatorRegistry());
    bind(TabSwitcherProxy.class).toInstance(new TabSwitcherProxy());
    bind(MessageBroker.class).toInstance(new MessageBroker());

    bind(JMenuBar.class).toProvider(MenuBarProvider.class);

    bind(JTextArea.class).annotatedWith(Names.named("log_area")).toInstance(new JTextArea());

    bind(JPanel.class).annotatedWith(Names.named("overview")).toProvider(OverviewPanelProvider.class);
    bind(JPanel.class).annotatedWith(Names.named("documents")).toProvider(DocumentsPanelProvider.class);
    bind(JPanel.class).annotatedWith(Names.named("search")).toProvider(SearchPanelProvider.class);
    bind(JPanel.class).annotatedWith(Names.named("analysis")).toProvider(AnalysisPanelProvider.class);
    bind(JPanel.class).annotatedWith(Names.named("commits")).toProvider(CommitsPanelProvider.class);
    bind(JPanel.class).annotatedWith(Names.named("logs")).toProvider(LogsPanelProvider.class);
    bind(JTabbedPane.class).annotatedWith(Names.named("main")).toProvider(TabbedPaneProvider.class);

    bind(JScrollPane.class).annotatedWith(Names.named("search_qparser")).toProvider(QueryParserPaneProvider.class);
    bind(JScrollPane.class).annotatedWith(Names.named("search_analyzer")).toProvider(AnalyzerPaneProvider.class);
    bind(JScrollPane.class).annotatedWith(Names.named("search_similarity")).toProvider(SimilarityPaneProvider.class);
    bind(JScrollPane.class).annotatedWith(Names.named("search_sort")).toProvider(SortPaneProvider.class);
    bind(JScrollPane.class).annotatedWith(Names.named("search_values")).toProvider(FieldValuesPaneProvider.class);
    bind(JScrollPane.class).annotatedWith(Names.named("search_mlt")).toProvider(MLTPaneProvider.class);

    bind(JPanel.class).annotatedWith(Names.named("analysis_preset")).toProvider(PresetAnalyzerPanelProvider.class);
    bind(JPanel.class).annotatedWith(Names.named("analysis_custom")).toProvider(CustomAnalyzerPanelProvider.class);

    bind(JFrame.class).toProvider(LukeWindowProvider.class);

    bind(OpenIndexDialogFactory.class).to(OpenIndexDialogFactoryImpl.class);
    bind(OptimizeIndexDialogFactory.class).to(OptimizeIndexDialogFactoryImpl.class);
    bind(CheckIndexDialogFactory.class).to(CheckIndexDialogFactoryImpl.class);
    bind(AddDocumentDialogFactory.class).to(AddDocumentDialogFactoryImpl.class);
    bind(EditFiltersDialogFactory.class).to(EditFiltersDialogFactoryImpl.class);
    bind(EditParamsDialogFactory.class).to(EditParamsDialogFactoryImpl.class);
    bind(IndexOptionsDialogFactory.class).to(IndexOptionsDialogFactoryImpl.class);
    bind(TermVectorDialogFactory.class).to(TermVectorDialogFactoryImpl.class);
    bind(DocValuesDialogFactory.class).to(DocValuesDialogFactoryImpl.class);
    bind(StoredValueDialogFactory.class).to(StoredValueDialogFactoryImpl.class);
    bind(ExplainDialogFactory.class).to(ExplainDialogFactoryImpl.class);
    bind(AnalysisChainDialogFactory.class).to(AnalysisChainDialogFactoryImpl.class);
    bind(TokenAttributeDialogFactory.class).to(TokenAttributeDialogFactoryImpl.class);
    bind(AboutDialogFactory.class).to(AboutDialogFactoryImpl.class);
    bind(HelpDialogFactory.class).to(HelpDialogFactoryImpl.class);
    bind(ConfirmDialogFactory.class).to(ConfirmDialogFactoryImpl.class);
  }

  private DesktopModule() {
  }
}
