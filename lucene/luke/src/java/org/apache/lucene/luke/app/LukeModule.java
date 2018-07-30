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

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.desktop.PreferencesImpl;
import org.apache.lucene.luke.models.analysis.AnalysisFactory;
import org.apache.lucene.luke.models.commits.CommitsFactory;
import org.apache.lucene.luke.models.documents.DocumentsFactory;
import org.apache.lucene.luke.models.overview.OverviewFactory;
import org.apache.lucene.luke.models.search.SearchFactory;
import org.apache.lucene.luke.models.tools.IndexToolsFactory;

public class LukeModule extends AbstractModule {

  private static final Injector injector = Guice.createInjector(new LukeModule());

  public static Injector getIngector() {
    return injector;
  }

  @Override
  protected void configure() {
    bind(OverviewFactory.class).toInstance(new OverviewFactory());
    bind(DocumentsFactory.class).toInstance(new DocumentsFactory());
    bind(SearchFactory.class).toInstance(new SearchFactory());
    bind(AnalysisFactory.class).toInstance(new AnalysisFactory());
    bind(CommitsFactory.class).toInstance(new CommitsFactory());
    bind(IndexToolsFactory.class).toInstance(new IndexToolsFactory());

    bind(DirectoryHandler.class).toInstance(new DirectoryHandler());
    bind(IndexHandler.class).toInstance(new IndexHandler());

    bind(Preferences.class).to(PreferencesImpl.class);
  }

  private LukeModule() {}
}
