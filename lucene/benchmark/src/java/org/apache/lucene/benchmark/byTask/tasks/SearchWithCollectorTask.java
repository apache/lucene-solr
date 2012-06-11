package org.apache.lucene.benchmark.byTask.tasks;
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

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.QueryMaker;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.TopScoreDocCollector;

/**
 * Does search w/ a custom collector
 */
public class SearchWithCollectorTask extends SearchTask {

  protected String clnName;

  public SearchWithCollectorTask(PerfRunData runData) {
    super(runData);
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    //check to make sure either the doc is being stored
    PerfRunData runData = getRunData();
    Config config = runData.getConfig();
    clnName = config.get("collector.class", "");
  }

  

  @Override
  public boolean withCollector() {
    return true;
  }

  @Override
  protected Collector createCollector() throws Exception {
    Collector collector = null;
    if (clnName.equalsIgnoreCase("topScoreDocOrdered") == true) {
      collector = TopScoreDocCollector.create(numHits(), true);
    } else if (clnName.equalsIgnoreCase("topScoreDocUnOrdered") == true) {
      collector = TopScoreDocCollector.create(numHits(), false);
    } else if (clnName.length() > 0){
      collector = Class.forName(clnName).asSubclass(Collector.class).newInstance();

    } else {
      collector = super.createCollector();
    }
    return collector;
  }

  @Override
  public QueryMaker getQueryMaker() {
    return getRunData().getQueryMaker(this);
  }

  @Override
  public boolean withRetrieve() {
    return false;
  }

  @Override
  public boolean withSearch() {
    return true;
  }

  @Override
  public boolean withTraverse() {
    return false;
  }

  @Override
  public boolean withWarm() {
    return false;
  }

}
