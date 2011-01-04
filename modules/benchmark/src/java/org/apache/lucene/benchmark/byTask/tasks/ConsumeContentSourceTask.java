package org.apache.lucene.benchmark.byTask.tasks;

/**
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
import org.apache.lucene.benchmark.byTask.feeds.ContentSource;
import org.apache.lucene.benchmark.byTask.feeds.DocData;
import org.apache.lucene.benchmark.byTask.utils.Config;

/**
 * Consumes a {@link org.apache.lucene.benchmark.byTask.feeds.ContentSource}.
 * Supports the following parameters:
 * <ul>
 * <li>content.source - the content source to use. (mandatory)
 * </ul>
 */
public class ConsumeContentSourceTask extends PerfTask {

  private ContentSource source;
  private DocData dd = new DocData();
  
  public ConsumeContentSourceTask(PerfRunData runData) {
    super(runData);
    Config config = runData.getConfig();
    String sourceClass = config.get("content.source", null);
    if (sourceClass == null) {
      throw new IllegalArgumentException("content.source must be defined");
    }
    try {
      source = Class.forName(sourceClass).asSubclass(ContentSource.class).newInstance();
      source.setConfig(config);
      source.resetInputs();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected String getLogMessage(int recsCount) {
    return "read " + recsCount + " documents from the content source";
  }
  
  @Override
  public void close() throws Exception {
    source.close();
    super.close();
  }

  @Override
  public int doLogic() throws Exception {
    dd = source.getNextDocData(dd);
    return 1;
  }

}
