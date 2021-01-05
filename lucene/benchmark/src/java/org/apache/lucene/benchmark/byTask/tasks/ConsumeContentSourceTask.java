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
package org.apache.lucene.benchmark.byTask.tasks;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.ContentSource;
import org.apache.lucene.benchmark.byTask.feeds.DocData;

/** Consumes a {@link org.apache.lucene.benchmark.byTask.feeds.ContentSource}. */
public class ConsumeContentSourceTask extends PerfTask {

  private final ContentSource source;
  private ThreadLocal<DocData> dd = new ThreadLocal<>();

  public ConsumeContentSourceTask(PerfRunData runData) {
    super(runData);
    source = runData.getContentSource();
  }

  @Override
  protected String getLogMessage(int recsCount) {
    return "read " + recsCount + " documents from the content source";
  }

  @Override
  public int doLogic() throws Exception {
    dd.set(source.getNextDocData(dd.get()));
    return 1;
  }
}
