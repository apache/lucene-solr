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
package org.apache.lucene.benchmark.byTask.feeds;

import com.ibm.icu.text.RuleBasedNumberFormat;
import java.util.Locale;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.benchmark.byTask.tasks.NewAnalyzerTask;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

/**
 * Creates queries whose content is a spelled-out <code>long</code> number starting from <code>
 * {@link Long#MIN_VALUE} + 10</code>.
 */
public class LongToEnglishQueryMaker implements QueryMaker {
  long counter = Long.MIN_VALUE + 10;
  protected QueryParser parser;

  // TODO: we could take param to specify locale...
  private final RuleBasedNumberFormat rnbf =
      new RuleBasedNumberFormat(Locale.ROOT, RuleBasedNumberFormat.SPELLOUT);

  @Override
  public Query makeQuery(int size) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized Query makeQuery() throws Exception {
    return parser.parse("" + rnbf.format(getNextCounter()) + "");
  }

  private synchronized long getNextCounter() {
    if (counter == Long.MAX_VALUE) {
      counter = Long.MIN_VALUE + 10;
    }
    return counter++;
  }

  @Override
  public void setConfig(Config config) throws Exception {
    Analyzer anlzr =
        NewAnalyzerTask.createAnalyzer(config.get("analyzer", StandardAnalyzer.class.getName()));
    parser = new QueryParser(DocMaker.BODY_FIELD, anlzr);
  }

  @Override
  public void resetInputs() {
    counter = Long.MIN_VALUE + 10;
  }

  @Override
  public String printQueries() {
    return "LongToEnglish: [" + Long.MIN_VALUE + " TO " + counter + "]";
  }
}
