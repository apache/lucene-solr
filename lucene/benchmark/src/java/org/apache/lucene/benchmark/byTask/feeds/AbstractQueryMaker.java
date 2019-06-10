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

import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.search.Query;

/**
 * Abstract base query maker. 
 * Each query maker should just implement the {@link #prepareQueries()} method.
 **/
public abstract class AbstractQueryMaker implements QueryMaker {

  protected int qnum = 0;
  protected Query[] queries;
  protected Config config;

  @Override
  public void resetInputs() throws Exception {
    qnum = 0;
    // re-initialize since properties by round may have changed.
    setConfig(config);
  }

  protected abstract Query[] prepareQueries() throws Exception;

  @Override
  public void setConfig(Config config) throws Exception {
    this.config = config;
    queries = prepareQueries();
  }

  @Override
  public String printQueries() {
    String newline = System.getProperty("line.separator");
    StringBuilder sb = new StringBuilder();
    if (queries != null) {
      for (int i = 0; i < queries.length; i++) {
        sb.append(i).append(". ").append(queries[i].getClass().getSimpleName()).append(" - ").append(queries[i].toString());
        sb.append(newline);
      }
    }
    return sb.toString();
  }

  @Override
  public Query makeQuery() throws Exception {
    return queries[nextQnum()];
  }
  
  // return next qnum
  protected synchronized int nextQnum() {
    int res = qnum;
    qnum = (qnum+1) % queries.length;
    return res;
  }

  /*
  *  (non-Javadoc)
  * @see org.apache.lucene.benchmark.byTask.feeds.QueryMaker#makeQuery(int)
  */
  @Override
  public Query makeQuery(int size) throws Exception {
    throw new Exception(this+".makeQuery(int size) is not supported!");
  }
}
