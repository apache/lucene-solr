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
 * Create queries for the test.
 */
public interface QueryMaker {

  /** 
   * Create the next query, of the given size.
   * @param size the size of the query - number of terms, etc.
   * @exception Exception if cannot make the query, or if size &gt; 0 was specified but this feature is not supported.
   */ 
  public Query makeQuery (int size) throws Exception;

  /** Create the next query */ 
  public Query makeQuery () throws Exception;

  /** Set the properties */
  public void setConfig (Config config) throws Exception;
  
  /** Reset inputs so that the test run would behave, input wise, as if it just started. */
  public void resetInputs() throws Exception;
  
  /** Print the queries */
  public String printQueries();
}
