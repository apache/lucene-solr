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
package org.apache.solr.search.grouping;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import java.io.IOException;
import java.util.List;

/**
 * Defines a grouping command.
 * This is an abstraction on how the {@link Collector} instances are created
 * and how the results are retrieved from the {@link Collector} instances.
 *
 * @lucene.experimental
 */
public interface Command<T> {

  /**
   * Returns a list of {@link Collector} instances to be
   * included in the search based on the .
   *
   * @return a list of {@link Collector} instances
   * @throws IOException If I/O related errors occur
   */
  List<Collector> create() throws IOException;

  /**
   * Run post-collection steps.
   * @throws IOException If I/O related errors occur
   */
  default void postCollect(IndexSearcher searcher) throws IOException {}

  /**
   * Returns the results that the collectors created
   * by {@link #create()} contain after a search has been executed.
   *
   * @return The results of the collectors
   */
  T result() throws IOException;

  /**
   * @return The key of this command to uniquely identify itself
   */
  String getKey();

  /**
   * @return The group sort (overall sort)
   */
  Sort getGroupSort();

  /**
   * @return The sort inside a group
   */
  Sort getWithinGroupSort();

}
