package org.apache.lucene.facet.search.aggregator;

import java.io.IOException;

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

/**
 * An Aggregator is the analogue of Lucene's Collector (see
 * {@link org.apache.lucene.search.Collector}), for processing the categories
 * belonging to a certain document. The Aggregator is responsible for doing
 * whatever it wishes with the categories it is fed, e.g., counting the number
 * of times that each category appears, or performing some computation on their
 * association values.
 * <P>
 * Much of the function of an Aggregator implementation is not described by this
 * interface. This includes the constructor and getter methods to retrieve the
 * results of the aggregation.
 * 
 * @lucene.experimental
 */
public interface Aggregator {

  /**
   * Specify the document (and its score in the search) that the following
   * {@link #aggregate(int)} calls will pertain to.
   */
  void setNextDoc(int docid, float score) throws IOException;

  /**
   * Collect (and do whatever an implementation deems appropriate) the
   * category given by its ordinal. This category belongs to a document
   * given earlier by {@link #setNextDoc(int, float)}.
   */
  void aggregate(int ordinal);

}
