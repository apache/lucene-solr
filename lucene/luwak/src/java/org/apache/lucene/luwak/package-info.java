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
 * <h2>Monitoring framework</h2>
 *
 * This package contains classes to allow the monitoring of a stream of
 * documents with a set of queries.
 *
 * To use, instantiate a {@link org.apache.lucene.luwak.Monitor} object,
 * register queries with it via
 * {@link org.apache.lucene.luwak.Monitor#register(org.apache.lucene.luwak.MonitorQuery...)},
 * and then match documents against it either invidually via
 * {@link org.apache.lucene.luwak.Monitor#match(org.apache.lucene.luwak.InputDocument, org.apache.lucene.luwak.MatcherFactory)}
 * or in batches via {@link org.apache.lucene.luwak.Monitor#match(org.apache.lucene.luwak.DocumentBatch, org.apache.lucene.luwak.MatcherFactory)}
 *
 */
package org.apache.lucene.luwak;