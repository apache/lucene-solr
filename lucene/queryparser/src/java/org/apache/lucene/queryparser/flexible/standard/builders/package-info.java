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
 * Standard Lucene Query Node Builders.
 *
 * <h2>Standard Lucene Query Node Builders</h2>
 *
 * <p>The package org.apache.lucene.queryparser.flexible.standard.builders contains all the builders
 * needed to build a Lucene Query object from a query node tree. These builders expect the query
 * node tree was already processed by the {@link
 * org.apache.lucene.queryparser.flexible.standard.processors.StandardQueryNodeProcessorPipeline}.
 *
 * <p>{@link org.apache.lucene.queryparser.flexible.standard.builders.StandardQueryTreeBuilder} is a
 * builder that already contains a defined map that maps each QueryNode object with its respective
 * builder.
 */
package org.apache.lucene.queryparser.flexible.standard.builders;
