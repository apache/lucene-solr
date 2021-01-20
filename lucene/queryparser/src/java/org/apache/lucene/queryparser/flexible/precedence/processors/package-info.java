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
 * Lucene Precedence Query Parser Processors
 *
 * <p>This package contains the 2 {@link
 * org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessor}s used by {@link
 * org.apache.lucene.queryparser.flexible.precedence.PrecedenceQueryParser}.
 *
 * <p>{@link
 * org.apache.lucene.queryparser.flexible.precedence.processors.BooleanModifiersQueryNodeProcessor}:
 * this processor is used to apply {@link
 * org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode}s on {@link
 * org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode} children according to the
 * boolean type or the default operator.
 *
 * <p>{@link
 * org.apache.lucene.queryparser.flexible.precedence.processors.PrecedenceQueryNodeProcessorPipeline}:
 * this processor pipeline is used by {@link
 * org.apache.lucene.queryparser.flexible.precedence.PrecedenceQueryParser}. It extends {@link
 * org.apache.lucene.queryparser.flexible.standard.processors.StandardQueryNodeProcessorPipeline}
 * and rearrange the pipeline so the boolean precedence is processed correctly. Check {@link
 * org.apache.lucene.queryparser.flexible.precedence.processors.PrecedenceQueryNodeProcessorPipeline}
 * for more details.
 */
package org.apache.lucene.queryparser.flexible.precedence.processors;
