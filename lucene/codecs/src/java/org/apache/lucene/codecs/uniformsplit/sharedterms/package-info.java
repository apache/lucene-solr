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
 * Pluggable term index / block terms dictionary implementations.
 *
 * <p>Extension of {@link org.apache.lucene.codecs.uniformsplit} with Shared Terms principle: Terms
 * are shared between all fields. It is particularly adapted to index a massive number of fields
 * because all the terms are stored in a single FST dictionary.
 *
 * <ul>
 *   <li>Designed to be extensible
 *   <li>Highly reduced on-heap memory usage when dealing with a massive number of fields.
 * </ul>
 */
package org.apache.lucene.codecs.uniformsplit.sharedterms;
