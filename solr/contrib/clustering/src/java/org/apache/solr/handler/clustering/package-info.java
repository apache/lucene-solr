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
 * A {@link org.apache.solr.handler.component.SearchComponent} for dynamic,
 * unsupervised grouping of
 * search results based on the content of their text fields or contextual
 * snippets around query-matching regions.
 *
 * <p>
 * The default implementation uses clustering algorithms from the
 * <a href="https://project.carrot2.org">Carrot<sup>2</sup> project</a>.
 */
package org.apache.solr.handler.clustering;




