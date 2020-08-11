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
 * This package contains several components useful to build a highlighter
 * on top of the {@link org.apache.lucene.search.Matches} API.
 *
 * {@link org.apache.lucene.search.matchhighlight.MatchRegionRetriever} can be
 * used to retrieve hit areas for a given {@link org.apache.lucene.search.Query}
 * and one (or more) indexed documents. These hit areas can be then passed to
 * {@link org.apache.lucene.search.matchhighlight.PassageSelector} and formatted
 * with {@link org.apache.lucene.search.matchhighlight.PassageFormatter}.
 */
package org.apache.lucene.search.matchhighlight;
