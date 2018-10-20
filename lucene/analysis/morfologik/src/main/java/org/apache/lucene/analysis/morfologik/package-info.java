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
 * This package provides dictionary-driven lemmatization ("accurate stemming")
 * filter and analyzer for the Polish Language, driven by the
 * <a href="http://morfologik.blogspot.com/">Morfologik library</a> developed 
 * by Dawid Weiss and Marcin Miłkowski.
 * <p>
 * The MorfologikFilter yields one or more terms for each token. Each
 * of those terms is given the same position in the index.
 * </p>
 */
package org.apache.lucene.analysis.morfologik;

