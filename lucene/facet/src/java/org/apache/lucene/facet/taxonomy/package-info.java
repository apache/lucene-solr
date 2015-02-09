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
 * Taxonomy of Categories.
 * <p>
 * Facets are defined using a hierarchy of categories, known as a <i>Taxonomy</i>.
 * For example, the taxonomy of a book store application might have the following structure:
 * 
 * <ul>
 *   <li>Author
 *     <ul>
 *       <li>Mark Twain</li>
 *       <li>J. K. Rowling</li>
 *     </ul>
 *   </li>
 * </ul>
 * 
 * <ul>
 *   <li>Date
 *     <ul>
 *       <li>2010</li>
 *     </ul>
 *     <ul>
 *       <li>March</li>
 *       <li>April</li>
 *     </ul>
 *   </li>
 *   <li>2009</li>
 * </ul>
 * 
 * <p>
 * The <i>Taxonomy</i> translates category-paths into integer identifiers (often termed <i>ordinals</i>) and vice versa.
 * The category <code>Author/Mark Twain</code> adds two nodes to the taxonomy: <code>Author</code> and 
 * <code>Author/Mark Twain</code>, each is assigned a different ordinal. The taxonomy maintains the invariant that a 
 * node always has an ordinal that is &lt; all its children.
 */
package org.apache.lucene.facet.taxonomy;