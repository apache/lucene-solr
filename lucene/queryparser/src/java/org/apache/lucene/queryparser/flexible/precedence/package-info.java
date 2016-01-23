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
 * Precedence Query Parser Implementation
 * 
 * <h2>Lucene Precedence Query Parser</h2>
 * 
 * <p>
 * The Precedence Query Parser extends the Standard Query Parser and enables 
 * the boolean precedence. So, the query &lt;a AND b OR c AND d&gt; is parsed to 
 * &lt;(+a +b) (+c +d)&gt; instead of &lt;+a +b +c +d&gt;.
 * <p>
 * Check {@link org.apache.lucene.queryparser.flexible.standard.StandardQueryParser} for more details about the
 * supported syntax and query parser functionalities. 
 */
package org.apache.lucene.queryparser.flexible.precedence;

