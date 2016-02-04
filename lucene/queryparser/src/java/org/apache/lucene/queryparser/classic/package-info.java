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
 * A simple query parser implemented with JavaCC.
 *
 * <p>Note that JavaCC defines lots of public classes, methods and fields
 * that do not need to be public.&nbsp; These clutter the documentation.&nbsp;
 * Sorry.
 * <p>Note that because JavaCC defines a class named <tt>Token</tt>, <tt>org.apache.lucene.analysis.Token</tt>
 * must always be fully qualified in source code in this package.
 *
 * <p><b>NOTE</b>: {@link org.apache.lucene.queryparser.flexible.standard} has an alternative queryparser that matches the syntax of this one, but is more modular,
 * enabling substantial customization to how a query is created.
 *
 * <h2>Query Parser Syntax</h2>
 *
 * <div id="minitoc-area">
 * <ul class="minitoc">
 * <li>
 * <a href="#Overview">Overview</a>
 * </li>
 * <li>
 * <a href="#Terms">Terms</a>
 * </li>
 * <li>
 * <a href="#Fields">Fields</a>
 * </li>
 * <li>
 * <a href="#Term_Modifiers">Term Modifiers</a>
 * <ul class="minitoc">
 * <li>
 * <a href="#Wildcard_Searches">Wildcard Searches</a>
 * </li>
 * <li>
 * <a href="#Regexp_Searches">Regular expression Searches</a>
 * </li>
 * <li>
 * <a href="#Fuzzy_Searches">Fuzzy Searches</a>
 * </li>
 * <li>
 * <a href="#Proximity_Searches">Proximity Searches</a>
 * </li>
 * <li>
 * <a href="#Range_Searches">Range Searches</a>
 * </li>
 * <li>
 * <a href="#Boosting_a_Term">Boosting a Term</a>
 * </li>
 * </ul>
 * </li>
 * <li>
 * <a href="#Boolean_operators">Boolean Operators</a>
 * <ul class="minitoc">
 * <li>
 * <a href="#OR">OR</a>
 * </li>
 * <li>
 * <a href="#AND">AND</a>
 * </li>
 * <li>
 * <a href="#+">+</a>
 * </li>
 * <li>
 * <a href="#NOT">NOT</a>
 * </li>
 * <li>
 * <a href="#-">-</a>
 * </li>
 * </ul>
 * </li>
 * <li>
 * <a href="#Grouping">Grouping</a>
 * </li>
 * <li>
 * <a href="#Field_Grouping">Field Grouping</a>
 * </li>
 * <li>
 * <a href="#Escaping_Special_Characters">Escaping Special Characters</a>
 * </li>
 * </ul>
 * </div>
 *         
 * <a name="N10013"></a><a name="Overview"></a>
 * <h2 class="boxed">Overview</h2>
 * <div class="section">
 * <p>Although Lucene provides the ability to create your own
 *             queries through its API, it also provides a rich query
 *             language through the Query Parser, a lexer which
 *             interprets a string into a Lucene Query using JavaCC.
 * <p>Generally, the query parser syntax may change from
 *         release to release.  This page describes the syntax as of
 *         the current release.  If you are using a different
 *         version of Lucene, please consult the copy of
 *         <span class="codefrag">docs/queryparsersyntax.html</span> that was distributed
 *         with the version you are using.
 * <p>
 *             Before choosing to use the provided Query Parser, please consider the following:
 *             <ol>
 *             
 * <li>If you are programmatically generating a query string and then
 *             parsing it with the query parser then you should seriously consider building
 *             your queries directly with the query API.  In other words, the query
 *             parser is designed for human-entered text, not for program-generated
 *             text.</li>
 * 
 *             
 * <li>Untokenized fields are best added directly to queries, and not
 *             through the query parser.  If a field's values are generated programmatically
 *             by the application, then so should query clauses for this field.
 *             An analyzer, which the query parser uses, is designed to convert human-entered
 *             text to terms.  Program-generated values, like dates, keywords, etc.,
 *             should be consistently program-generated.</li>
 * 
 *             
 * <li>In a query form, fields which are general text should use the query
 *             parser.  All others, such as date ranges, keywords, etc. are better added
 *             directly through the query API.  A field with a limit set of values,
 *             that can be specified with a pull-down menu should not be added to a
 *             query string which is subsequently parsed, but rather added as a
 *             TermQuery clause.</li>
 *             
 * </ol>
 *           
 * </div>
 * 
 *         
 * <a name="N10032"></a><a name="Terms"></a>
 * <h2 class="boxed">Terms</h2>
 * <div class="section">
 * <p>A query is broken up into terms and operators. There are two types of terms: Single Terms and Phrases.
 * <p>A Single Term is a single word such as "test" or "hello".
 * <p>A Phrase is a group of words surrounded by double quotes such as "hello dolly".
 * <p>Multiple terms can be combined together with Boolean operators to form a more complex query (see below).
 * <p>Note: The analyzer used to create the index will be used on the terms and phrases in the query string.
 *         So it is important to choose an analyzer that will not interfere with the terms used in the query string.
 * </div>
 * 
 *         
 * <a name="N10048"></a><a name="Fields"></a>
 * <h2 class="boxed">Fields</h2>
 * <div class="section">
 * <p>Lucene supports fielded data. When performing a search you can either specify a field, or use the default field. The field names and default field is implementation specific.
 * <p>You can search any field by typing the field name followed by a colon ":" and then the term you are looking for.
 * <p>As an example, let's assume a Lucene index contains two fields, title and text and text is the default field.
 *         If you want to find the document entitled "The Right Way" which contains the text "don't go this way", you can enter:
 * <pre class="code">title:"The Right Way" AND text:go</pre>
 * <p>or
 * <pre class="code">title:"The Right Way" AND go</pre>
 * <p>Since text is the default field, the field indicator is not required.
 * <p>Note: The field is only valid for the term that it directly precedes, so the query
 * <pre class="code">title:The Right Way</pre>
 * <p>Will only find "The" in the title field. It will find "Right" and "Way" in the default field (in this case the text field).
 * </div>
 * 
 *         
 * <a name="N1006D"></a><a name="Term_Modifiers"></a>
 * <h2 class="boxed">Term Modifiers</h2>
 * <div class="section">
 * <p>Lucene supports modifying query terms to provide a wide range of searching options.
 * <a name="N10076"></a><a name="Wildcard_Searches"></a>
 * <h3 class="boxed">Wildcard Searches</h3>
 * <p>Lucene supports single and multiple character wildcard searches within single terms
 *         (not within phrase queries).
 * <p>To perform a single character wildcard search use the "?" symbol.
 * <p>To perform a multiple character wildcard search use the "*" symbol.
 * <p>The single character wildcard search looks for terms that match that with the single character replaced. For example, to search for "text" or "test" you can use the search:
 * <pre class="code">te?t</pre>
 * <p>Multiple character wildcard searches looks for 0 or more characters. For example, to search for test, tests or tester, you can use the search: 
 * <pre class="code">test*</pre>
 * <p>You can also use the wildcard searches in the middle of a term.
 * <pre class="code">te*t</pre>
 * <p>Note: You cannot use a * or ? symbol as the first character of a search.
 * <a name="Regexp_Searches"></a>
 * <h3 class="boxed">Regular Expression Searches</h3>
 * <p>Lucene supports regular expression searches matching a pattern between forward slashes "/". The syntax may change across releases, but the current supported
 * syntax is documented in the {@link org.apache.lucene.util.automaton.RegExp RegExp} class. For example to find documents containing "moat" or "boat":
 * 
 * <pre class="code">/[mb]oat/</pre>
 * <a name="N1009B"></a><a name="Fuzzy_Searches"></a>
 * <h3 class="boxed">Fuzzy Searches</h3>
 * <p>Lucene supports fuzzy searches based on Damerau-Levenshtein Distance. To do a fuzzy search use the tilde, "~", symbol at the end of a Single word Term. For example to search for a term similar in spelling to "roam" use the fuzzy search: 
 * <pre class="code">roam~</pre>
 * <p>This search will find terms like foam and roams.
 * <p>An additional (optional) parameter can specify the maximum number of edits allowed. The value is between 0 and 2, For example:
 * <pre class="code">roam~1</pre>
 * <p>The default that is used if the parameter is not given is 2 edit distances.
 * <p>Previously, a floating point value was allowed here. This syntax is considered deprecated and will be removed in Lucene 5.0
 * <a name="N100B4"></a><a name="Proximity_Searches"></a>
 * <h3 class="boxed">Proximity Searches</h3>
 * <p>Lucene supports finding words are a within a specific distance away. To do a proximity search use the tilde, "~", symbol at the end of a Phrase. For example to search for a "apache" and "jakarta" within 10 words of each other in a document use the search:
 * <pre class="code">"jakarta apache"~10</pre>
 * <a name="N100C1"></a><a name="Range_Searches"></a>
 * <h3 class="boxed">Range Searches</h3>
 * <p>Range Queries allow one to match documents whose field(s) values
 *             are between the lower and upper bound specified by the Range Query.
 *             Range Queries can be inclusive or exclusive of the upper and lower bounds.
 *             Sorting is done lexicographically.
 * <pre class="code">mod_date:[20020101 TO 20030101]</pre>
 * <p>This will find documents whose mod_date fields have values between 20020101 and 20030101, inclusive.
 *             Note that Range Queries are not reserved for date fields.  You could also use range queries with non-date fields:
 * <pre class="code">title:{Aida TO Carmen}</pre>
 * <p>This will find all documents whose titles are between Aida and Carmen, but not including Aida and Carmen.
 * <p>Inclusive range queries are denoted by square brackets.  Exclusive range queries are denoted by
 *             curly brackets.
 * <a name="N100DA"></a><a name="Boosting_a_Term"></a>
 * <h3 class="boxed">Boosting a Term</h3>
 * <p>Lucene provides the relevance level of matching documents based on the terms found. To boost a term use the caret, "^", symbol with a boost factor (a number) at the end of the term you are searching. The higher the boost factor, the more relevant the term will be.
 * <p>Boosting allows you to control the relevance of a document by boosting its term. For example, if you are searching for
 * <pre class="code">jakarta apache</pre>
 * <p>and you want the term "jakarta" to be more relevant boost it using the ^ symbol along with the boost factor next to the term.
 *         You would type:
 * <pre class="code">jakarta^4 apache</pre>
 * <p>This will make documents with the term jakarta appear more relevant. You can also boost Phrase Terms as in the example: 
 * <pre class="code">"jakarta apache"^4 "Apache Lucene"</pre>
 * <p>By default, the boost factor is 1. Although the boost factor must be positive, it can be less than 1 (e.g. 0.2)
 * </div>
 * 
 * 
 *         
 * <a name="N100FA"></a><a name="Boolean_operators"></a>
 * <h2 class="boxed">Boolean Operators</h2>
 * <div class="section">
 * <p>Boolean operators allow terms to be combined through logic operators.
 *         Lucene supports AND, "+", OR, NOT and "-" as Boolean operators(Note: Boolean operators must be ALL CAPS).
 * <a name="N10103"></a><a name="OR"></a>
 * <h3 class="boxed">OR</h3>
 * <p>The OR operator is the default conjunction operator. This means that if there is no Boolean operator between two terms, the OR operator is used.
 *         The OR operator links two terms and finds a matching document if either of the terms exist in a document. This is equivalent to a union using sets.
 *         The symbol || can be used in place of the word OR.
 * <p>To search for documents that contain either "jakarta apache" or just "jakarta" use the query:
 * <pre class="code">"jakarta apache" jakarta</pre>
 * <p>or
 * <pre class="code">"jakarta apache" OR jakarta</pre>
 * <a name="N10116"></a><a name="AND"></a>
 * <h3 class="boxed">AND</h3>
 * <p>The AND operator matches documents where both terms exist anywhere in the text of a single document.
 *         This is equivalent to an intersection using sets. The symbol &amp;&amp; can be used in place of the word AND.
 * <p>To search for documents that contain "jakarta apache" and "Apache Lucene" use the query: 
 * <pre class="code">"jakarta apache" AND "Apache Lucene"</pre>
 * <a name="N10126"></a>
 * <h3 class="boxed">+</h3>
 * <p>The "+" or required operator requires that the term after the "+" symbol exist somewhere in a the field of a single document.
 * <p>To search for documents that must contain "jakarta" and may contain "lucene" use the query:
 * <pre class="code">+jakarta lucene</pre>
 * <a name="N10136"></a><a name="NOT"></a>
 * <h3 class="boxed">NOT</h3>
 * <p>The NOT operator excludes documents that contain the term after NOT.
 *         This is equivalent to a difference using sets. The symbol ! can be used in place of the word NOT.
 * <p>To search for documents that contain "jakarta apache" but not "Apache Lucene" use the query: 
 * <pre class="code">"jakarta apache" NOT "Apache Lucene"</pre>
 * <p>Note: The NOT operator cannot be used with just one term. For example, the following search will return no results:
 * <pre class="code">NOT "jakarta apache"</pre>
 * <a name="N1014C"></a>
 * <h3 class="boxed">-</h3>
 * <p>The "-" or prohibit operator excludes documents that contain the term after the "-" symbol.
 * <p>To search for documents that contain "jakarta apache" but not "Apache Lucene" use the query: 
 * <pre class="code">"jakarta apache" -"Apache Lucene"</pre>
 * </div>
 * 
 *         
 * <a name="N1015D"></a><a name="Grouping"></a>
 * <h2 class="boxed">Grouping</h2>
 * <div class="section">
 * <p>Lucene supports using parentheses to group clauses to form sub queries. This can be very useful if you want to control the boolean logic for a query.
 * <p>To search for either "jakarta" or "apache" and "website" use the query:
 * <pre class="code">(jakarta OR apache) AND website</pre>
 * <p>This eliminates any confusion and makes sure you that website must exist and either term jakarta or apache may exist.
 * </div>
 * 
 *         
 * <a name="N10170"></a><a name="Field_Grouping"></a>
 * <h2 class="boxed">Field Grouping</h2>
 * <div class="section">
 * <p>Lucene supports using parentheses to group multiple clauses to a single field.
 * <p>To search for a title that contains both the word "return" and the phrase "pink panther" use the query:
 * <pre class="code">title:(+return +"pink panther")</pre>
 * </div>
 * 
 *         
 * <a name="N10180"></a><a name="Escaping_Special_Characters"></a>
 * <h2 class="boxed">Escaping Special Characters</h2>
 * <div class="section">
 * <p>Lucene supports escaping special characters that are part of the query syntax. The current list special characters are
 * <p>+ - &amp;&amp; || ! ( ) { } [ ] ^ " ~ * ? : \ /
 * <p>To escape these character use the \ before the character. For example to search for (1+1):2 use the query:
 * <pre class="code">\(1\+1\)\:2</pre>
 * </div>
 */
package org.apache.lucene.queryparser.classic;
