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
 * Javascript expressions.
 * <p>A Javascript expression is a numeric expression specified using an expression syntax that's based on JavaScript expressions. You can construct expressions using:</p>
 * <ul>
 * <li>Integer, floating point, hex and octal literals</li>
 * <li>Arithmetic operators: <code>+ - * / %</code></li>
 * <li>Bitwise operators: <code>| &amp; ^ ~ &lt;&lt; &gt;&gt; &gt;&gt;&gt;</code></li>
 * <li>Boolean operators (including the ternary operator): <code>&amp;&amp; || ! ?:</code></li>
 * <li>Comparison operators: <code>&lt; &lt;= == &gt;= &gt;</code></li>
 * <li>Common mathematic functions: <code>abs ceil exp floor ln log10 logn max min sqrt pow</code></li>
 * <li>Trigonometric library functions: <code>acosh acos asinh asin atanh atan atan2 cosh cos sinh sin tanh tan</code></li>
 * <li>Distance functions: <code>haversin</code></li>
 * <li>Miscellaneous functions: <code>min, max</code></li>
 * <li>Arbitrary external variables - see {@link org.apache.lucene.expressions.Bindings}</li>
 * </ul>
 * 
 * <p>
 * JavaScript order of precedence rules apply for operators. Shortcut evaluation is used for logical operators—the second argument is only evaluated if the value of the expression cannot be determined after evaluating the first argument. For example, in the expression <code>a || b</code>, <code>b</code> is only evaluated if a is not true.
 * </p>
 * 
 * <p>
 * To compile an expression, use {@link org.apache.lucene.expressions.js.JavascriptCompiler}.
 * </p>
 */
package org.apache.lucene.expressions.js;
