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
PR.registerLangHandler(PR.createSimpleLexer([["pln",/^[\t\n\r \xa0]+/,null,"\t\n\r �\xa0"],["str",/^(?:"(?:[^"\\]|\\[\S\s])*(?:"|$)|'(?:[^'\\]|\\[\S\s])*(?:'|$))/,null,"\"'"]],[["com",/^--(?:\[(=*)\[[\S\s]*?(?:]\1]|$)|[^\n\r]*)/],["str",/^\[(=*)\[[\S\s]*?(?:]\1]|$)/],["kwd",/^(?:and|break|do|else|elseif|end|false|for|function|if|in|local|nil|not|or|repeat|return|then|true|until|while)\b/,null],["lit",/^[+-]?(?:0x[\da-f]+|(?:\.\d+|\d+(?:\.\d*)?)(?:e[+-]?\d+)?)/i],
["pln",/^[_a-z]\w*/i],["pun",/^[^\w\t\n\r \xa0][^\w\t\n\r "'+=\xa0-]*/]]),["lua"]);
