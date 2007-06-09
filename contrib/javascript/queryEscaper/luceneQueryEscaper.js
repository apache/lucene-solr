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

// Special characters are + - && || ! ( ) { } [ ] ^ " ~ * ? : \
// Special words are (case-sensitive) AND NOT OR
// We escape the common ones, i.e. ! ? * ( ) " :

// escapes a lucene query.
// @param Form field that contains the query, or the query string
function doEscapeQuery(queryArg)
{
  var query = getQueryValue(queryArg);
  query = escapeAsterisk(query);
  query = escapeQuotes(query);
  query = escapeColon(query);
  query = escapeQuestionMark(query);
  query = escapeExclamationMark(query);
  query = escapeParentheses(query);  
  query = escapeSquareBrackets(query);  
  query = escapeBraces(query);  
  query = escapeCaret(query);  
  query = escapeSquiggle(query);  
  query = escapeDoubleAmpersands(query);  
  query = escapeDoubleBars(query);  
  return query;
}

function getQueryValue(queryArg)
{
  var query;
  // check if its a form field
  if(typeof(queryArg.form) != "undefined")
  {
    query = queryArg.value;
  }
  else
  {
    query = queryArg;
  }
  return query;
}

function escapeAsterisk(query)
{
  return query.replace(/[\*]/g, "\\*");
}

function escapeQuotes(query)
{
  return query.replace(/[\"]/g, "\\\"");
}

function escapeColon(query)
{
  return query.replace(/[\:]/g, "\\:");
}

function escapeQuestionMark(query)
{
  return query.replace(/[?]/g, "\\?");
}

function escapeExclamationMark(query)
{
  return query.replace(/[!]/g, "\\!");
}

function escapeParentheses(query)
{
  return query.replace(/[(]/g, "\\(").replace(/[)]/g, "\\)");
}

function escapeSquareBrackets(query)
{
  return query.replace(/[\[]/g, "\\[").replace(/[\]]/g, "\\]");
}

function escapeBraces(query)
{
  return query.replace(/[{]/g, "\\{").replace(/[}]/g, "\\}");
}

function escapeCaret(query)
{
  return query.replace(/[\^]/g, "\\^");
}

function escapeSquiggle(query)
{
  return query.replace(/[~]/g, "\\~");
}

function escapeDoubleAmpersands(query)
{
  return query.replace(/[&]{2}/g, "\\&\\&");
}

function escapeDoubleBars(query)
{
  return query.replace(/[\|]{2}/g, "\\|\\|");
}
