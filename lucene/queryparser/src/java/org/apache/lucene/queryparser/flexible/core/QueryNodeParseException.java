package org.apache.lucene.queryparser.flexible.core;

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

import org.apache.lucene.queryparser.flexible.messages.Message;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.parser.SyntaxParser;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

/**
 * This should be thrown when an exception happens during the query parsing from
 * string to the query node tree.
 * 
 * @see QueryNodeException
 * @see SyntaxParser
 * @see QueryNode
 */
public class QueryNodeParseException extends QueryNodeException {

  private CharSequence query;

  private int beginColumn = -1;

  private int beginLine = -1;

  private String errorToken = "";

  public QueryNodeParseException(Message message) {
    super(message);
  }

  public QueryNodeParseException(Throwable throwable) {
    super(throwable);
  }

  public QueryNodeParseException(Message message, Throwable throwable) {
    super(message, throwable);
  }

  public void setQuery(CharSequence query) {
    this.query = query;
    this.message = new MessageImpl(
        QueryParserMessages.INVALID_SYNTAX_CANNOT_PARSE, query, "");
  }

  public CharSequence getQuery() {
    return this.query;
  }

  /**
   * @param errorToken
   *          the errorToken in the query
   */
  protected void setErrorToken(String errorToken) {
    this.errorToken = errorToken;
  }

  public String getErrorToken() {
    return this.errorToken;
  }

  public void setNonLocalizedMessage(Message message) {
    this.message = message;
  }

  /**
   * For EndOfLine and EndOfFile ("&lt;EOF&gt;") parsing problems the last char in the
   * string is returned For the case where the parser is not able to figure out
   * the line and column number -1 will be returned
   * 
   * @return line where the problem was found
   */
  public int getBeginLine() {
    return this.beginLine;
  }

  /**
   * For EndOfLine and EndOfFile ("&lt;EOF&gt;") parsing problems the last char in the
   * string is returned For the case where the parser is not able to figure out
   * the line and column number -1 will be returned
   * 
   * @return column of the first char where the problem was found
   */
  public int getBeginColumn() {
    return this.beginColumn;
  }

  /**
   * @param beginLine
   *          the beginLine to set
   */
  protected void setBeginLine(int beginLine) {
    this.beginLine = beginLine;
  }

  /**
   * @param beginColumn
   *          the beginColumn to set
   */
  protected void setBeginColumn(int beginColumn) {
    this.beginColumn = beginColumn;
  }
}
