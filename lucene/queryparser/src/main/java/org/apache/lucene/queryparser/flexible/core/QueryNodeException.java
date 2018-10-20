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
package org.apache.lucene.queryparser.flexible.core;

import java.util.Locale;

import org.apache.lucene.queryparser.flexible.messages.Message;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.messages.NLS;
import org.apache.lucene.queryparser.flexible.messages.NLSException;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

/**
 * <p>
 * This exception should be thrown if something wrong happens when dealing with
 * {@link QueryNode}s.
 * </p>
 * <p>
 * It also supports NLS messages.
 * </p>
 * 
 * @see Message
 * @see NLS
 * @see NLSException
 * @see QueryNode
 */
public class QueryNodeException extends Exception implements NLSException {

  protected Message message = new MessageImpl(QueryParserMessages.EMPTY_MESSAGE);

  public QueryNodeException(Message message) {
    super(message.getKey());

    this.message = message;

  }

  public QueryNodeException(Throwable throwable) {
    super(throwable);
  }

  public QueryNodeException(Message message, Throwable throwable) {
    super(message.getKey(), throwable);

    this.message = message;

  }

  @Override
  public Message getMessageObject() {
    return this.message;
  }

  @Override
  public String getMessage() {
    return getLocalizedMessage();
  }

  @Override
  public String getLocalizedMessage() {
    return getLocalizedMessage(Locale.getDefault());
  }

  public String getLocalizedMessage(Locale locale) {
    return this.message.getLocalizedMessage(locale);
  }

  @Override
  public String toString() {
    return this.message.getKey() + ": " + getLocalizedMessage();
  }

}
