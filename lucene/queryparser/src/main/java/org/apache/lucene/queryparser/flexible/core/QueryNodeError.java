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

import org.apache.lucene.queryparser.flexible.messages.Message;
import org.apache.lucene.queryparser.flexible.messages.NLSException;

/**
 * Error class with NLS support
 * 
 * @see org.apache.lucene.queryparser.flexible.messages.NLS
 * @see org.apache.lucene.queryparser.flexible.messages.Message
 */
public class QueryNodeError extends Error implements NLSException {
  private Message message;

  /**
   * @param message
   *          - NLS Message Object
   */
  public QueryNodeError(Message message) {
    super(message.getKey());

    this.message = message;

  }

  /**
   * @param throwable
   *          - @see java.lang.Error
   */
  public QueryNodeError(Throwable throwable) {
    super(throwable);
  }

  /**
   * @param message
   *          - NLS Message Object
   * @param throwable
   *          - @see java.lang.Error
   */
  public QueryNodeError(Message message, Throwable throwable) {
    super(message.getKey(), throwable);

    this.message = message;

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.messages.NLSException#getMessageObject()
   */
  @Override
  public Message getMessageObject() {
    return this.message;
  }

}
