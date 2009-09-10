package org.apache.lucene.messages;

/**
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

import java.util.Locale;

/**
 * Default implementation of Message interface.
 * For Native Language Support (NLS), system of software internationalization.
 */
public class MessageImpl implements Message {

  private static final long serialVersionUID = -3077643314630884523L;

  private String key;

  private Object[] arguments = new Object[0];

  public MessageImpl(String key) {
    this.key = key;

  }

  public MessageImpl(String key, Object[] args) {
    this(key);
    this.arguments = args;
  }

  public Object[] getArguments() {
    return this.arguments;
  }

  public String getKey() {
    return this.key;
  }

  public String getLocalizedMessage() {
    return getLocalizedMessage(Locale.getDefault());
  }

  public String getLocalizedMessage(Locale locale) {
    return NLS.getLocalizedMessage(getKey(), locale, getArguments());
  }

  public String toString() {
    Object[] args = getArguments();
    String argsString = "";
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        argsString += args[i] + (i < args.length ? "" : ", ");
      }
    }
    return getKey() + " " + argsString;
  }

}
