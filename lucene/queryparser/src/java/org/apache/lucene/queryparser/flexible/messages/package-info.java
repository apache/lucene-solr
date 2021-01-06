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
 * For Native Language Support (NLS), system of software internationalization.
 *
 * <h2>NLS message API</h2>
 *
 * <p>This utility API, adds support for NLS messages in the apache code. It is currently used by
 * the lucene "New Flexible Query PArser".
 *
 * <p>Features:
 *
 * <ol>
 *   <li>Message reference in the code, using static Strings
 *   <li>Message resource validation at class load time, for easier debugging
 *   <li>Allows for message IDs to be re-factored using eclipse or other code re-factor tools
 *   <li>Allows for reference count on messages, just like code
 *   <li>Lazy loading of Message Strings
 *   <li>Normal loading Message Strings
 * </ol>
 *
 * <br>
 * <br>
 *
 * <p>Lazy loading of Message Strings
 *
 * <pre class="prettyprint">
 *   public class MessagesTestBundle extends NLS {
 *
 *     private static final String BUNDLE_NAME = MessagesTestBundle.class.getName();
 *
 *     private MessagesTestBundle() {
 *       // should never be instantiated
 *     }
 *
 *     static {
 *       // register all string ids with NLS class and initialize static string
 *       // values
 *       NLS.initializeMessages(BUNDLE_NAME, MessagesTestBundle.class);
 *     }
 *
 *     // static string must match the strings in the property files.
 *     public static String Q0001E_INVALID_SYNTAX;
 *     public static String Q0004E_INVALID_SYNTAX_ESCAPE_UNICODE_TRUNCATION;
 *
 *     // this message is missing from the properties file
 *     public static String Q0005E_MESSAGE_NOT_IN_BUNDLE;
 *   }
 *
 *     // Create a message reference
 *     Message invalidSyntax = new MessageImpl(MessagesTestBundle.Q0001E_INVALID_SYNTAX, "XXX");
 *
 *     // Do other stuff in the code...
 *     // when is time to display the message to the user or log the message on a file
 *     // the message is loaded from the correct bundle
 *
 *     String message1 = invalidSyntax.getLocalizedMessage();
 *     String message2 = invalidSyntax.getLocalizedMessage(Locale.JAPANESE);
 * </pre>
 *
 * <br>
 * <br>
 *
 * <p>Normal loading of Message Strings
 *
 * <pre class="prettyprint">
 *   String message1 = NLS.getLocalizedMessage(MessagesTestBundle.Q0004E_INVALID_SYNTAX_ESCAPE_UNICODE_TRUNCATION);
 *   String message2 = NLS.getLocalizedMessage(MessagesTestBundle.Q0004E_INVALID_SYNTAX_ESCAPE_UNICODE_TRUNCATION, Locale.JAPANESE);
 * </pre>
 *
 * <p>The org.apache.lucene.messages.TestNLS junit contains several other examples. The TestNLS java
 * code is available from the Apache Lucene code repository.
 */
package org.apache.lucene.queryparser.flexible.messages;
