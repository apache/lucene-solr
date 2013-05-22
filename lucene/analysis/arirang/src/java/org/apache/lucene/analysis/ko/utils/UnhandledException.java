package org.apache.lucene.analysis.ko.utils;

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

public class UnhandledException extends RuntimeException {

  /**
   * Required for serialization support.
   * 
   * @see java.io.Serializable
   */
  private static final long serialVersionUID = 1832101364842773720L;

  /**
   * Constructs the exception using a cause.
   *
   * @param cause  the underlying cause
   */
  public UnhandledException(Throwable cause) {
    super(cause);
  }

  /**
   * Constructs the exception using a message and cause.
   *
   * @param message  the message to use
   * @param cause  the underlying cause
   */
  public UnhandledException(String message, Throwable cause) {
    super(message, cause);
  }
}
