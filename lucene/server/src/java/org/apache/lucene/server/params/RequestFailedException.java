package org.apache.lucene.server.params;

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

// nocommit rename to InvalidRequestExc?

/** Exception thrown on an invalid request. */
public class RequestFailedException extends IllegalArgumentException {

  /** The request that contains the failure. */
  public final Request request;

  /** Which parameter led to the failure, if any (can be
   * null). */
  public final String param;

  /** The full path leading to the parameter (e.g.,
   *  sort.fields[0].field). */
  public final String path;

  /** The specific reason for the failure (e.g., "expected
   *  int but got string"). */
  public final String reason;

  /** Creates this. */
  public RequestFailedException(Request r, String param, String path, String reason) {
    super(path + ": " + reason);
    this.request = r;
    this.param = param;
    this.path = path;
    this.reason = reason;
  }

  /** Creates this from another {@code
   *  RequestFailedException}, adding further details. */
  public RequestFailedException(RequestFailedException other, String details) {
    super(other.reason + ": " + details);
    this.reason = other.reason + ": " + details;
    this.request = other.request;
    this.param = other.param;
    this.path = other.path;
  }
}
