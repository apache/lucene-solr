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
package org.apache.solr.client.solrj.response;

import java.util.Locale;

/**
 * Represents the state of an asynchronous request.
 * 
 * @see org.apache.solr.client.solrj.request.CollectionAdminRequest.RequestStatus
 */
public enum RequestStatusState {

  /** The request was completed. */
  COMPLETED("completed"),

  /** The request has failed. */
  FAILED("failed"),

  /** The request is in progress. */
  RUNNING("running"),

  /** The request was submitted, but has not yet started. */
  SUBMITTED("submitted"),

  /** The request Id was not found. */
  NOT_FOUND("notfound");

  private final String key;

  private RequestStatusState(String key) {
    this.key = key;
  }

  /**
   * Returns the string representation of this state, for using as a key. For backward compatibility, it returns the
   * lowercase form of the state's name.
   */
  public String getKey() {
    return key;
  }

  /**
   * Resolves a key that was returned from {@link #getKey()} to a {@link RequestStatusState}. For backwards
   * compatibility, it resolves the key "notfound" to {@link #NOT_FOUND}.
   */
  public static RequestStatusState fromKey(String key) {
    try {
      return RequestStatusState.valueOf(key.toUpperCase(Locale.ENGLISH));
    } catch (final IllegalArgumentException e) {
      if (key.equalsIgnoreCase(RequestStatusState.NOT_FOUND.getKey())) {
        return RequestStatusState.NOT_FOUND;
      } else {
        throw e;
      }
    }
  }

}