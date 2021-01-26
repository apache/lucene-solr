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
package org.apache.solr.cluster.placement;

import java.util.HashMap;
import java.util.Map;

/**
 * Exception thrown when a placement modification is rejected by the placement plugin.
 * Additional details about the reasons are provided if available
 * in {@link #getRejectedModifications()} or in the {@link #toString()} methods.
 */
public class PlacementModificationException extends PlacementException {
  private final Map<String, String> rejectedModifications = new HashMap<>();

  public PlacementModificationException() {
    super();
  }

  public PlacementModificationException(String message) {
    super(message);
  }

  public PlacementModificationException(String message, Throwable cause) {
    super(message, cause);
  }

  public PlacementModificationException(Throwable cause) {
    super(cause);
  }

  /**
   * Add information about the modification that cause this exception.
   * @param modification requested modification details
   * @param reason reason for rejection
   */
  public void addRejectedModification(String modification, String reason) {
    rejectedModifications.put(modification, reason);
  }

  /**
   * Return rejected modifications and reasons for rejections.
   */
  public Map<String, String> getRejectedModifications() {
    return rejectedModifications;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    if (!rejectedModifications.isEmpty()) {
        sb.append(": ")
          .append(rejectedModifications.size())
          .append(" rejections:");
      rejectedModifications.forEach((modification, reason) ->
          sb.append("\n")
              .append(modification)
              .append("\t")
              .append(reason));

    }
    return sb.toString();
  }
}
