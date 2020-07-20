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

package org.apache.solr.cloud.gumi;

import java.util.List;

/**
 * Exception thrown by a {@link GumiPlugin} when it is unable to compute placement for whatever reason (except an
 * {@link InterruptedException} that {@link GumiPlugin#computePlacement}
 * is also allowed to throw).
 */
public class GumiException extends Exception {

  public GumiException() {
    super();
  }

  public GumiException(String message) {
    super(message);
  }

  public GumiException(String message, Throwable cause) {
    super(message, cause);
  }

  public GumiException(Throwable cause) {
    super(cause);
  }

  protected GumiException(String message, Throwable cause,
                      boolean enableSuppression,
                      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
