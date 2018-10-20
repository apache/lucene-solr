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
package org.apache.lucene.util;

/** time unit constants for use in annotations. */
public final class TimeUnits {
  private TimeUnits() {}

  /** 1 second in milliseconds */
  public static final int SECOND = 1000;
  /** 1 minute in milliseconds */
  public static final int MINUTE = 60 * SECOND;
  /** 1 hour in milliseconds */
  public static final int HOUR = 60 * MINUTE;  
}
