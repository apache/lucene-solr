package org.apache.lucene.index;

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
 * Base for query timeout implementations, which will provide a {@code shouldExit()} method,
 * used with {@link ExitableDirectoryReader}.
 */
public interface QueryTimeout {
  
  /**
   * Called from {@link ExitableDirectoryReader.ExitableTermsEnum#next()} 
   * to determine whether to stop processing a query.
   */
  public abstract boolean shouldExit();

  /** Returns true if timeouts are enabled for this query (i.e. if shouldExit would ever return true) */
  public default boolean isTimeoutEnabled() {
    return true;
  }

}

