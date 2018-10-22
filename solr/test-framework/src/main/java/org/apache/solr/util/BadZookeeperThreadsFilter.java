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
package org.apache.solr.util;

import com.carrotsearch.randomizedtesting.ThreadFilter;

public class BadZookeeperThreadsFilter implements ThreadFilter {

  @Override
  public boolean reject(Thread t) {
    String name = t.getName();
    
    StackTraceElement [] stack = t.getStackTrace();
    if (name.startsWith("Thread-") && stack.length > 1 && stack[stack.length - 2].getClassName().equals("org.apache.zookeeper.Login$1")) {
      return true; // see ZOOKEEPER-2100
    }

    return false;
  }
}
