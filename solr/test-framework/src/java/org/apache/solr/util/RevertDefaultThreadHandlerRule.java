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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.carrotsearch.randomizedtesting.rules.StatementAdapter;

public final class RevertDefaultThreadHandlerRule implements TestRule {
  private final static AtomicBoolean applied = new AtomicBoolean();
  
  @Override
  public Statement apply(Statement s, Description d) {
    return new StatementAdapter(s) {
      @Override
      protected void before() throws Throwable {
        if (!applied.getAndSet(true)) {
          UncaughtExceptionHandler p = Thread.getDefaultUncaughtExceptionHandler();
          try {
            // Try to initialize a zookeeper class that reinitializes default exception handler.
            Class<?> cl = NIOServerCnxnFactory.class;
            // Make sure static initializers have been called.
            Class.forName(cl.getName(), true, cl.getClassLoader());
          } finally {
            if (p == Thread.getDefaultUncaughtExceptionHandler()) {
            //  throw new RuntimeException("Zookeeper no longer resets default thread handler.");
            }
            Thread.setDefaultUncaughtExceptionHandler(p);
          }
        }
      }
    };
  }
}
