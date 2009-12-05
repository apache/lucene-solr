package org.apache.lucene.util;

/**
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


import org.junit.rules.TestWatchman;
import org.junit.runners.model.FrameworkMethod;

import java.lang.reflect.Method;


public final class InterceptTestCaseEvents extends TestWatchman {
  private Object obj;

  public InterceptTestCaseEvents(Object obj) {
    this.obj = obj;
  }

  @Override
  public void failed(Throwable e, FrameworkMethod method) {
    try {
      Method reporter = method.getMethod().getDeclaringClass().getMethod("reportAdditionalFailureInfo",(Class<?>[]) null);
      reporter.invoke(obj, (Object[])null);
    } catch (Exception e1) {
      System.err.println("InterceptTestCaseEvents.failed(). Cannot invoke reportAdditionalFailureInfo() method in" +
              " consuming class, is it declared and public?");
    }
    super.failed(e, method);
  }

  @Override
  public void finished(FrameworkMethod method) {
    super.finished(method);
  }

  @Override
  public void starting(FrameworkMethod method) {
    super.starting(method);
  }

  @Override
  public void succeeded(FrameworkMethod method) {
    super.succeeded(method);
  }
}
