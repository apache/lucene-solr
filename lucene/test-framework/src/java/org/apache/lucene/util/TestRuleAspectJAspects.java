package org.apache.lucene.util;

import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;

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

public class TestRuleAspectJAspects extends TestRuleAdapter {
  private TestRuleMarkFailure failureMarker;

  public TestRuleAspectJAspects(TestRuleMarkFailure failureMarker) {
    this.failureMarker = failureMarker;
  }
  
  @Override
  protected void before() throws Throwable {
    pointcutBeforeSuite();
  }
  
  @Override
  protected void afterIfSuccessful() throws Throwable {
    if (failureMarker.wasSuccessful()) {
      pointcutAfterSuite();
    }
  }

  // AspectJ-injected aspects go here.

  private void pointcutBeforeSuite() {}
  private void pointcutAfterSuite() {}
}