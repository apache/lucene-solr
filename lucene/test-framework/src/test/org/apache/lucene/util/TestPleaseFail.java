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

import org.junit.Assert;

/**
 * This test is for manually causing assertion failures or errors (to trigger the event for
 * occasional needs of testing the test framework itself from command line).
 */
public class TestPleaseFail extends LuceneTestCase {
  public void testFail() {
    Assert.assertNull("(intentional failure)", System.getProperty("please.fail"));
  }

  public void testError() {
    if (System.getProperty("please.fail") != null) {
      throw new RuntimeException("(intentional error)");
    }
  }
}
