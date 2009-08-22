package org.apache.lucene.analysis;

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

import java.util.Set;
 
import org.apache.lucene.util.LuceneTestCase;

/** 
 * Base class for all Lucene unit tests that use TokenStreams.  
 * <p>
 * This class runs all tests twice, one time with {@link TokenStream#setOnlyUseNewAPI} <code>false</code>
 * and after that one time with <code>true</code>.
 */
public abstract class BaseTokenStreamTestCase extends LuceneTestCase {

  private boolean onlyUseNewAPI = false;
  private final Set testWithNewAPI;
  
  public BaseTokenStreamTestCase() {
    super();
    this.testWithNewAPI = null; // run all tests also with onlyUseNewAPI
  }

  public BaseTokenStreamTestCase(String name) {
    super(name);
    this.testWithNewAPI = null; // run all tests also with onlyUseNewAPI
  }

  public BaseTokenStreamTestCase(Set testWithNewAPI) {
    super();
    this.testWithNewAPI = testWithNewAPI;
  }

  public BaseTokenStreamTestCase(String name, Set testWithNewAPI) {
    super(name);
    this.testWithNewAPI = testWithNewAPI;
  }

  // @Override
  protected void setUp() throws Exception {
    super.setUp();
    TokenStream.setOnlyUseNewAPI(onlyUseNewAPI);
  }

  // @Override
  public void runBare() throws Throwable {
    // Do the test with onlyUseNewAPI=false (default)
    try {
      onlyUseNewAPI = false;
      super.runBare();
    } catch (Throwable e) {
      System.out.println("Test failure of "+getName()+" occurred with onlyUseNewAPI=false");
      throw e;
    }

    if (testWithNewAPI == null || testWithNewAPI.contains(getName())) {
      // Do the test again with onlyUseNewAPI=true
      try {
        onlyUseNewAPI = true;
        super.runBare();
      } catch (Throwable e) {
        System.out.println("Test failure of "+getName()+" occurred with onlyUseNewAPI=true");
        throw e;
      }
    }
  }

}
