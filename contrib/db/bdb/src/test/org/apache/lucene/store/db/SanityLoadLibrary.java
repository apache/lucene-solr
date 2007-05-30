package org.apache.lucene.store.db;

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

import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.Environment;

/**
 * Simple sanity testing application to verify that the underlying 
 * native library can be loaded cleanly.
 *
 * For use in the build.xml of this contrib, to determine if tests 
 * should be skipped.
 */
public class SanityLoadLibrary {
  public static void main(String[] ignored) throws Exception {
    EnvironmentConfig envConfig = EnvironmentConfig.DEFAULT;
    envConfig.setAllowCreate(false);
    Environment env = new Environment(null, envConfig);
  }
}
