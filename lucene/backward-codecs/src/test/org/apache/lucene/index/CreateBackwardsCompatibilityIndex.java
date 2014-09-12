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

import java.io.IOException;

// This class exists only so it has a name that the junit runner will not pickup,
// so these index creation "tests" can only be run explicitly
public class CreateBackwardsCompatibilityIndex extends TestBackwardsCompatibility {

  // These indexes will be created under directory /tmp/idx/.
  //
  // Be sure to create the indexes with the actual format:
  //  ant test -Dtestcase=TestBackwardsCompatibility -Dversion=x.y.z -Dtests.codec=LuceneXY -Dtests.useSecurityManager=false
  //
  // Zip up the generated indexes:
  //
  //    cd /tmp/idx/index.cfs   ; zip index.<VERSION>.cfs.zip *
  //    cd /tmp/idx/index.nocfs ; zip index.<VERSION>.nocfs.zip *
  //
  // Then move those 2 zip files to your trunk checkout and add them
  // to the oldNames array.

  public void testCreateCFS() throws IOException {
    createIndex("index.cfs", true, false);
  }

  public void testCreateNonCFS() throws IOException {
    createIndex("index.nocfs", false, false);
  }

  // These are only needed for the special upgrade test to verify
  // that also single-segment indexes are correctly upgraded by IndexUpgrader.
  // You don't need them to be build for non-4.0 (the test is happy with just one
  // "old" segment format, version is unimportant:

  public void testCreateSingleSegmentCFS() throws IOException {
    createIndex("index.singlesegment.cfs", true, true);
  }

  public void testCreateSingleSegmentNoCFS() throws IOException {
    createIndex("index.singlesegment.nocfs", false, true);
  }
}
