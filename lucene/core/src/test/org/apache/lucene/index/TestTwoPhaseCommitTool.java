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
package org.apache.lucene.index;


import java.io.IOException;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;

public class TestTwoPhaseCommitTool extends LuceneTestCase {

  private static class TwoPhaseCommitImpl implements TwoPhaseCommit {
    static boolean commitCalled = false;
    final boolean failOnPrepare;
    final boolean failOnCommit;
    final boolean failOnRollback;
    boolean rollbackCalled = false;
    Map<String, String> prepareCommitData = null;
    Map<String, String> commitData = null;

    public TwoPhaseCommitImpl(boolean failOnPrepare, boolean failOnCommit, boolean failOnRollback) {
      this.failOnPrepare = failOnPrepare;
      this.failOnCommit = failOnCommit;
      this.failOnRollback = failOnRollback;
    }

    @Override
    public long prepareCommit() throws IOException {
      return prepareCommit(null);
    }

    public long prepareCommit(Map<String, String> commitData) throws IOException {
      this.prepareCommitData = commitData;
      assertFalse("commit should not have been called before all prepareCommit were", commitCalled);
      if (failOnPrepare) {
        throw new IOException("failOnPrepare");
      }
      return 1;
    }

    @Override
    public long commit() throws IOException {
      return commit(null);
    }

    public long commit(Map<String, String> commitData) throws IOException {
      this.commitData = commitData;
      commitCalled = true;
      if (failOnCommit) {
        throw new RuntimeException("failOnCommit");
      }
      return 1;
    }

    @Override
    public void rollback() throws IOException {
      rollbackCalled = true;
      if (failOnRollback) {
        throw new Error("failOnRollback");
      }
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    TwoPhaseCommitImpl.commitCalled = false; // reset count before every test
  }

  public void testPrepareThenCommit() throws Exception {
    // tests that prepareCommit() is called on all objects before commit()
    TwoPhaseCommitImpl[] objects = new TwoPhaseCommitImpl[2];
    for (int i = 0; i < objects.length; i++) {
      objects[i] = new TwoPhaseCommitImpl(false, false, false);
    }

    // following call will fail if commit() is called before all prepare() were
    TwoPhaseCommitTool.execute(objects);
  }

  public void testRollback() throws Exception {
    // tests that rollback is called if failure occurs at any stage
    int numObjects = random().nextInt(8) + 3; // between [3, 10]
    TwoPhaseCommitImpl[] objects = new TwoPhaseCommitImpl[numObjects];
    for (int i = 0; i < objects.length; i++) {
      boolean failOnPrepare = random().nextBoolean();
      // we should not hit failures on commit usually
      boolean failOnCommit = random().nextDouble() < 0.05;
      boolean railOnRollback = random().nextBoolean();
      objects[i] = new TwoPhaseCommitImpl(failOnPrepare, failOnCommit, railOnRollback);
    }

    boolean anyFailure = false;
    try {
      TwoPhaseCommitTool.execute(objects);
    } catch (Throwable t) {
      anyFailure = true;
    }

    if (anyFailure) {
      // if any failure happened, ensure that rollback was called on all.
      for (TwoPhaseCommitImpl tpc : objects) {
        assertTrue("rollback was not called while a failure occurred during the 2-phase commit", tpc.rollbackCalled);
      }
    }
  }

  public void testNullTPCs() throws Exception {
    int numObjects = random().nextInt(4) + 3; // between [3, 6]
    TwoPhaseCommit[] tpcs = new TwoPhaseCommit[numObjects];
    boolean setNull = false;
    for (int i = 0; i < tpcs.length; i++) {
      boolean isNull = random().nextDouble() < 0.3;
      if (isNull) {
        setNull = true;
        tpcs[i] = null;
      } else {
        tpcs[i] = new TwoPhaseCommitImpl(false, false, false);
      }
    }

    if (!setNull) {
      // none of the TPCs were picked to be null, pick one at random
      int idx = random().nextInt(numObjects);
      tpcs[idx] = null;
    }

    // following call would fail if TPCTool won't handle null TPCs properly
    TwoPhaseCommitTool.execute(tpcs);
  }

}
