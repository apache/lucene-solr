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
package org.apache.lucene.misc.store;

import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;
import java.util.Set;
import org.apache.lucene.util.Constants;
import org.junit.Assume;

public class NativeLibEnableRule extends TestRuleAdapter {
  enum OperatingSystem {
    LINUX(Constants.LINUX),
    WINDOWS(Constants.WINDOWS),
    SUN_OS(Constants.SUN_OS),
    MAC(Constants.MAC_OS_X),
    FREE_BSD(Constants.FREE_BSD);

    public final boolean enabled;

    OperatingSystem(boolean enabled) {
      this.enabled = enabled;
    }
  }

  private final Set<OperatingSystem> runOn;

  public NativeLibEnableRule(Set<OperatingSystem> runOn) {
    this.runOn = runOn;
  }

  @Override
  protected void before() {
    Assume.assumeTrue(
        "Test ignored (tests.native is false)",
        Boolean.parseBoolean(System.getProperty("tests.native", "false")));

    Assume.assumeTrue(
        "Test ignored, only applies to architectures: " + runOn,
        runOn.stream().anyMatch(os -> os.enabled));
  }
}
