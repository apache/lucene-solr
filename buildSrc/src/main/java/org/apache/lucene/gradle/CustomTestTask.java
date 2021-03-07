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
package org.apache.lucene.gradle;

import org.gradle.StartParameter;
import org.gradle.api.internal.DocumentationRegistry;
import org.gradle.api.internal.tasks.testing.JvmTestExecutionSpec;
import org.gradle.api.internal.tasks.testing.TestExecuter;
import org.gradle.api.internal.tasks.testing.filter.DefaultTestFilter;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.testing.Test;
import org.gradle.internal.operations.BuildOperationExecutor;
import org.gradle.internal.time.Clock;
import org.gradle.internal.work.WorkerLeaseRegistry;

public class CustomTestTask extends Test {
  @Input
  public int dups = 1;

  @Override
  protected TestExecuter<JvmTestExecutionSpec> createTestExecuter() {
    CustomProgressLogger logger = new CustomProgressLogger(getProgressLoggerFactory());

    return new CustomTestExecuter(logger, getProcessBuilderFactory(), getActorFactory(), getModuleRegistry(),
        getServices().get(WorkerLeaseRegistry.class),
        getServices().get(BuildOperationExecutor.class),
        getServices().get(StartParameter.class).getMaxWorkerCount(),
        getServices().get(Clock.class),
        getServices().get(DocumentationRegistry.class),
        (DefaultTestFilter) getFilter(),
        dups);
  }
}
