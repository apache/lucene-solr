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
    return new CustomTestExecuter(getProcessBuilderFactory(), getActorFactory(), getModuleRegistry(),
        getServices().get(WorkerLeaseRegistry.class),
        getServices().get(BuildOperationExecutor.class),
        getServices().get(StartParameter.class).getMaxWorkerCount(),
        getServices().get(Clock.class),
        getServices().get(DocumentationRegistry.class),
        (DefaultTestFilter) getFilter(),
        dups);
  }
}
