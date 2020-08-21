/*
 * Copyright 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.gradle;

import org.gradle.api.file.FileTree;
import org.gradle.api.internal.DocumentationRegistry;
import org.gradle.api.internal.classpath.ModuleRegistry;
import org.gradle.api.internal.tasks.testing.JvmTestExecutionSpec;
import org.gradle.api.internal.tasks.testing.TestClassProcessor;
import org.gradle.api.internal.tasks.testing.TestExecuter;
import org.gradle.api.internal.tasks.testing.TestFramework;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;
import org.gradle.api.internal.tasks.testing.WorkerTestClassProcessorFactory;
import org.gradle.api.internal.tasks.testing.detection.DefaultTestClassScanner;
import org.gradle.api.internal.tasks.testing.detection.TestFrameworkDetector;
import org.gradle.api.internal.tasks.testing.filter.DefaultTestFilter;
import org.gradle.api.internal.tasks.testing.processors.MaxNParallelTestClassProcessor;
import org.gradle.api.internal.tasks.testing.processors.PatternMatchTestClassProcessor;
import org.gradle.api.internal.tasks.testing.processors.RestartEveryNTestClassProcessor;
import org.gradle.api.internal.tasks.testing.processors.RunPreviousFailedFirstTestClassProcessor;
import org.gradle.api.internal.tasks.testing.processors.TestMainAction;
import org.gradle.api.internal.tasks.testing.worker.ForkingTestClassProcessor;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.internal.Factory;
import org.gradle.internal.actor.ActorFactory;
import org.gradle.internal.operations.BuildOperationExecutor;
import org.gradle.internal.time.Clock;
import org.gradle.internal.work.WorkerLeaseRegistry;
import org.gradle.process.internal.worker.WorkerProcessFactory;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The default test class scanner factory.
 */
public class CustomTestExecuter implements TestExecuter<JvmTestExecutionSpec> {
    private static final Logger LOGGER = Logging.getLogger(CustomTestExecuter.class);

    private final WorkerProcessFactory workerFactory;
    private final ActorFactory actorFactory;
    private final ModuleRegistry moduleRegistry;
    private final WorkerLeaseRegistry workerLeaseRegistry;
    private final BuildOperationExecutor buildOperationExecutor;
    private final int maxWorkerCount;
    private final Clock clock;
    private final DocumentationRegistry documentationRegistry;
    private final DefaultTestFilter testFilter;
    private final int dups;
    private final CustomProgressLogger logger;
    private TestClassProcessor processor;

    public CustomTestExecuter(CustomProgressLogger logger, WorkerProcessFactory workerFactory, ActorFactory actorFactory, ModuleRegistry moduleRegistry,
                              WorkerLeaseRegistry workerLeaseRegistry, BuildOperationExecutor buildOperationExecutor, int maxWorkerCount,
                              Clock clock, DocumentationRegistry documentationRegistry, DefaultTestFilter testFilter,
                              int dups) {
        this.workerFactory = workerFactory;
        this.actorFactory = actorFactory;
        this.moduleRegistry = moduleRegistry;
        this.workerLeaseRegistry = workerLeaseRegistry;
        this.buildOperationExecutor = buildOperationExecutor;
        this.maxWorkerCount = maxWorkerCount;
        this.clock = clock;
        this.documentationRegistry = documentationRegistry;
        this.testFilter = testFilter;
        this.dups = dups;
        this.logger = logger;
    }

    public Set<File> copyOf(Iterable<? extends File> it) {
        LinkedHashSet<File> ff = new LinkedHashSet<>();
        it.forEach(ff::add);
        return ff;
    }

    @Override
    public void execute(final JvmTestExecutionSpec testExecutionSpec, TestResultProcessor testResultProcessor) {
        final TestFramework testFramework = testExecutionSpec.getTestFramework();
        final WorkerTestClassProcessorFactory testInstanceFactory = testFramework.getProcessorFactory();
        final WorkerLeaseRegistry.WorkerLease currentWorkerLease = workerLeaseRegistry.getCurrentWorkerLease();
        final Set<File> classpath = copyOf(testExecutionSpec.getClasspath());
        final Set<File> modulePath = copyOf(testExecutionSpec.getModulePath());
        final List<String> testWorkerImplementationModules = testFramework.getTestWorkerImplementationModules();
        final Factory<TestClassProcessor> forkingProcessorFactory = new Factory<TestClassProcessor>() {
            @Override
            public TestClassProcessor create() {
                return new ForkingTestClassProcessor(currentWorkerLease, workerFactory, testInstanceFactory, testExecutionSpec.getJavaForkOptions(),
                    classpath, modulePath, testWorkerImplementationModules, testFramework.getWorkerConfigurationAction(), moduleRegistry, documentationRegistry);
            }
        };
        final Factory<TestClassProcessor> reforkingProcessorFactory = new Factory<TestClassProcessor>() {
            @Override
            public TestClassProcessor create() {
                return new RestartEveryNTestClassProcessor(forkingProcessorFactory, testExecutionSpec.getForkEvery());
            }
        };

        processor = new MaxNParallelTestClassProcessor(getMaxParallelForks(testExecutionSpec), reforkingProcessorFactory, actorFactory);

        processor =
            new PatternMatchTestClassProcessor(testFilter,
                 new RunPreviousFailedFirstTestClassProcessor(testExecutionSpec.getPreviousFailedTestClasses(),
                     new DuplicateSuiteTestClassProcessor(logger, dups, processor)));

        final FileTree testClassFiles = testExecutionSpec.getCandidateClassFiles();

        Runnable detector;
        if (testExecutionSpec.isScanForTestClasses() && testFramework.getDetector() != null) {
            TestFrameworkDetector testFrameworkDetector = testFramework.getDetector();
            testFrameworkDetector.setTestClasses(testExecutionSpec.getTestClassesDirs().getFiles());
            testFrameworkDetector.setTestClasspath(classpath);
            detector = new DefaultTestClassScanner(testClassFiles, testFrameworkDetector, processor);
        } else {
            detector = new DefaultTestClassScanner(testClassFiles, null, processor);
        }

        final Object testTaskOperationId = buildOperationExecutor.getCurrentOperation().getParentId();

        try {
            new TestMainAction(detector, processor, testResultProcessor, clock, testTaskOperationId, testExecutionSpec.getPath(), "Gradle Test Run " + testExecutionSpec.getIdentityPath()).run();
        } finally {
            logger.completeAll();
        }
    }

    @Override
    public void stopNow() {
        if (processor != null) {
            processor.stopNow();
        }
    }

    private int getMaxParallelForks(JvmTestExecutionSpec testExecutionSpec) {
        int maxParallelForks = testExecutionSpec.getMaxParallelForks();
        if (maxParallelForks > maxWorkerCount) {
            LOGGER.info("{}.maxParallelForks ({}) is larger than max-workers ({}), forcing it to {}", testExecutionSpec.getPath(), maxParallelForks, maxWorkerCount, maxWorkerCount);
            maxParallelForks = maxWorkerCount;
        }
        return maxParallelForks;
    }

}
