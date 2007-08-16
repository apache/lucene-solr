package org.apache.lucene.benchmark;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.lucene.benchmark.stats.TestData;

import java.io.File;


/**
 *
 * @deprecated Use the Task based benchmarker
 **/
public interface Benchmarker
{
    /**
     * Benchmark according to the implementation, using the workingDir as the place to store things.
     *
     * @param workingDir The {@link java.io.File} directory to store temporary data in for running the benchmark
     * @param options Any {@link BenchmarkOptions} that are needed for this benchmark.  This
     * @return The {@link org.apache.lucene.benchmark.stats.TestData} used to run the benchmark.
     */
    TestData[] benchmark(File workingDir, BenchmarkOptions options) throws Exception;
}
