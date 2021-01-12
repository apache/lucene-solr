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

/**
 * Lucene Benchmarking Package
 *
 * <p>The benchmark contribution contains tools for benchmarking Lucene using standard, freely
 * available corpora.
 *
 * <p>Gradle will download the corpus automatically, place it in a temp directory and then unpack it
 * to the working.dir directory specified in the build. The temp directory and working directory can
 * be safely removed after a run. However, the next time the task is run, it will need to download
 * the files again.
 *
 * <p>Classes implementing the Benchmarker interface should have a no-argument constructor if they
 * are to be used with the Driver class. The Driver class is provided for convenience only. Feel
 * free to implement your own main class for your benchmarker.
 *
 * <p>The StandardBenchmarker is meant to be just that, a standard that runs out of the box with no
 * configuration or changes needed. Other benchmarking classes may derive from it to provide
 * alternate views or to take in command line options. When reporting benchmarking runs you should
 * state any alterations you have made.
 *
 * <p>To run the short version of the StandardBenchmarker, call "ant run-micro-standard". This
 * should take a minute or so to complete and give you a preliminary idea of how your change affects
 * the code.
 *
 * <p>To run the long version of the StandardBenchmarker, call "ant run-standard". This takes
 * considerably longer.
 *
 * <p>The original code for these classes was donated by Andrzej Bialecki at
 * http://issues.apache.org/jira/browse/LUCENE-675 and has been updated by Grant Ingersoll to make
 * some parts of the code reusable in other benchmarkers
 */
package org.apache.lucene.benchmark;
