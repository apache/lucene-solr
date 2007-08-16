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


/**
 * Marker Interface defining some common options.  Implementations should define their own set of options that can be
 * cast to in the {@link Benchmarker} interface.
 * <p/>
 * As benchmarks are added, perhaps a common set of Options will become clear
 *
 *
 * @deprecated Use the task based approach instead
 **/
public interface BenchmarkOptions
{
}
