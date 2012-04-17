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
 * Various benchmarking constants (mostly defaults)
 **/
public class Constants
{
    public static final int DEFAULT_RUN_COUNT = 5;
    public static final int DEFAULT_SCALE_UP = 5;
    public static final int DEFAULT_LOG_STEP = 1000;

    public static Boolean[] BOOLEANS = new Boolean[] { Boolean.FALSE, Boolean.TRUE };

    public static final int DEFAULT_MAXIMUM_DOCUMENTS = Integer.MAX_VALUE;
}
