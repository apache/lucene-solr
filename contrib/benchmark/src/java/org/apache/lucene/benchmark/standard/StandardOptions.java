package org.apache.lucene.benchmark.standard;

import org.apache.lucene.benchmark.BenchmarkOptions;
import org.apache.lucene.benchmark.Constants;
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
 *
 * @deprecated Use the Task based stuff instead
 **/
public class StandardOptions implements BenchmarkOptions
{
    private int runCount = Constants.DEFAULT_RUN_COUNT;
    private int logStep = Constants.DEFAULT_LOG_STEP;
    private int scaleUp = Constants.DEFAULT_SCALE_UP;
    private int maximumDocumentsToIndex = Constants.DEFAULT_MAXIMUM_DOCUMENTS;


    public int getMaximumDocumentsToIndex()
    {
        return maximumDocumentsToIndex;
    }

    public void setMaximumDocumentsToIndex(int maximumDocumentsToIndex)
    {
        this.maximumDocumentsToIndex = maximumDocumentsToIndex;
    }

    /**
     * How often to print out log messages when in benchmark loops
     */
    public int getLogStep()
    {
        return logStep;
    }

    public void setLogStep(int logStep)
    {
        this.logStep = logStep;
    }

    /**
     * The number of times to run the benchmark
     */
    public int getRunCount()
    {
        return runCount;
    }

    public void setRunCount(int runCount)
    {
        this.runCount = runCount;
    }

    public int getScaleUp()
    {
        return scaleUp;
    }

    public void setScaleUp(int scaleUp)
    {
        this.scaleUp = scaleUp;
    }
}
