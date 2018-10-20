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
package org.apache.lucene.search.suggest;


import java.util.List;
import java.util.Locale;

/**
 * Average with standard deviation.
 */
final class Average
{
    /**
     * Average (in milliseconds).
     */
    public final double avg;

    /**
     * Standard deviation (in milliseconds).
     */
    public final double stddev;

    /**
     * 
     */
    Average(double avg, double stddev)
    {
        this.avg = avg;
        this.stddev = stddev;
    }

    @Override
    public String toString()
    {
        return String.format(Locale.ROOT, "%.0f [+- %.2f]", 
            avg, stddev);
    }

    static Average from(List<Double> values)
    {
        double sum = 0;
        double sumSquares = 0;

        for (double l : values)
        {
            sum += l;
            sumSquares += l * l;
        }

        double avg = sum / (double) values.size();
        return new Average(
            (sum / (double) values.size()), 
            Math.sqrt(sumSquares / (double) values.size() - avg * avg));
    }
}