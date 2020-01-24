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
package org.apache.solr.util.hll;

import org.apache.solr.SolrTestCase;
import org.junit.Test;

/**
 * Tests {@link HLLUtil} static methods.
 */
public class HLLUtilTest extends SolrTestCase {
    /**
     * Tests that {@link HLLUtil#largeEstimatorCutoff(int, int)} is the same
     * as a trivial implementation.
     */
    @Test
    public void largeEstimatorCutoffTest() {
        for(int log2m=HLL.MINIMUM_LOG2M_PARAM; log2m<=HLL.MAXIMUM_LOG2M_PARAM; log2m++) {
            for(int regWidth=HLL.MINIMUM_REGWIDTH_PARAM; regWidth<=HLL.MINIMUM_REGWIDTH_PARAM; regWidth++) {
                final double cutoff = HLLUtil.largeEstimatorCutoff(log2m, regWidth);

                // See blog post (http://research.neustar.biz/2013/01/24/hyperloglog-googles-take-on-engineering-hll/)
                // and original paper (Fig. 3) for information on 2^L and
                // "large range correction" cutoff.
                final double expected = Math.pow(2, Math.pow(2, regWidth) - 2 + log2m) / 30.0;
                assertEquals(cutoff, expected, 0.0001);
            }
        }
    }
}
