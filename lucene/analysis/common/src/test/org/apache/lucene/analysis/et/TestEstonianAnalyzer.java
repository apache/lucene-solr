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
package org.apache.lucene.analysis.et;


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;

public class TestEstonianAnalyzer extends BaseTokenStreamTestCase {

    /** This test fails with NPE when the
     * stopwords file is missing in classpath */
    public void testResourcesAvailable() {
        new EstonianAnalyzer().close();
    }

    /** test stopwords and stemming */
    public void testBasics() throws IOException {
        Analyzer a = new EstonianAnalyzer();
        // stemming
        checkOneTerm(a, "teadaolevalt", "teadaole");
        checkOneTerm(a, "teadaolevaid", "teadaole");
        checkOneTerm(a, "teadaolevatest", "teadaole");
        checkOneTerm(a, "teadaolevail", "teadaole");
        checkOneTerm(a, "teadaolevatele", "teadaole");
        checkOneTerm(a, "teadaolevatel", "teadaole");
        checkOneTerm(a, "teadaolevateks", "teadaole");
        checkOneTerm(a, "teadaolevate", "teadaole");
        checkOneTerm(a, "teadaolevaks", "teadaole");
        checkOneTerm(a, "teadaoleval", "teadaole");
        checkOneTerm(a, "teadaolevates", "teadaole");
        checkOneTerm(a, "teadaolevat", "teadaole");
        checkOneTerm(a, "teadaolevast", "teadaole");
        checkOneTerm(a, "teadaoleva", "teadaole");
        checkOneTerm(a, "teadaolevais", "teadaole");
        checkOneTerm(a, "teadaolevas", "teadaole");
        checkOneTerm(a, "teadaolevad", "teadaole");
        checkOneTerm(a, "teadaolevale", "teadaole");
        checkOneTerm(a, "teadaolevatesse", "teadaole");
        // stopword
        assertAnalyzesTo(a, "alla", new String[] { });
        a.close();
    }


}
