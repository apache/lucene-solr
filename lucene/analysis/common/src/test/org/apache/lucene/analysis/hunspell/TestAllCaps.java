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
package org.apache.lucene.analysis.hunspell;

import org.junit.BeforeClass;

public class TestAllCaps extends StemmerTestBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    init("allcaps.aff", "allcaps.dic");
  }

  public void testGood() {
    assertStemsTo("OpenOffice.org", "OpenOffice.org");
    assertStemsTo("UNICEF's", "UNICEF");

    // Hunspell returns these title-cased stems, so for consistency we do, too
    assertStemsTo("OPENOFFICE.ORG", "Openoffice.org");
    assertStemsTo("UNICEF'S", "Unicef");
  }

  public void testWrong() {
    assertStemsTo("Openoffice.org");
    assertStemsTo("Unicef");
    assertStemsTo("Unicef's");
  }
}
