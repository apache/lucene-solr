package org.apache.solr.ltr.util;

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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestNameValidator {

  @Test
  public void testValidator() {
    assertTrue(NameValidator.check("test"));
    assertTrue(NameValidator.check("constant"));
    assertTrue(NameValidator.check("test_test"));
    assertTrue(NameValidator.check("TEst"));
    assertTrue(NameValidator.check("TEST"));
    assertTrue(NameValidator.check("328195082960784"));
    assertFalse(NameValidator.check("    "));
    assertFalse(NameValidator.check(""));
    assertFalse(NameValidator.check("test?"));
    assertFalse(NameValidator.check("??????"));
    assertFalse(NameValidator.check("_____-----"));
    assertFalse(NameValidator.check("12345,67890.31"));
    assertFalse(NameValidator.check("aasdasdadasdzASADADSAZ01239()[]|_-"));
    assertFalse(NameValidator.check(null));
    assertTrue(NameValidator.check("a"));
    assertTrue(NameValidator.check("test()"));
    assertTrue(NameValidator.check("test________123"));

  }
}
