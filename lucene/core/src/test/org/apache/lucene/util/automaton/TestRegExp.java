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
package org.apache.lucene.util.automaton;


import org.apache.lucene.util.LuceneTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

public class TestRegExp extends LuceneTestCase {

  /**
   * Simple smoke test for regular expression.
   */
  public void testSmoke() {
    RegExp r = new RegExp("a(b+|c+)d");
    Automaton a = r.toAutomaton();
    assertTrue(a.isDeterministic());
    CharacterRunAutomaton run = new CharacterRunAutomaton(a);
    assertTrue(run.run("abbbbbd"));
    assertTrue(run.run("acd"));
    assertFalse(run.run("ad"));
  }

  /**
   * Compiles a regular expression that is prohibitively expensive to
   * determinize and expexts to catch an exception for it.
   */
  public void testDeterminizeTooManyStates() {
    // LUCENE-6046
    String source = "[ac]*a[ac]{50,200}";
    TooComplexToDeterminizeException expected = expectThrows(TooComplexToDeterminizeException.class, () -> {
      new RegExp(source).toAutomaton();
    });
    assertTrue(expected.getMessage().contains(source));
  }

  // LUCENE-6713
  public void testSerializeTooManyStatesToDeterminizeExc() throws Exception {
    // LUCENE-6046
    String source = "[ac]*a[ac]{50,200}";
    TooComplexToDeterminizeException expected = expectThrows(TooComplexToDeterminizeException.class, () -> {
      new RegExp(source).toAutomaton();
    });
    assertTrue(expected.getMessage().contains(source));

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutput out = new ObjectOutputStream(bos);   
    out.writeObject(expected);
    byte[] bytes = bos.toByteArray();

    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    ObjectInput in = new ObjectInputStream(bis);
    TooComplexToDeterminizeException e2 = (TooComplexToDeterminizeException) in.readObject();
    assertNotNull(e2.getMessage());
  }

  // LUCENE-6046
  public void testRepeatWithEmptyString() throws Exception {
    Automaton a = new RegExp("[^y]*{1,2}").toAutomaton(1000);
    // paranoia:
    assertTrue(a.toString().length() > 0);
  }

  public void testRepeatWithEmptyLanguage() throws Exception {
    Automaton a = new RegExp("#*").toAutomaton(1000);
    // paranoia:
    assertTrue(a.toString().length() > 0);
    a = new RegExp("#+").toAutomaton(1000);
    assertTrue(a.toString().length() > 0);
    a = new RegExp("#{2,10}").toAutomaton(1000);
    assertTrue(a.toString().length() > 0);
    a = new RegExp("#?").toAutomaton(1000);
    assertTrue(a.toString().length() > 0);
  }
}
