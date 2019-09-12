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

package org.apache.solr.client.solrj.io.stream.expr;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class InjectionDefenseTest {

  private static final String EXPLOITABLE = "let(a=search(foo,q=\"time_dt:[?$? TO ?$?]\",fl=\"id,time_dt\",sort=\"time_dt asc\"))";
  private static final String NUMBER = "let(a=search(foo,q=\"gallons_f:[?#? TO ?#?]\",fl=\"id,gallons_f,time_dt\",sort=\"time_dt asc\"))";
  private static final String NUMBER_OK = "let(a=search(foo,q=\"gallons_f:[2 TO 3.5]\",fl=\"id,gallons_f,time_dt\",sort=\"time_dt asc\"))";
  private static final String ALLOWED = "let(a=search(foo,q=\"time_dt:[?$? TO ?$?]\",fl=\"id,time_dt\",sort=\"time_dt asc\"), x=?(2)?)";
  private static final String INJECTED = "let(a=search(foo,q=\"time_dt:[2000-01-01T00:00:00Z TO 2020-01-01T00:00:00Z]\",fl=\"id,time_dt\",sort=\"time_dt asc\"), x=jdbc( connection=\"jdbc:postgresql://localhost:5432/ouchdb\",sql=\"select * from users\",sort=\"id asc\"),z=jdbc( connection=\"jdbc:postgresql://localhost:5432/ouchdb\",sql=\"select * from race_cars\",sort=\"id asc\"))";

  @Test(expected = InjectedExpressionException.class)
  public void testSafeExpression() {

    InjectionDefense defender = new InjectionDefense(EXPLOITABLE);

    defender.addParameter("2000-01-01T00:00:00Z");
    defender.addParameter("2020-01-01T00:00:00Z]\",fl=\"id\",sort=\"id asc\"), b=jdbc( connection=\"jdbc:postgresql://localhost:5432/ouchdb\",sql=\"select * from users\",sort=\"id asc\"),c=search(foo,q=\"time_dt:[* TO 2020-01-01T00:00:00Z");

    defender.safeExpression();
  }

  @Test(expected = InjectedExpressionException.class)
  public void testSafeString() {

    InjectionDefense defender = new InjectionDefense(EXPLOITABLE);

    defender.addParameter("2000-01-01T00:00:00Z");
    defender.addParameter("2020-01-01T00:00:00Z]\",fl=\"id\",sort=\"id asc\"), b=jdbc( connection=\"jdbc:postgresql://localhost:5432/ouchdb\",sql=\"select * from users\",sort=\"id asc\"),c=search(foo,q=\"time_dt:[* TO 2020-01-01T00:00:00Z");

    defender.safeExpressionString();
  }

  @Test
  public void testExpectedInjectionOfExpressions() {
    InjectionDefense defender = new InjectionDefense(ALLOWED);

    defender.addParameter("2000-01-01T00:00:00Z");
    defender.addParameter("2020-01-01T00:00:00Z");
    defender.addParameter("jdbc( connection=\"jdbc:postgresql://localhost:5432/ouchdb\",sql=\"select * from users\",sort=\"id asc\"),z=jdbc( connection=\"jdbc:postgresql://localhost:5432/ouchdb\",sql=\"select * from race_cars\",sort=\"id asc\")");

    // no exceptions
    assertNotNull(defender.safeExpression());
    assertEquals(INJECTED, defender.safeExpressionString());

  }

  @Test(expected = InjectedExpressionException.class)
  public void testWrongNumberInjected() {
    InjectionDefense defender = new InjectionDefense(ALLOWED);

    defender.addParameter("2000-01-01T00:00:00Z");
    defender.addParameter("2020-01-01T00:00:00Z");
    defender.addParameter("jdbc( connection=\"jdbc:postgresql://localhost:5432/ouchdb\",sql=\"select * from users\",sort=\"id asc\")");

    // no exceptions
    defender.safeExpression();
    assertEquals(INJECTED, defender.safeExpressionString());

  }

  @Test
  public void testBuildExpression() {
    InjectionDefense defender = new InjectionDefense(ALLOWED);

    defender.addParameter("2000-01-01T00:00:00Z");
    defender.addParameter("2020-01-01T00:00:00Z");
    defender.addParameter("jdbc( connection=\"jdbc:postgresql://localhost:5432/ouchdb\",sql=\"select * from users\",sort=\"id asc\"),z=jdbc( connection=\"jdbc:postgresql://localhost:5432/ouchdb\",sql=\"select * from race_cars\",sort=\"id asc\")");

    assertEquals(INJECTED, defender.buildExpression());
  }

  @Test
  public void testInjectNumber() {
    InjectionDefense defender = new InjectionDefense(NUMBER);

    defender.addParameter("2");
    defender.addParameter("3.5");

    assertEquals(NUMBER_OK, defender.buildExpression());
  }

  @Test(expected = NumberFormatException.class)
  public void testInjectAlphaFail() {
    InjectionDefense defender = new InjectionDefense(NUMBER);

    defender.addParameter("a");
    defender.addParameter("3.5");

    defender.buildExpression();

  }
}

