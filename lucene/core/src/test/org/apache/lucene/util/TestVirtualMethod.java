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
package org.apache.lucene.util;


public class TestVirtualMethod extends LuceneTestCase {

  private static final VirtualMethod<TestVirtualMethod> publicTestMethod =
    new VirtualMethod<>(TestVirtualMethod.class, "publicTest", String.class);
  private static final VirtualMethod<TestVirtualMethod> protectedTestMethod =
    new VirtualMethod<>(TestVirtualMethod.class, "protectedTest", int.class);

  public void publicTest(String test) {}
  protected void protectedTest(int test) {}
  
  static class TestClass1 extends TestVirtualMethod {
    @Override
    public void publicTest(String test) {}
    @Override
    protected void protectedTest(int test) {}
  }

  static class TestClass2 extends TestClass1 {
    @Override // make it public here
    public void protectedTest(int test) {}
  }

  static class TestClass3 extends TestClass2 {
    @Override
    public void publicTest(String test) {}
  }

  static class TestClass4 extends TestVirtualMethod {
  }

  static class TestClass5 extends TestClass4 {
  }

  public void testGeneral() {
    assertEquals(0, publicTestMethod.getImplementationDistance(this.getClass()));
    assertEquals(1, publicTestMethod.getImplementationDistance(TestClass1.class));
    assertEquals(1, publicTestMethod.getImplementationDistance(TestClass2.class));
    assertEquals(3, publicTestMethod.getImplementationDistance(TestClass3.class));
    assertFalse(publicTestMethod.isOverriddenAsOf(TestClass4.class));
    assertFalse(publicTestMethod.isOverriddenAsOf(TestClass5.class));
    
    assertEquals(0, protectedTestMethod.getImplementationDistance(this.getClass()));
    assertEquals(1, protectedTestMethod.getImplementationDistance(TestClass1.class));
    assertEquals(2, protectedTestMethod.getImplementationDistance(TestClass2.class));
    assertEquals(2, protectedTestMethod.getImplementationDistance(TestClass3.class));
    assertFalse(protectedTestMethod.isOverriddenAsOf(TestClass4.class));
    assertFalse(protectedTestMethod.isOverriddenAsOf(TestClass5.class));
    
    assertTrue(VirtualMethod.compareImplementationDistance(TestClass3.class, publicTestMethod, protectedTestMethod) > 0);
    assertEquals(0, VirtualMethod.compareImplementationDistance(TestClass5.class, publicTestMethod, protectedTestMethod));
  }

  @SuppressWarnings({"rawtypes","unchecked"})
  public void testExceptions() {
    try {
      // cast to Class to remove generics:
      publicTestMethod.getImplementationDistance((Class) LuceneTestCase.class);
      fail("LuceneTestCase is not a subclass and can never override publicTest(String)");
    } catch (IllegalArgumentException arg) {
      // pass
    }
    
    try {
      new VirtualMethod<>(TestVirtualMethod.class, "bogus");
      fail("Method bogus() does not exist, so IAE should be thrown");
    } catch (IllegalArgumentException arg) {
      // pass
    }
    
    try {
      new VirtualMethod<>(TestClass2.class, "publicTest", String.class);
      fail("Method publicTest(String) is not declared in TestClass2, so IAE should be thrown");
    } catch (IllegalArgumentException arg) {
      // pass
    }

    try {
      // try to create a second instance of the same baseClass / method combination
      new VirtualMethod<>(TestVirtualMethod.class, "publicTest", String.class);
      fail("Violating singleton status succeeded");
    } catch (UnsupportedOperationException arg) {
      // pass
    }
  }
  
}
