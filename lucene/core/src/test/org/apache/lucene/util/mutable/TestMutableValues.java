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
package org.apache.lucene.util.mutable;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/**
 * Simple test of the basic contract of the various {@link MutableValue} implementaitons.
 */
public class TestMutableValues extends LuceneTestCase {

  public void testStr() {
    MutableValueStr xxx = new MutableValueStr();
    assert xxx.value.get().equals(new BytesRef()) : "defaults have changed, test utility may not longer be as high";
    assert xxx.exists : "defaults have changed, test utility may not longer be as high";
    assertSanity(xxx);
    MutableValueStr yyy = new MutableValueStr();
    assertSanity(yyy);

    assertEquality(xxx, yyy);

    xxx.exists = false;
    assertSanity(xxx);

    assertInEquality(xxx,yyy);

    yyy.exists = false;
    assertEquality(xxx, yyy);

    xxx.value.clear();
    xxx.value.copyChars("zzz");
    xxx.exists = true;
    assertSanity(xxx);

    assertInEquality(xxx,yyy);

    yyy.value.clear();
    yyy.value.copyChars("aaa");
    yyy.exists = true;
    assertSanity(yyy);

    assertInEquality(xxx,yyy);
    assertTrue(0 < xxx.compareTo(yyy));
    assertTrue(yyy.compareTo(xxx) < 0);

    xxx.copy(yyy);
    assertSanity(xxx);
    assertEquality(xxx, yyy);

    // special BytesRef considerations...

    xxx.exists = false;
    xxx.value.clear(); // but leave bytes alone
    assertInEquality(xxx,yyy);

    yyy.exists = false;
    yyy.value.clear(); // but leave bytes alone
    assertEquality(xxx, yyy);

  }

  public void testDouble() {
    MutableValueDouble xxx = new MutableValueDouble();
    assert xxx.value == 0.0D : "defaults have changed, test utility may not longer be as high";
    assert xxx.exists : "defaults have changed, test utility may not longer be as high";
    assertSanity(xxx);
    MutableValueDouble yyy = new MutableValueDouble();
    assertSanity(yyy);

    assertEquality(xxx, yyy);

    xxx.exists = false;
    assertSanity(xxx);

    assertInEquality(xxx,yyy);

    yyy.exists = false;
    assertEquality(xxx, yyy);

    xxx.value = 42.0D;
    xxx.exists = true;
    assertSanity(xxx);

    assertInEquality(xxx,yyy);

    yyy.value = -99.0D;
    yyy.exists = true;
    assertSanity(yyy);

    assertInEquality(xxx,yyy);
    assertTrue(0 < xxx.compareTo(yyy));
    assertTrue(yyy.compareTo(xxx) < 0);

    xxx.copy(yyy);
    assertSanity(xxx);
    assertEquality(xxx, yyy);
  }

  public void testInt() {
    MutableValueInt xxx = new MutableValueInt();
    assert xxx.value == 0 : "defaults have changed, test utility may not longer be as high";
    assert xxx.exists : "defaults have changed, test utility may not longer be as high";
    assertSanity(xxx);
    MutableValueInt yyy = new MutableValueInt();
    assertSanity(yyy);

    assertEquality(xxx, yyy);

    xxx.exists = false;
    assertSanity(xxx);

    assertInEquality(xxx,yyy);

    yyy.exists = false;
    assertEquality(xxx, yyy);

    xxx.value = 42;
    xxx.exists = true;
    assertSanity(xxx);

    assertInEquality(xxx,yyy);

    yyy.value = -99;
    yyy.exists = true;
    assertSanity(yyy);

    assertInEquality(xxx,yyy);
    assertTrue(0 < xxx.compareTo(yyy));
    assertTrue(yyy.compareTo(xxx) < 0);

    xxx.copy(yyy);
    assertSanity(xxx);
    assertEquality(xxx, yyy);
  }

  public void testFloat() {
    MutableValueFloat xxx = new MutableValueFloat();
    assert xxx.value == 0.0F : "defaults have changed, test utility may not longer be as high";
    assert xxx.exists : "defaults have changed, test utility may not longer be as high";
    assertSanity(xxx);
    MutableValueFloat yyy = new MutableValueFloat();
    assertSanity(yyy);

    assertEquality(xxx, yyy);

    xxx.exists = false;
    assertSanity(xxx);

    assertInEquality(xxx,yyy);

    yyy.exists = false;
    assertEquality(xxx, yyy);

    xxx.value = 42.0F;
    xxx.exists = true;
    assertSanity(xxx);

    assertInEquality(xxx,yyy);

    yyy.value = -99.0F;
    yyy.exists = true;
    assertSanity(yyy);

    assertInEquality(xxx,yyy);
    assertTrue(0 < xxx.compareTo(yyy));
    assertTrue(yyy.compareTo(xxx) < 0);

    xxx.copy(yyy);
    assertSanity(xxx);
    assertEquality(xxx, yyy);
  }

  public void testLong() {
    MutableValueLong xxx = new MutableValueLong();
    assert xxx.value == 0L : "defaults have changed, test utility may not longer be as high";
    assert xxx.exists : "defaults have changed, test utility may not longer be as high";
    assertSanity(xxx);
    MutableValueLong yyy = new MutableValueLong();
    assertSanity(yyy);

    assertEquality(xxx, yyy);

    xxx.exists = false;
    assertSanity(xxx);

    assertInEquality(xxx,yyy);

    yyy.exists = false;
    assertEquality(xxx, yyy);

    xxx.value = 42L;
    xxx.exists = true;
    assertSanity(xxx);

    assertInEquality(xxx,yyy);

    yyy.value = -99L;
    yyy.exists = true;
    assertSanity(yyy);

    assertInEquality(xxx,yyy);
    assertTrue(0 < xxx.compareTo(yyy));
    assertTrue(yyy.compareTo(xxx) < 0);

    xxx.copy(yyy);
    assertSanity(xxx);
    assertEquality(xxx, yyy);
  }
  
  public void testBool() {
    MutableValueBool xxx = new MutableValueBool();
    assert xxx.value == false : "defaults have changed, test utility may not longer be as high";
    assert xxx.exists : "defaults have changed, test utility may not longer be as high";
    assertSanity(xxx);
    MutableValueBool yyy = new MutableValueBool();
    assertSanity(yyy);

    assertEquality(xxx, yyy);

    xxx.exists = false;
    assertSanity(xxx);

    assertInEquality(xxx,yyy);

    yyy.exists = false;
    assertEquality(xxx, yyy);

    xxx.value = true;
    xxx.exists = true;
    assertSanity(xxx);

    assertInEquality(xxx,yyy);

    yyy.value = false;
    yyy.exists = true;
    assertSanity(yyy);

    assertInEquality(xxx,yyy);
    assertTrue(0 < xxx.compareTo(yyy));
    assertTrue(yyy.compareTo(xxx) < 0);

    xxx.copy(yyy);
    assertSanity(xxx);
    assertEquality(xxx, yyy);
  }
  

  private void assertSanity(MutableValue x) {
    assertEquality(x, x);
    MutableValue y = x.duplicate();
    assertEquality(x, y);
  }   
   
  private void assertEquality(MutableValue x, MutableValue y) {
    assertEquals(x.hashCode(), y.hashCode());

    assertEquals(x, y); 
    assertEquals(y, x);

    assertTrue(x.equalsSameType(y));
    assertTrue(y.equalsSameType(x));

    assertEquals(0, x.compareTo(y));
    assertEquals(0, y.compareTo(x));

    assertEquals(0, x.compareSameType(y));
    assertEquals(0, y.compareSameType(x));
  } 
     
  private void assertInEquality(MutableValue x, MutableValue y) {
    assertFalse(x.equals(y));
    assertFalse(y.equals(x));

    assertFalse(x.equalsSameType(y));
    assertFalse(y.equalsSameType(x));

    assertFalse(0 == x.compareTo(y));
    assertFalse(0 == y.compareTo(x));
  }      

}
