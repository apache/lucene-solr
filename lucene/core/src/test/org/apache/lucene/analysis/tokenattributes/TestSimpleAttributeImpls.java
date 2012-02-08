package org.apache.lucene.analysis.tokenattributes;

/**
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

import org.apache.lucene.index.Payload;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.AttributeSource.AttributeFactory;
import org.apache.lucene.util._TestUtil;

import java.util.Collections;
import java.util.HashMap;

@Deprecated
public class TestSimpleAttributeImpls extends LuceneTestCase {
  
  public void testFlagsAttribute() throws Exception {
    FlagsAttributeImpl att = new FlagsAttributeImpl();
    assertEquals(0, att.getFlags());

    att.setFlags(1234);
    assertEquals("flags=1234", att.toString());

    FlagsAttributeImpl att2 = (FlagsAttributeImpl) assertCloneIsEqual(att);
    assertEquals(1234, att2.getFlags());

    att2 = (FlagsAttributeImpl) assertCopyIsEqual(att);
    assertEquals(1234, att2.getFlags());
    
    att.clear();
    assertEquals(0, att.getFlags());
    
    _TestUtil.assertAttributeReflection(att,
      Collections.singletonMap(FlagsAttribute.class.getName() + "#flags", att.getFlags()));
  }
  
  public void testPositionIncrementAttribute() throws Exception {
    PositionIncrementAttributeImpl att = new PositionIncrementAttributeImpl();
    assertEquals(1, att.getPositionIncrement());

    att.setPositionIncrement(1234);
    assertEquals("positionIncrement=1234", att.toString());

    PositionIncrementAttributeImpl att2 = (PositionIncrementAttributeImpl) assertCloneIsEqual(att);
    assertEquals(1234, att2.getPositionIncrement());

    att2 = (PositionIncrementAttributeImpl) assertCopyIsEqual(att);
    assertEquals(1234, att2.getPositionIncrement());
    
    att.clear();
    assertEquals(1, att.getPositionIncrement());
    
    _TestUtil.assertAttributeReflection(att,
      Collections.singletonMap(PositionIncrementAttribute.class.getName() + "#positionIncrement", att.getPositionIncrement()));
  }
  
  public void testTypeAttribute() throws Exception {
    TypeAttributeImpl att = new TypeAttributeImpl();
    assertEquals(TypeAttribute.DEFAULT_TYPE, att.type());

    att.setType("hallo");
    assertEquals("type=hallo", att.toString());

    TypeAttributeImpl att2 = (TypeAttributeImpl) assertCloneIsEqual(att);
    assertEquals("hallo", att2.type());

    att2 = (TypeAttributeImpl) assertCopyIsEqual(att);
    assertEquals("hallo", att2.type());
    
    att.clear();
    assertEquals(TypeAttribute.DEFAULT_TYPE, att.type());
    
    _TestUtil.assertAttributeReflection(att,
      Collections.singletonMap(TypeAttribute.class.getName() + "#type", att.type()));
  }
  
  public void testPayloadAttribute() throws Exception {
    PayloadAttributeImpl att = new PayloadAttributeImpl();
    assertNull(att.getPayload());

    Payload pl = new Payload(new byte[]{1,2,3,4});
    att.setPayload(pl);
    
    _TestUtil.assertAttributeReflection(att,
      Collections.singletonMap(PayloadAttribute.class.getName() + "#payload", pl));

    PayloadAttributeImpl att2 = (PayloadAttributeImpl) assertCloneIsEqual(att);
    assertEquals(pl, att2.getPayload());
    assertNotSame(pl, att2.getPayload());

    att2 = (PayloadAttributeImpl) assertCopyIsEqual(att);
    assertEquals(pl, att2.getPayload());
    assertNotSame(pl, att2.getPayload());
    
    att.clear();
    assertNull(att.getPayload());
  }
  
  public void testOffsetAttribute() throws Exception {
    OffsetAttributeImpl att = new OffsetAttributeImpl();
    assertEquals(0, att.startOffset());
    assertEquals(0, att.endOffset());

    att.setOffset(12, 34);
    // no string test here, because order unknown
    
    _TestUtil.assertAttributeReflection(att,
      new HashMap<String,Object>() {{
        put(OffsetAttribute.class.getName() + "#startOffset", 12);
        put(OffsetAttribute.class.getName() + "#endOffset", 34);
      }});

    OffsetAttributeImpl att2 = (OffsetAttributeImpl) assertCloneIsEqual(att);
    assertEquals(12, att2.startOffset());
    assertEquals(34, att2.endOffset());

    att2 = (OffsetAttributeImpl) assertCopyIsEqual(att);
    assertEquals(12, att2.startOffset());
    assertEquals(34, att2.endOffset());
    
    att.clear();
    assertEquals(0, att.startOffset());
    assertEquals(0, att.endOffset());
  }
  
  public void testKeywordAttribute() {
    AttributeImpl attrImpl = AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY.createAttributeInstance(KeywordAttribute.class);
    assertSame(KeywordAttributeImpl.class, attrImpl.getClass());
    KeywordAttributeImpl att = (KeywordAttributeImpl) attrImpl;
    assertFalse(att.isKeyword());
    att.setKeyword(true);
    assertTrue(att.isKeyword());
    
    KeywordAttributeImpl assertCloneIsEqual = (KeywordAttributeImpl) assertCloneIsEqual(att);
    assertTrue(assertCloneIsEqual.isKeyword());
    assertCloneIsEqual.clear();
    assertFalse(assertCloneIsEqual.isKeyword());
    assertTrue(att.isKeyword());
    
    att.copyTo(assertCloneIsEqual);
    assertTrue(assertCloneIsEqual.isKeyword());
    assertTrue(att.isKeyword());
    
    _TestUtil.assertAttributeReflection(att,
      Collections.singletonMap(KeywordAttribute.class.getName() + "#keyword", att.isKeyword()));
  }
  
  public static final AttributeImpl assertCloneIsEqual(AttributeImpl att) {
    AttributeImpl clone = (AttributeImpl) att.clone();
    assertEquals("Clone must be equal", att, clone);
    assertEquals("Clone's hashcode must be equal", att.hashCode(), clone.hashCode());
    return clone;
  }

  public static final AttributeImpl assertCopyIsEqual(AttributeImpl att) throws Exception {
    AttributeImpl copy = att.getClass().newInstance();
    att.copyTo(copy);
    assertEquals("Copied instance must be equal", att, copy);
    assertEquals("Copied instance's hashcode must be equal", att.hashCode(), copy.hashCode());
    return copy;
  }

}
