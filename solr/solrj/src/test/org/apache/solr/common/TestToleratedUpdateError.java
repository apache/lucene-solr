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
package org.apache.solr.common;

import java.util.EnumSet;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.ToleratedUpdateError.CmdType;
import org.apache.solr.common.util.SimpleOrderedMap;

import org.apache.lucene.util.TestUtil;
import org.junit.Test;

/** Basic testing of the serialization/encapsulation code in ToleratedUpdateError */
public class TestToleratedUpdateError extends SolrTestCase {
  
  private final static CmdType[] ALL_TYPES = EnumSet.allOf(CmdType.class).toArray(new CmdType[0]);
  
  public void testBasics() {
    
    assertFalse((new ToleratedUpdateError(CmdType.ADD, "doc1", "some error")).equals
                (new ToleratedUpdateError(CmdType.ADD, "doc2", "some error")));
    assertFalse((new ToleratedUpdateError(CmdType.ADD, "doc1", "some error")).equals
                (new ToleratedUpdateError(CmdType.ADD, "doc1", "some errorxx")));
    assertFalse((new ToleratedUpdateError(CmdType.ADD, "doc1", "some error")).equals
                (new ToleratedUpdateError(CmdType.DELID, "doc1", "some error")));
  }

  public void testParseMetadataErrorHandling() {

    assertNull(ToleratedUpdateError.parseMetadataIfToleratedUpdateError("some other key", "some value"));

    // see if someone tries to trick us into having an NPE...
    ToleratedUpdateError valid = new ToleratedUpdateError(CmdType.ADD, "doc2", "some error");
    String badKey = valid.getMetadataKey().replace(":", "X");
    assertNull(ToleratedUpdateError.parseMetadataIfToleratedUpdateError(badKey, valid.getMetadataValue()));
  }

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testParseMapErrorChecking() {
    SimpleOrderedMap<String> bogus = new SimpleOrderedMap<>();
    SolrException e = expectThrows(SolrException.class, () -> ToleratedUpdateError.parseMap(bogus));
    assertTrue(e.toString(), e.getMessage().contains("Map does not represent a ToleratedUpdateError"));

    bogus.add("id", "some id");
    bogus.add("message", "some message");
    e = expectThrows(SolrException.class, () -> ToleratedUpdateError.parseMap(bogus));
    assertTrue(e.toString(), e.getMessage().contains("Map does not represent a ToleratedUpdateError"));
    
    bogus.add("type", "not a real type");
    e = expectThrows(SolrException.class, () -> ToleratedUpdateError.parseMap(bogus));
    assertTrue(e.toString(), e.getMessage().contains("Invalid type"));
  }
  
  @SuppressWarnings({"unchecked"})
  public void testParseMap() {
    // trivial
    @SuppressWarnings({"rawtypes"})
    SimpleOrderedMap valid = new SimpleOrderedMap<String>();
    valid.add("type", CmdType.ADD.toString());
    valid.add("id", "some id");
    valid.add("message", "some message");
    
    ToleratedUpdateError in = ToleratedUpdateError.parseMap(valid);
    compare(in, MAP_COPPIER);
    compare(in, METADATA_COPPIER);

    // randomized
    int numIters = atLeast(5000);
    for (int i = 0; i < numIters; i++) {
      valid = new SimpleOrderedMap<String>();
      valid.add("type", ALL_TYPES[TestUtil.nextInt(random(), 0, ALL_TYPES.length-1)].toString());
      valid.add("id", TestUtil.randomUnicodeString(random()));
      valid.add("message", TestUtil.randomUnicodeString(random()));
      
      in = ToleratedUpdateError.parseMap(valid);
      compare(in, MAP_COPPIER);
      compare(in, METADATA_COPPIER);
    }
  }
  
  public void checkRoundTripComparisons(Coppier coppier) {

    // some simple basics
    for (ToleratedUpdateError in : new ToleratedUpdateError[] {
        new ToleratedUpdateError(CmdType.ADD, "doc1", "some error"),
        new ToleratedUpdateError(CmdType.DELID, "doc1", "some diff error"),
        new ToleratedUpdateError(CmdType.DELQ, "-field:yakko other_field:wakko", "some other error"),
      }) {
      
      compare(in, coppier);
    }

    // randomized testing of non trivial keys/values
    int numIters = atLeast(5000);
    for (int i = 0; i < numIters; i++) {
      ToleratedUpdateError in = new ToleratedUpdateError
        (ALL_TYPES[TestUtil.nextInt(random(), 0, ALL_TYPES.length-1)],
         TestUtil.randomUnicodeString(random()),
         TestUtil.randomUnicodeString(random()));
      compare(in, coppier);
    }
  }

  public void testMetadataRoundTripComparisons(Coppier coppier) {
    checkRoundTripComparisons(METADATA_COPPIER);
  }
  
  public void testMapRoundTripComparisons() {
    checkRoundTripComparisons(MAP_COPPIER);
  }

  /** trivial sanity check */
  public void testMaxErrorsValueConversion() {
    
    assertEquals(-1, ToleratedUpdateError.getUserFriendlyMaxErrors(-1));
    assertEquals(-1, ToleratedUpdateError.getUserFriendlyMaxErrors(Integer.MAX_VALUE));
    
    assertEquals(Integer.MAX_VALUE, ToleratedUpdateError.getEffectiveMaxErrors(Integer.MAX_VALUE));
    assertEquals(Integer.MAX_VALUE, ToleratedUpdateError.getEffectiveMaxErrors(-1));

    for (int val : new int[] {0, 1, 10, 42, 600000 }) {
      assertEquals(val, ToleratedUpdateError.getEffectiveMaxErrors(val));
      assertEquals(val, ToleratedUpdateError.getUserFriendlyMaxErrors(val));
    }
    
  }

  public void compare(ToleratedUpdateError in, Coppier coppier) {
      ToleratedUpdateError out = coppier.copy(in);
      assertNotNull(out);
      compare(in, out);
  }
  
  public void compare(ToleratedUpdateError in, ToleratedUpdateError out) {
    assertEquals(out.getType(), in.getType());
    assertEquals(out.getId(), in.getId());
    assertEquals(out.getMessage(), in.getMessage());
    
    assertEquals(out.hashCode(), in.hashCode());
    assertEquals(out.toString(), in.toString());
    
    assertEquals(in.getMetadataKey(), out.getMetadataKey());
    assertEquals(in.getMetadataValue(), out.getMetadataValue());
    
    assertEquals(out, in);
    assertEquals(in, out);
  }
  
  private static abstract class Coppier {
    public abstract ToleratedUpdateError copy(ToleratedUpdateError in);
  }

  private static final Coppier MAP_COPPIER = new Coppier() {
    public ToleratedUpdateError copy(ToleratedUpdateError in) {
      return ToleratedUpdateError.parseMap(in.getSimpleMap());
    }
  };
  
  private static final Coppier METADATA_COPPIER = new Coppier() {
    public ToleratedUpdateError copy(ToleratedUpdateError in) {
      return ToleratedUpdateError.parseMetadataIfToleratedUpdateError
        (in.getMetadataKey(), in.getMetadataValue());
    }
  };
  
}




