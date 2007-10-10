package org.apache.lucene.index;
/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Map;

public class TestPositionBasedTermVectorMapper extends LuceneTestCase {
  protected String[] tokens;
  protected int[][] thePositions;
  protected TermVectorOffsetInfo[][] offsets;
  protected int numPositions;


  public TestPositionBasedTermVectorMapper(String s) {
    super(s);
  }

  protected void setUp() throws Exception {
    super.setUp();
    tokens = new String[]{"here", "is", "some", "text", "to", "test", "extra"};
    thePositions = new int[tokens.length][];
    offsets = new TermVectorOffsetInfo[tokens.length][];
    numPositions = 0;
    //save off the last one so we can add it with the same positions as some of the others, but in a predictable way
    for (int i = 0; i < tokens.length - 1; i++)
    {
      thePositions[i] = new int[2 * i + 1];//give 'em all some positions
      for (int j = 0; j < thePositions[i].length; j++)
      {
        thePositions[i][j] = numPositions++;
      }
      offsets[i] = new TermVectorOffsetInfo[thePositions[i].length];
      for (int j = 0; j < offsets[i].length; j++) {
        offsets[i][j] = new TermVectorOffsetInfo(j, j + 1);//the actual value here doesn't much matter
      }
    }
    thePositions[tokens.length - 1] = new int[1];
    thePositions[tokens.length - 1][0] = 0;//put this at the same position as "here"
    offsets[tokens.length - 1] = new TermVectorOffsetInfo[1];
    offsets[tokens.length - 1][0] = new TermVectorOffsetInfo(0, 1);
  }

  public void test() throws IOException {
    PositionBasedTermVectorMapper mapper = new PositionBasedTermVectorMapper();
    
    mapper.setExpectations("test", tokens.length, true, true);
    //Test single position
    for (int i = 0; i < tokens.length; i++) {
      String token = tokens[i];
      mapper.map(token, 1, null, thePositions[i]);

    }
    Map map = mapper.getFieldToTerms();
    assertTrue("map is null and it shouldn't be", map != null);
    assertTrue("map Size: " + map.size() + " is not: " + 1, map.size() == 1);
    Map positions = (Map) map.get("test");
    assertTrue("thePositions is null and it shouldn't be", positions != null);
    
    assertTrue("thePositions Size: " + positions.size() + " is not: " + numPositions, positions.size() == numPositions);
    BitSet bits = new BitSet(numPositions);
    for (Iterator iterator = positions.entrySet().iterator(); iterator.hasNext();) {
      Map.Entry entry = (Map.Entry) iterator.next();
      PositionBasedTermVectorMapper.TVPositionInfo info = (PositionBasedTermVectorMapper.TVPositionInfo) entry.getValue();
      assertTrue("info is null and it shouldn't be", info != null);
      int pos = ((Integer) entry.getKey()).intValue();
      bits.set(pos);
      assertTrue(info.getPosition() + " does not equal: " + pos, info.getPosition() == pos);
      assertTrue("info.getOffsets() is null and it shouldn't be", info.getOffsets() != null);
      if (pos == 0)
      {
        assertTrue("info.getTerms() Size: " + info.getTerms().size() + " is not: " + 2, info.getTerms().size() == 2);//need a test for multiple terms at one pos
        assertTrue("info.getOffsets() Size: " + info.getOffsets().size() + " is not: " + 2, info.getOffsets().size() == 2);
      }
      else
      {
        assertTrue("info.getTerms() Size: " + info.getTerms().size() + " is not: " + 1, info.getTerms().size() == 1);//need a test for multiple terms at one pos
        assertTrue("info.getOffsets() Size: " + info.getOffsets().size() + " is not: " + 1, info.getOffsets().size() == 1);
      }
    }
    assertTrue("Bits are not all on", bits.cardinality() == numPositions);
  }



  
}
