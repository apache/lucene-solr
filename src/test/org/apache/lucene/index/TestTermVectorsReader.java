package org.apache.lucene.index;

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

import junit.framework.TestCase;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.util.Arrays;

public class TestTermVectorsReader extends TestCase {
  private TermVectorsWriter writer = null;
  //Must be lexicographically sorted, will do in setup, versus trying to maintain here
  private String [] testFields = {"f1", "f2", "f3"};
  private boolean [] testFieldsStorePos = {true, false, true, false};
  private boolean [] testFieldsStoreOff = {true, false, false, true};  
  private String [] testTerms = {"this", "is", "a", "test"};
  private int [][] positions = new int[testTerms.length][];
  private TermVectorOffsetInfo [][] offsets = new TermVectorOffsetInfo[testTerms.length][];
  private RAMDirectory dir = new RAMDirectory();
  private String seg = "testSegment";
  private FieldInfos fieldInfos = new FieldInfos();

  public TestTermVectorsReader(String s) {
    super(s);
  }

  protected void setUp() throws IOException {
    for (int i = 0; i < testFields.length; i++) {
      fieldInfos.add(testFields[i], true, true, testFieldsStorePos[i], testFieldsStoreOff[i]);
    }
    
    for (int i = 0; i < testTerms.length; i++)
    {
      positions[i] = new int[3];
      for (int j = 0; j < positions[i].length; j++) {
        // poditions are always sorted in increasing order
        positions[i][j] = (int)(j * 10 + Math.random() * 10);
      }
      offsets[i] = new TermVectorOffsetInfo[3];
      for (int j = 0; j < offsets[i].length; j++){
        // ofsets are alway sorted in increasing order
        offsets[i][j] = new TermVectorOffsetInfo(j * 10, j * 10 + testTerms[i].length());
      }        
    }
    Arrays.sort(testTerms);
    for (int j = 0; j < 5; j++) {
      writer = new TermVectorsWriter(dir, seg, fieldInfos);
      writer.openDocument();

      for (int k = 0; k < testFields.length; k++) {
        writer.openField(testFields[k]);
        for (int i = 0; i < testTerms.length; i++) {
          writer.addTerm(testTerms[i], 3, positions[i], offsets[i]);      
        }
        writer.closeField();
      }
      writer.closeDocument();
      writer.close();
    }
  }

  protected void tearDown() {

  }

  public void test() {
      //Check to see the files were created properly in setup
      assertTrue(writer.isDocumentOpen() == false);          
      assertTrue(dir.fileExists(seg + TermVectorsWriter.TVD_EXTENSION));
      assertTrue(dir.fileExists(seg + TermVectorsWriter.TVX_EXTENSION));
  }
  
  public void testReader() throws IOException {
    TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos);
    assertTrue(reader != null);
    TermFreqVector vector = reader.get(0, testFields[0]);
    assertTrue(vector != null);
    String [] terms = vector.getTerms();
    assertTrue(terms != null);
    assertTrue(terms.length == testTerms.length);
    for (int i = 0; i < terms.length; i++) {
      String term = terms[i];
      //System.out.println("Term: " + term);
      assertTrue(term.equals(testTerms[i]));
    }
  }  
  
  public void testPositionReader() throws IOException {
    TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos);
    assertTrue(reader != null);
    TermPositionVector vector;
    String [] terms;
    vector = (TermPositionVector)reader.get(0, testFields[0]);
    assertTrue(vector != null);      
    terms = vector.getTerms();
    assertTrue(terms != null);
    assertTrue(terms.length == testTerms.length);
    for (int i = 0; i < terms.length; i++) {
      String term = terms[i];
      //System.out.println("Term: " + term);
      assertTrue(term.equals(testTerms[i]));
      int [] positions = vector.getTermPositions(i);
      assertTrue(positions != null);
      assertTrue(positions.length == this.positions[i].length);
      for (int j = 0; j < positions.length; j++) {
        int position = positions[j];
        assertTrue(position == this.positions[i][j]);
      }
      TermVectorOffsetInfo [] offset = vector.getOffsets(i);
      assertTrue(offset != null);
      assertTrue(offset.length == this.offsets[i].length);
      for (int j = 0; j < offset.length; j++) {
        TermVectorOffsetInfo termVectorOffsetInfo = offset[j];
        assertTrue(termVectorOffsetInfo.equals(offsets[i][j]));
      }
    }
    
    TermFreqVector freqVector = reader.get(0, testFields[1]); //no pos, no offset
    assertTrue(freqVector != null);      
    assertTrue(freqVector instanceof TermPositionVector == false);
    terms = freqVector.getTerms();
    assertTrue(terms != null);
    assertTrue(terms.length == testTerms.length);
    for (int i = 0; i < terms.length; i++) {
      String term = terms[i];
      //System.out.println("Term: " + term);
      assertTrue(term.equals(testTerms[i]));        
    }
  }
  
  public void testOffsetReader() throws IOException {
    TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos);
    assertTrue(reader != null);
    TermPositionVector vector = (TermPositionVector)reader.get(0, testFields[0]);
    assertTrue(vector != null);
    String [] terms = vector.getTerms();
    assertTrue(terms != null);
    assertTrue(terms.length == testTerms.length);
    for (int i = 0; i < terms.length; i++) {
      String term = terms[i];
      //System.out.println("Term: " + term);
      assertTrue(term.equals(testTerms[i]));
      int [] positions = vector.getTermPositions(i);
      assertTrue(positions != null);
      assertTrue(positions.length == this.positions[i].length);
      for (int j = 0; j < positions.length; j++) {
        int position = positions[j];
        assertTrue(position == this.positions[i][j]);
      }
      TermVectorOffsetInfo [] offset = vector.getOffsets(i);
      assertTrue(offset != null);
      assertTrue(offset.length == this.offsets[i].length);
      for (int j = 0; j < offset.length; j++) {
        TermVectorOffsetInfo termVectorOffsetInfo = offset[j];
        assertTrue(termVectorOffsetInfo.equals(offsets[i][j]));
      }
    }
  }
  

  /**
   * Make sure exceptions and bad params are handled appropriately
   */ 
  public void testBadParams() {
    try {
      TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos);
      assertTrue(reader != null);
      //Bad document number, good field number
      reader.get(50, testFields[0]);
      fail();      
    } catch (IOException e) {
      // expected exception
    }
    try {
      TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos);
      assertTrue(reader != null);
      //Bad document number, no field
      reader.get(50);
      fail();      
    } catch (IOException e) {
      // expected exception
    }
    try {
      TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos);
      assertTrue(reader != null);
      //good document number, bad field number
      TermFreqVector vector = reader.get(0, "f50");
      assertTrue(vector == null);      
    } catch (IOException e) {
      fail();
    }
  }    
}
