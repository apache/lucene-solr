package org.apache.lucene.index;


import junit.framework.TestCase;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.util.Arrays;

public class TestTermVectorsReader extends TestCase {
  private TermVectorsWriter writer = null;
  //Must be lexicographically sorted, will do in setup, versus trying to maintain here
  private String [] testFields = {"f1", "f2", "f3"};
  private String [] testTerms = {"this", "is", "a", "test"};
  private RAMDirectory dir = new RAMDirectory();
  private String seg = "testSegment";
  private FieldInfos fieldInfos = new FieldInfos();

  public TestTermVectorsReader(String s) {
    super(s);
  }

  protected void setUp() {
    for (int i = 0; i < testFields.length; i++) {
      fieldInfos.add(testFields[i], true, true);
    }
    
    try {
      Arrays.sort(testTerms);
      for (int j = 0; j < 5; j++) {
        writer = new TermVectorsWriter(dir, seg, fieldInfos);
        writer.openDocument();

        for (int k = 0; k < testFields.length; k++) {
          writer.openField(testFields[k]);
          for (int i = 0; i < testTerms.length; i++) {
            writer.addTerm(testTerms[i], i);      
          }
          writer.closeField();
        }
        writer.closeDocument();
        writer.close();
      }

    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
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
  
  public void testReader() {
    try {
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
      
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
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
      TermFreqVector vector = reader.get(50, testFields[0]);
      assertTrue(vector == null);      
    } catch (Exception e) {
      assertTrue(false);
    }
    try {
      TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos);
      assertTrue(reader != null);
      //good document number, bad field number
      TermFreqVector vector = reader.get(0, "f50");
      assertTrue(vector == null);      
    } catch (Exception e) {
      assertTrue(false);
    }
  }    
}
