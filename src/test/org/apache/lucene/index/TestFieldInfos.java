package org.apache.lucene.index;


import junit.framework.TestCase;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.OutputStream;

import java.io.IOException;
import java.util.Map;

//import org.cnlp.utils.properties.ResourceBundleHelper;

public class TestFieldInfos extends TestCase {

  private Document testDoc = new Document();

  public TestFieldInfos(String s) {
    super(s);
  }

  protected void setUp() {
    DocHelper.setupDoc(testDoc);
  }

  protected void tearDown() {
  }

  public void test() {
    //Positive test of FieldInfos
    assertTrue(testDoc != null);
    FieldInfos fieldInfos = new FieldInfos();
    fieldInfos.add(testDoc);
    //Since the complement is stored as well in the fields map
    assertTrue(fieldInfos.size() == 7); //this is 7 b/c we are using the no-arg constructor
    RAMDirectory dir = new RAMDirectory();
    String name = "testFile";
    OutputStream output = dir.createFile(name);
    assertTrue(output != null);
    //Use a RAMOutputStream
    
    try {
      fieldInfos.write(output);
      output.close();
      assertTrue(output.length() > 0);
      FieldInfos readIn = new FieldInfos(dir, name);
      assertTrue(fieldInfos.size() == readIn.size());
      FieldInfo info = readIn.fieldInfo("textField1");
      assertTrue(info != null);
      assertTrue(info.storeTermVector == false);
      
      info = readIn.fieldInfo("textField2");
      assertTrue(info != null);
      assertTrue(info.storeTermVector == true);
      
      dir.close();

    } catch (IOException e) {
      assertTrue(false);
    }

  }
}
