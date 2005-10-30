package org.apache.lucene.index;


import junit.framework.TestCase;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

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
    assertTrue(fieldInfos.size() == DocHelper.all.size()); //this is all b/c we are using the no-arg constructor
    RAMDirectory dir = new RAMDirectory();
    String name = "testFile";
    IndexOutput output = dir.createOutput(name);
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
      assertTrue(info.omitNorms == false);

      info = readIn.fieldInfo("textField2");
      assertTrue(info != null);
      assertTrue(info.storeTermVector == true);
      assertTrue(info.omitNorms == false);

      info = readIn.fieldInfo("textField3");
      assertTrue(info != null);
      assertTrue(info.storeTermVector == false);
      assertTrue(info.omitNorms == true);

      info = readIn.fieldInfo("omitNorms");
      assertTrue(info != null);
      assertTrue(info.storeTermVector == false);
      assertTrue(info.omitNorms == true);

      dir.close();

    } catch (IOException e) {
      assertTrue(false);
    }

  }
}
