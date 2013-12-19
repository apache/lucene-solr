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
package org.apache.solr.morphlines.cell;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.Constants;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.handler.extraction.SolrContentHandler;
import org.apache.solr.morphlines.solr.AbstractSolrMorphlineTestBase;
import org.apache.solr.schema.IndexSchema;
import org.apache.tika.metadata.Metadata;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class SolrCellMorphlineTest extends AbstractSolrMorphlineTestBase {

  private Map<String,Integer> expectedRecords = new HashMap<String,Integer>();

  @BeforeClass
  public static void beforeClass2() {
    assumeFalse("FIXME: This test fails under Java 8 due to the Saxon dependency - see SOLR-1301", Constants.JRE_IS_MINIMUM_JAVA8);
    assumeFalse("FIXME: This test fails under J9 due to the Saxon dependency - see SOLR-1301", System.getProperty("java.vm.info", "<?>").contains("IBM J9"));
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    String path = RESOURCES_DIR + "/test-documents";
    expectedRecords.put(path + "/sample-statuses-20120906-141433.avro", 2);
    expectedRecords.put(path + "/sample-statuses-20120906-141433", 2);
    expectedRecords.put(path + "/sample-statuses-20120906-141433.gz", 2);
    expectedRecords.put(path + "/sample-statuses-20120906-141433.bz2", 2);
    expectedRecords.put(path + "/cars.csv", 5);
    expectedRecords.put(path + "/cars.csv.gz", 5);
    expectedRecords.put(path + "/cars.tar.gz", 4);
    expectedRecords.put(path + "/cars.tsv", 5);
    expectedRecords.put(path + "/cars.ssv", 5);
    expectedRecords.put(path + "/test-documents.7z", 9);
    expectedRecords.put(path + "/test-documents.cpio", 9);
    expectedRecords.put(path + "/test-documents.tar", 9);
    expectedRecords.put(path + "/test-documents.tbz2", 9);
    expectedRecords.put(path + "/test-documents.tgz", 9);
    expectedRecords.put(path + "/test-documents.zip", 9);
    expectedRecords.put(path + "/multiline-stacktrace.log", 4);
    
    FileUtils.copyFile(new File(RESOURCES_DIR + "/custom-mimetypes.xml"), new File(tempDir + "/custom-mimetypes.xml"));
  }
  
  @Test
  public void testSolrCellJPGCompressed() throws Exception {
    
    morphline = createMorphline("test-morphlines/solrCellJPGCompressed");    
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
        path + "/testJPEG_EXIF.jpg",
        path + "/testJPEG_EXIF.jpg.gz",
        path + "/testJPEG_EXIF.jpg.tar.gz",
        //path + "/jpeg2000.jp2",
    };
    testDocumentTypesInternal(files, expectedRecords);
  }  

  @Test
  public void testSolrCellXML() throws Exception {
    morphline = createMorphline("test-morphlines/solrCellXML");    
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
        path + "/testXML2.xml",
    };
    testDocumentTypesInternal(files, expectedRecords);
  }  

  @Test
  public void testSolrCellDocumentTypes() throws Exception {
 
    morphline = createMorphline("test-morphlines/solrCellDocumentTypes");    
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
        path + "/testBMPfp.txt",
        path + "/boilerplate.html",
        path + "/NullHeader.docx",
        path + "/testWORD_various.doc",          
        path + "/testPDF.pdf",
        path + "/testJPEG_EXIF.jpg",
        path + "/testJPEG_EXIF.jpg.gz",
        path + "/testJPEG_EXIF.jpg.tar.gz",
        path + "/testXML.xml",          
//        path + "/cars.csv",
//        path + "/cars.tsv",
//        path + "/cars.ssv",
//        path + "/cars.csv.gz",
//        path + "/cars.tar.gz",
        path + "/sample-statuses-20120906-141433.avro",
        path + "/sample-statuses-20120906-141433",
        path + "/sample-statuses-20120906-141433.gz",
        path + "/sample-statuses-20120906-141433.bz2",
    };
    testDocumentTypesInternal(files, expectedRecords);
  }
  
  @Test
  public void testSolrCellDocumentTypes2() throws Exception {
    morphline = createMorphline("test-morphlines/solrCellDocumentTypes");    
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
        path + "/testPPT_various.ppt",
        path + "/testPPT_various.pptx",        
        path + "/testEXCEL.xlsx",
        path + "/testEXCEL.xls", 
        path + "/testPages.pages", 
        //path + "/testNumbers.numbers", 
        //path + "/testKeynote.key",
        
        path + "/testRTFVarious.rtf", 
        path + "/complex.mbox", 
        path + "/test-outlook.msg", 
        path + "/testEMLX.emlx",
//        path + "/testRFC822",  
        path + "/rsstest.rss", 
//        path + "/testDITA.dita", 
        
        path + "/testMP3i18n.mp3", 
        path + "/testAIFF.aif", 
        path + "/testFLAC.flac", 
//        path + "/testFLAC.oga", 
//        path + "/testVORBIS.ogg",  
        path + "/testMP4.m4a", 
        path + "/testWAV.wav", 
//        path + "/testWMA.wma", 
        
        path + "/testFLV.flv", 
//        path + "/testWMV.wmv", 
        
        path + "/testBMP.bmp", 
        path + "/testPNG.png", 
        path + "/testPSD.psd",        
        path + "/testSVG.svg",  
        path + "/testTIFF.tif",     

//        path + "/test-documents.7z", 
//        path + "/test-documents.cpio",
//        path + "/test-documents.tar", 
//        path + "/test-documents.tbz2", 
//        path + "/test-documents.tgz",
//        path + "/test-documents.zip",
//        path + "/test-zip-of-zip.zip",
//        path + "/testJAR.jar",
        
//        path + "/testKML.kml", 
//        path + "/testRDF.rdf", 
        path + "/testVISIO.vsd",
//        path + "/testWAR.war", 
//        path + "/testWindows-x86-32.exe",
//        path + "/testWINMAIL.dat", 
//        path + "/testWMF.wmf", 
    };   
    testDocumentTypesInternal(files, expectedRecords);
  }

  /**
   * Test that the ContentHandler properly strips the illegal characters
   */
  @Test
  public void testTransformValue() {
    String fieldName = "user_name";
    assertFalse("foobar".equals(getFoobarWithNonChars()));

    Metadata metadata = new Metadata();
    // load illegal char string into a metadata field and generate a new document,
    // which will cause the ContentHandler to be invoked.
    metadata.set(fieldName, getFoobarWithNonChars());
    StripNonCharSolrContentHandlerFactory contentHandlerFactory =
      new StripNonCharSolrContentHandlerFactory(DateUtil.DEFAULT_DATE_FORMATS);
    IndexSchema schema = h.getCore().getLatestSchema();
    SolrContentHandler contentHandler =
      contentHandlerFactory.createSolrContentHandler(metadata, new MapSolrParams(new HashMap()), schema);
    SolrInputDocument doc = contentHandler.newDocument();
    String foobar = doc.getFieldValue(fieldName).toString();
    assertTrue("foobar".equals(foobar));
  }

  /**
   * Returns string "foobar" with illegal characters interspersed.
   */
  private String getFoobarWithNonChars() {
    char illegalChar = '\uffff';
    StringBuilder builder = new StringBuilder();
    builder.append(illegalChar).append(illegalChar).append("foo").append(illegalChar)
      .append(illegalChar).append("bar").append(illegalChar).append(illegalChar);
    return builder.toString();
  }

}
