/** 
 * Copyright 2004 The Apache Software Foundation 
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
package org.apache.lucene.gdata.search.index;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.xml.sax.SAXException;

import junit.framework.TestCase;

/**
 * @author Simon Willnauer
 *
 */
public class TestIndexLogReader extends TestCase {
    File logFile;
    Map<String, IndexAction> actions;
    protected void setUp() throws Exception {
        this.logFile = new File(System.getProperty("java.io.tmpdir"),"log");
        this.logFile.deleteOnExit();
        this.logFile.createNewFile();
        this.actions = new HashMap<String,IndexAction>();
   
        
    }
    private IndexLogWriter writeLog() throws IOException{
        this.actions = new HashMap<String,IndexAction>();
        IndexLogWriter writer = new IndexLogWriter(this.logFile);
        boolean totwice = true;
        for (int i = 0; i < 10; i++) {
            IndexAction a = null;
            if(i%3 ==0)
                a= IndexAction.INSERT;
            if(i%3 ==1)
                a= IndexAction.UPDATE;
            if(i%3 ==2)
                a= IndexAction.DELETE;
            this.actions.put(""+i,a);
            writer.writeAction(""+i,a);
            /*
             * double action
             */
            if(i == 9 && totwice){
                i = 0;
                totwice = false;
            }
        }
        return writer;
    }
    protected void tearDown() throws Exception {
        super.tearDown();
        this.logFile.delete();
    }

    /*
     * Test method for 'org.apache.lucene.gdata.search.index.IndexLogReader.readIndexLog(File, Map<String, IndexAction>)'
     */
    public void testReadIndexLog() throws SAXException, IOException {
        writeLog().close();
        
        Map<String,IndexAction> readActionMap = new HashMap<String,IndexAction>();
        IndexLogReader.readIndexLog(this.logFile,readActionMap);
        Set<String> keySet = this.actions.keySet();
        assertEquals(10,keySet.size());
        for (String string : keySet) {
            assertTrue(readActionMap.get(string).equals(actions.get(string)));
        }
        
       
        
    }
    
    /*
     * Test method for 'org.apache.lucene.gdata.search.index.IndexLogReader.readIndexLog(File, Map<String, IndexAction>)'
     */
    public void testReadIndexLogNoInputMap() throws SAXException, IOException {
        writeLog().close();
      
        Map<String,IndexAction> readActionMap = IndexLogReader.readIndexLog(this.logFile,null);
        Set<String> keySet = this.actions.keySet();
        assertEquals(10,keySet.size());
        for (String string : keySet) {
            assertTrue(readActionMap.get(string).equals(actions.get(string)));
        }
        
    }
    
    /*
     * Test method for 'org.apache.lucene.gdata.search.index.IndexLogReader.readIndexLog(File, Map<String, IndexAction>)'
     */
    public void testReadIndexLogFixLogFile() throws SAXException, IOException {
        writeLog();
          
        Map<String,IndexAction> readActionMap = IndexLogReader.readIndexLog(this.logFile,null);
        Set<String> keySet = this.actions.keySet();
        assertEquals(10,keySet.size());
        for (String string : keySet) {
            assertTrue(readActionMap.get(string).equals(actions.get(string)));
        }
        
    }
    
    public void testWriterClosed()throws SAXException, IOException{
        IndexLogWriter writer = writeLog();
        writer.close();
        try{
            writer.writeAction(null,null);
            fail("writer is closed");
        }catch (IllegalStateException e) {
          
        }
        try{
            writer.close();
            fail("writer is closed");
        }catch (IllegalStateException e) {
          
        }
        
        
    }

}
