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
package org.apache.lucene.gdata.storage; 
 
import java.util.ArrayList; 
import java.util.List; 
 
import junit.framework.TestCase; 
 
import org.apache.lucene.gdata.storage.IDGenerator; 
 
/** 
 * @author Simon Willnauer 
 * 
 */ 
public class TestIDGenerator extends TestCase { 
    private IDGenerator idgen; 
 
    private int initialCap = 100; 
 
    @Override 
    protected void setUp() throws Exception { 
        this.idgen = new IDGenerator(this.initialCap); 
         
         
    } 
 
    @Override 
    protected void tearDown() throws Exception {
        
        this.idgen.stopIDGenerator();
       
    } 
 
    /** 
     * Test method for 'org.apache.lucene.gdata.storage.IDGenerator.getUID()' 
     * @throws InterruptedException  
     */ 
    public void testGetUID() throws InterruptedException { 
         
        List<String> idlist = new ArrayList<String>(); 
        //TODO think about a better way to test this 
        for (int i = 0; i < 1000; i++) { 
            String id = this.idgen.getUID(); 
            assertNotNull(id); 
            assertFalse(idlist.contains(id)); 
            idlist.add(id); 
             
                         
             
        } 
 
    } 
 
     
} 
