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
