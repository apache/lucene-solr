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
package org.apache.lucene.gdata.storage.lucenestorage;

import org.apache.lucene.gdata.storage.lucenestorage.SingleHostConcurrentStorageLock.ConcurrencyException;

import junit.framework.TestCase;

public class TestSingelHostConcurrencyLock extends TestCase {
    SingleHostConcurrentStorageLock lock;
    boolean threadResult = false;
    protected void setUp() throws Exception {
        this.lock = (SingleHostConcurrentStorageLock)SingleHostConcurrentStorageLock.getConcurrentStorageLock();
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        threadResult = false;
        this.lock.close();
    }

    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.SingleHostConcurrentStorageLock.getConcurrentStorageLock()'
     */
    public void testGetConcurrentStorageLock() {
        ConcurrentStorageLock lock = SingleHostConcurrentStorageLock.getConcurrentStorageLock();
        assertEquals(lock,SingleHostConcurrentStorageLock.getConcurrentStorageLock() );
        
    }

    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.SingleHostConcurrentStorageLock.setLock(String)'
     */
    public void testSetLock() throws InterruptedException {
        final String key = "someKey";
        final String nextKey = "fooKey";
        assertTrue(lock.setLock(key));
        assertTrue(lock.isKeyLocked(key));
        
        try{
            this.lock.setLock(key);
            fail("thread has already locked the key");
        }catch (Exception e) {
            // TODO: handle exception
        }
        
        try{
            assertTrue(lock.setLock(nextKey));
            fail("thread has already locked the key");
        }catch (Exception e) {
            // TODO: handle exception
        }
        
        Thread t = new Thread(new Runnable(){
            public void run(){
                threadResult = lock.setLock(key);
              
                
                
            }
        });
        t.start();
        t.join(300);
        assertFalse(threadResult);
        
        t = new Thread(new Runnable(){
            public void run(){
                threadResult = lock.setLock(nextKey);
              
                
                
            }
        });
        t.start();
        t.join(300);
        assertTrue(threadResult);
    }

    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.SingleHostConcurrentStorageLock.releaseLock(String)'
     */
    public void testReleaseLock() throws InterruptedException {
        final String key = "someKey";
        final String nextKey = "fooKey";
        assertTrue(lock.setLock(key));
        assertTrue(lock.isKeyLocked(key));
        assertTrue(lock.releaseLock(key));
        assertTrue(this.lock.setLock(key));
        try{
            assertTrue(lock.setLock(nextKey));
            fail("thread has already locked the key");
        }catch (Exception e) {
            // TODO: handle exception
        }
        
        Thread t = new Thread(new Runnable(){
            public void run(){
                threadResult = lock.setLock(nextKey);
            }
        });
        t.start();
        t.join(300);
        assertTrue(threadResult);
        try{
        this.lock.releaseLock(nextKey);
           fail("current thread is not owner");
        }catch (ConcurrencyException e) {
            // TODO: handle exception
        }

    }

    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.SingleHostConcurrentStorageLock.releaseThreadLocks()'
     */
    public void testReleaseThreadLocks() {
        
    }

    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.SingleHostConcurrentStorageLock.isKeyLocked(String)'
     */
    public void testIsKeyLocked() {

    }

    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.SingleHostConcurrentStorageLock.close()'
     */
    public void testClose() {

    }

}
