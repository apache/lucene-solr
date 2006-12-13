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
 * See the License for the specific language governing permissi/**
 * 
 */

package org.apache.lucene.gdata.utils;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

public class TestSimpleObjectPool extends TestCase {
    private Pool testPool;
    private int SIZE = 10;
    protected void setUp() throws Exception {
        this.testPool =new SimpleObjectPool(SIZE,new ObjectFactoryStub());
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /*
     * Test method for 'org.apache.lucene.gdata.utils.SimpleObjectPool.SimpleObjectPool(int, PoolObjectFactory<Type>)'
     */
    public void testSimpleObjectPool() {
        SimpleObjectPool pool = new SimpleObjectPool(1,new ObjectFactoryStub());
        assertEquals(pool.getSize(),SimpleObjectPool.MINIMALSIZE);
        pool = new SimpleObjectPool(-100,new ObjectFactoryStub());
        assertEquals(pool.getSize(),SimpleObjectPool.MINIMALSIZE);
        pool = new SimpleObjectPool(new ObjectFactoryStub());
        assertEquals(pool.getSize(),SimpleObjectPool.DEFAULTSIZE);
        pool = new SimpleObjectPool(100,new ObjectFactoryStub());
        
        assertEquals(100,pool.getSize());
        try{
        pool = new SimpleObjectPool(1,null);
            fail("factory must not be null");
        }catch (Exception e) {
            // TODO: handle exception
        }
        

    }

    /*
     * Test method for 'org.apache.lucene.gdata.utils.SimpleObjectPool.aquire()'
     */
    public void testAquire() {
        List l = new ArrayList(SIZE);
        for (int i = 0; i < SIZE; i++) {
            Object o = this.testPool.aquire();
            assertNotNull(o);
            assertFalse(l.contains(o));
            l.add(o);
            
        }
        for (Object object : l) {
            this.testPool.release(object);
        }
        for (int i = 0; i < SIZE; i++) {
            Object o = this.testPool.aquire();
            assertNotNull(o);
            assertTrue(l.contains(o));
         
            
        }
        
    }

    /*
     * Test method for 'org.apache.lucene.gdata.utils.SimpleObjectPool.release(Type)'
     */
    public void testRelease() {
        List l = new ArrayList(SIZE);
        for (int i = 0; i < SIZE; i++) {
            Object o = this.testPool.aquire();
            assertNotNull(o);
            assertFalse(l.contains(o));
            l.add(o);
            
        }
        
        for (Object object : l) {
            this.testPool.release(object);
        }
        for (int i = 0; i < 10; i++) {
            this.testPool.release(new Object());
        }
        
        for (int i = 0; i < SIZE; i++) {
            Object o = this.testPool.aquire();
            assertNotNull(o);
            assertTrue(l.contains(o));
         
            
        }
        
        //############################
        
        for (Object object : l) {
            this.testPool.release(object);
        }
        
        for (int i = 0; i < SIZE +SIZE; i++) {
            Object o = this.testPool.aquire();
            assertNotNull(o);
            
            if(i>= SIZE)
                assertFalse(l.contains(o));
            else
            assertTrue(l.contains(o));
        }
    }
    
    public void testDestroy(){
        this.testPool.destroy();
        try{
        this.testPool.aquire();
        fail("pool is already closed");
        }catch (Exception e) {
            // TODO: handle exception
        }
        this.testPool.release(new Object());
    }
    
    static class ObjectFactoryStub implements PoolObjectFactory{

        public Object getInstance() {
            
            return new Object();
        }

        

        public void destroyInstance(Object type) {
            //
        }
        
        
    }

}
