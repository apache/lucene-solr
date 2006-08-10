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
package org.apache.lucene.gdata.storage.db4o;

import java.io.File;
import java.lang.reflect.Proxy;

import com.db4o.Db4o;
import com.db4o.ObjectContainer;
import com.db4o.ObjectServer;

import junit.framework.TestCase;

public class TestObjectServerDecorator extends TestCase {
    ObjectServer decorator;
    ObjectServer actualServer;
    String dbFile = "test.yap";
    protected void setUp() throws Exception {
        decorator = (ObjectServer) Proxy.newProxyInstance(this.getClass().getClassLoader(),new Class[]{ObjectServer.class},new ObjectServerDecorator("u","p","127.0.0.1",10101));
        actualServer= Db4o.openServer(dbFile,10101);
        actualServer.grantAccess("u","p");
        
    }

    protected void tearDown() throws Exception {
        actualServer.close();
        File dbF = new File(dbFile);
        assertTrue(dbF.delete());
        
    }

    /*
     * Test method for 'org.apache.lucene.gdata.storage.db4o.ObjectServerDecorator.invoke(Object, Method, Object[])'
     */
    public void testInvoke() {
        assertFalse(this.decorator.close());
        assertNull(this.decorator.ext());
        assertEquals(0,this.decorator.hashCode());
        ObjectContainer container = this.decorator.openClient(); 
        assertNotNull(container);
        assertTrue(this.decorator.openClient()instanceof ObjectContainer);
        container.close();
        
    }

}
