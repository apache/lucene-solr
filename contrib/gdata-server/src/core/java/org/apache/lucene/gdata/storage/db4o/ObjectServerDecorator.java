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

package org.apache.lucene.gdata.storage.db4o;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

import com.db4o.Db4o;
import com.db4o.ObjectServer;

/**
 * @author Simon Willnauer
 *
 */
public class ObjectServerDecorator implements InvocationHandler {
    private final int port;
    private final String user;
    private final String password;
    private final String host;
    private Method openClient;
    /**
     * 
     */
    public ObjectServerDecorator(String user, String password, String host, int port) {
        this.user = user;
        this.password = password;
        this.host = host;
        this.port = port;
        try {
            this.openClient = ObjectServer.class.getMethod("openClient",new Class[]{});
        } catch (Exception e) {
         //ignore method is visible   
            e.printStackTrace();
        }
    }

    /**
     * @see java.lang.reflect.InvocationHandler#invoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
     */
    public Object invoke(Object arg0, Method arg1, Object[] arg2)
            throws Throwable {
        if(arg1.equals(this.openClient)){
            return Db4o.openClient(this.host,this.port, this.user, this.password);
        }
        Class clazz = arg1.getReturnType();
        
        if(!clazz.isPrimitive())
            return null;
        if(clazz == Boolean.TYPE)
            return false;
        return 0;
    }

}
