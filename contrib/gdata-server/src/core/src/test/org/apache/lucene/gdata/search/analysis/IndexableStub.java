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
package org.apache.lucene.gdata.search.analysis;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 * @author Simon Willnauer
 *
 */
public class IndexableStub extends Indexable {

    private String content;
    private boolean retNull;
    int times = 1;
    int count = 0;
    IndexableStub() {
        super(null);

    }
    public void returnProxyTimes(int times){
        this.times = times;
    }
    public void setReturnNull(boolean returnNull){
        this.retNull = returnNull;
    }
    public void setReturnValueTextContent(String content){
        this.content = content;
    }
    @Override
    public Node applyPath(String xPath) throws XPathExpressionException {
        if(xPath == null)
            throw new XPathExpressionException("path is null");
        if(this.retNull)
            return null;
        if(times == count)
            return null;
        times++;
        return (Node)Proxy.newProxyInstance(this.getClass().getClassLoader(),new Class[] {Node.class,NamedNodeMap.class},new Handler(this.content));
        
    }

    private static class Handler implements InvocationHandler{
        String returnValue;
        public Handler(String toReturn){
            this.returnValue = toReturn;
        }
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if(method.getName().equals("getNextSibling")){
                return null;
            }
            if(method.getReturnType() == String.class)
                return this.returnValue;
            if(method.getReturnType() == Node.class)
                
            return (Node)Proxy.newProxyInstance(this.getClass().getClassLoader(),new Class[] {Node.class,NamedNodeMap.class},new Handler(this.returnValue));
            if(method.getReturnType() == NamedNodeMap.class)
                return  (NamedNodeMap)Proxy.newProxyInstance(this.getClass().getClassLoader(),new Class[] {Node.class,NamedNodeMap.class},new Handler(this.returnValue));
            return null;
            
        }
        
    }
    


}

