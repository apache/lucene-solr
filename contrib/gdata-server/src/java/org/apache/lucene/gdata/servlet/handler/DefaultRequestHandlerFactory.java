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
 
package org.apache.lucene.gdata.servlet.handler; 
 
/** 
 * Default implementation for RequestHandlerFactory Builds the 
 * {@link org.apache.lucene.gdata.servlet.handler.GDataRequestHandler} 
 * instances. 
 *  
 * @author Simon Willnauer 
 *  
 */ 
public class DefaultRequestHandlerFactory extends RequestHandlerFactory { 
 
    DefaultRequestHandlerFactory() { 
        // 
    } 
 
    /** 
     * @see org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory#getUpdateHandler() 
     */ 
    @Override 
    public GDataRequestHandler getUpdateHandler() { 
 
        return new DefaultUpdateHandler(); 
    } 
 
    /** 
     * @see org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory#getDeleteHandler() 
     */ 
    @Override 
    public GDataRequestHandler getDeleteHandler() { 
 
        return new DefaultDeleteHandler(); 
    } 
 
    /** 
     * @see org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory#getQueryHandler() 
     */ 
    @Override 
    public GDataRequestHandler getQueryHandler() { 
 
        return new DefaultGetHandler(); 
    } 
 
    /** 
     * @see org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory#getInsertHandler() 
     */ 
    @Override 
    public GDataRequestHandler getInsertHandler() { 
 
        return new DefaultInsertHandler(); 
    } 
 
} 
