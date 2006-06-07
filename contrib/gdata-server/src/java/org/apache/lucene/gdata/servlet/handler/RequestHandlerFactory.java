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
 * @author Simon Willnauer 
 *  
 */ 
public abstract class RequestHandlerFactory { 
     
    private static RequestHandlerFactory INSTANCE = null; 
 
    /** 
     * This method creates a singleton instance of the given type. The fist call 
     * will create an instance of the given class which will be returned in 
     * every subsequent call. Any subsequent call to this method will ignore the 
     * given class object.  
     *  
     * @param factoryImplementation - 
     *            the factory implementation (must be a subtype of this Class) 
     *  
     * @return - a singleton instance of the given type 
     *  
     */ 
    public static synchronized RequestHandlerFactory getInstance( 
            Class factoryImplementation) { 
        if (INSTANCE == null) { 
 
            INSTANCE = createInstance(factoryImplementation); 
        } 
        return INSTANCE; 
    } 
 
    /** 
     * Singleton - Pattern using private constructor 
     *  
     */ 
    RequestHandlerFactory() { 
        super(); 
 
    } 
 
    private static RequestHandlerFactory createInstance( 
            final Class qualifiedClass) { 
        if (qualifiedClass == null) 
            throw new IllegalArgumentException( 
                    "Factory class is null -- must be a implementation of org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory"); 
        try { 
            return (RequestHandlerFactory) qualifiedClass.newInstance(); 
        } catch (Exception e) { 
            FactoryImplementationException ex = new FactoryImplementationException( 
                    "Factory implementation could not be created", e.getCause()); 
            ex.setStackTrace(e.getStackTrace()); 
            throw ex; 
        } 
    } 
 
    /** 
     * Creates a UpdateHandler which processes a GDATA UPDATE request. 
     * @return - an RequestHandlerInstance 
     */ 
    public abstract GDataRequestHandler getUpdateHandler(); 
 
    /** 
     * Creates a DeleteHandler which processes a GDATA DELETE request. 
     * @return - an RequestHandlerInstance 
     */ 
    public abstract GDataRequestHandler getDeleteHandler(); 
 
    /** 
     * Creates a QueryHandler which processes a GDATA Query / Get request. 
     * @return - an RequestHandlerInstance 
     */ 
    public abstract GDataRequestHandler getQueryHandler(); 
 
    /** 
     * Creates a InsertHandler which processes a GDATA Insert request. 
     * @return - an RequestHandlerInstance 
     */ 
    public abstract GDataRequestHandler getInsertHandler(); 
     
 
 
    private static class FactoryImplementationException extends 
            RuntimeException { 
 
        /** 
         *  
         */ 
        private static final long serialVersionUID = 3166033278825112569L; 
 
        /** 
         * Constructs a new FactoryImplementationException with the specified 
         * cause and message 
         *  
         * @param arg0 - 
         *            the detail message 
         * @param arg1 - 
         *            the throw cause 
         */ 
        public FactoryImplementationException(String arg0, Throwable arg1) { 
            super(arg0, arg1); 
 
        } 
 
    } 
 
} 
