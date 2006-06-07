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
package org.apache.lucene.gdata.server; 
 
 
/** 
 * The {@link ServiceFactory} creates {@link Service} implementations to access 
 * the GData - Server components. 
 *  
 * @author Simon Willnauer 
 *  
 */ 
public class ServiceFactory { 
 
    private static ServiceFactory INSTANCE = null; 
 
    /** 
     * @return - a Singleton Instance of the factory 
     */ 
    public static synchronized ServiceFactory getInstance() { 
        if (INSTANCE == null) 
            INSTANCE = new ServiceFactory(); 
        return INSTANCE; 
 
    } 
 
    private ServiceFactory() { 
        // private constructor --> singleton 
    } 
 
    /** 
     * Creates a {@link Service} implementation. 
     *  
     * @return a Service Implementation 
     */ 
    public Service getService() { 
        try{ 
        return new GDataService(); 
        }catch (Exception e) { 
            // 
        } 
        return null; 
    } 
} 
