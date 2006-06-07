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
package org.apache.lucene.gdata.server.registry; 
 
import com.google.gdata.data.ExtensionProfile; 
import com.google.gdata.data.Feed; 
 
/** 
 * @author Simon Willnauer 
 * 
 */ 
public class RegistryBuilder { 
 
    /** 
     *  
     */ 
    public static void buildRegistry(){ 
        // TODO Implement this!! -- just for develping purposes 
        GDataServerRegistry reg = GDataServerRegistry.getRegistry(); 
        FeedInstanceConfigurator configurator = new FeedInstanceConfigurator(); 
        configurator.setFeedType(Feed.class); 
        configurator.setFeedId("weblog"); 
        configurator.setExtensionProfileClass(ExtensionProfile.class); 
        reg.registerFeed(configurator); 
         
    } 
     
} 
