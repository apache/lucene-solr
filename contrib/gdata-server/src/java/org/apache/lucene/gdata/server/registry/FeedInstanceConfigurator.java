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
 
/** 
 * @author Simon Willnauer 
 * 
 */ 
public class FeedInstanceConfigurator { 
    private Class feedType; 
    private String feedId; 
    private Class extensionProfileClass; 
    /** 
     * @return Returns the feedType. 
     */ 
    public Class getFeedType() { 
        return this.feedType; 
    } 
    /** 
     * @param feedType The feedType to set. 
     */ 
    public void setFeedType(Class feedType) { 
        this.feedType = feedType; 
    } 
    /** 
     * @return Returns the feedURL. 
     */ 
    public String getFeedId() { 
        return this.feedId; 
    } 
    /** 
     * @param feedURL The feedURL to set. 
     */ 
    public void setFeedId(String feedURL) { 
        this.feedId = feedURL; 
    } 
     
    /** 
     * @return - the extension profile for this feed 
     */ 
    public Class getExtensionProfilClass(){ 
        return this.extensionProfileClass; 
    } 
     
    /** 
     * @param extensionProfilClass 
     */ 
    public void setExtensionProfileClass(Class extensionProfilClass){ 
        this.extensionProfileClass = extensionProfilClass; 
    } 
     
 
} 
