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

/**
 * This interface describes a service provided by the GData-Server.
 * @see org.apache.lucene.gdata.server.registry.GDataServerRegistry 
 * @author Simon Willnauer
 *
 */
public interface ProvidedService {

    /** 
     * @return Returns the feedType. 
     */
    public abstract Class getFeedType();

    /** 
     * @return - the extension profile for this feed 
     */
    public abstract ExtensionProfile getExtensionProfile();

    /**
     * @return the entry Type configured for this Service
     */
    public abstract Class getEntryType();

    /**
     * @return - the servicename
     */
    public abstract String getName();

}