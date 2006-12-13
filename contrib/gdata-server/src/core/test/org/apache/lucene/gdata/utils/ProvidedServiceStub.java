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
package org.apache.lucene.gdata.utils;

import javax.xml.transform.Templates;

import org.apache.lucene.gdata.search.config.IndexSchema;
import org.apache.lucene.gdata.server.registry.ProvidedService;

import com.google.gdata.data.Entry;
import com.google.gdata.data.ExtensionProfile;
import com.google.gdata.data.Feed;

public class ProvidedServiceStub implements ProvidedService {

    public static final String SERVICE_NAME = "service";
    private IndexSchema indexSchema;

    public ProvidedServiceStub() {
        super();
        // TODO Auto-generated constructor stub
    }

    public Class getFeedType() {

        return Feed.class;
    }

    public ExtensionProfile getExtensionProfile() {

        return new ExtensionProfile();
    }

    public Class getEntryType() {

        return Entry.class;
    }

    public String getName() {

        return SERVICE_NAME;
    }

    public void destroy() {
    }
    public void setIndexSchema(IndexSchema schema){
        this.indexSchema = schema;
        this.indexSchema.setName(SERVICE_NAME);
    }
    public IndexSchema getIndexSchema() {
        
        return this.indexSchema;
    }

    public Templates getTransformTemplate() {
        
        return null;
    }

  

}
