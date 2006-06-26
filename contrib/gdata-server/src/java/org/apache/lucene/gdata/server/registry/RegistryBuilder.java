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

import java.io.IOException;

import org.apache.commons.digester.Digester;
import org.xml.sax.SAXException;

/**
 * Reads the configuration file and creates the
 * {@link org.apache.lucene.gdata.server.registry.GDataServerRegistry} singleton
 * instance. All services and components will be instanciated and registered in
 * the registry.
 * 
 * @author Simon Willnauer
 * 
 */
class RegistryBuilder {

    /**
     * builds the {@link GDataServerRegistry} accessible via the
     * {@link GDataServerRegistry#getRegistry()} method
     * 
     * @throws IOException -
     *             if an IOException occures while reading the config file
     * @throws SAXException -
     *             if the config file can not be parsed
     */
    static void buildRegistry() throws IOException, SAXException {

        buildFromConfiguration(new Digester(), GDataServerRegistry
                .getRegistry());

    }

    private static void buildFromConfiguration(Digester digester,
            GDataServerRegistry registry) throws IOException, SAXException {
        
        digester.setValidating(false);
        digester.push(registry);
        digester.addCallMethod("gdata/server-components/component",
                "registerComponent", 0, new Class[] { Class.class });
        digester.addObjectCreate("gdata/service", ProvidedServiceConfig.class);
        digester.addSetProperties("gdata/service");
        digester.addSetNext("gdata/service", "registerService");
        digester.addBeanPropertySetter("gdata/service/feed-class", "feedType");
        digester.addBeanPropertySetter("gdata/service/entry-class", "entryType");
        digester.addBeanPropertySetter("gdata/service/extension-profile",
                "extensionProfileClass");
        digester.parse(RegistryBuilder.class
                .getResourceAsStream("/gdata-config.xml"));
    }

   

}
