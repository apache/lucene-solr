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
import org.apache.lucene.gdata.search.config.IndexSchema;
import org.apache.lucene.gdata.search.config.IndexSchemaField;
import org.apache.lucene.gdata.server.registry.configuration.ComponentConfiguration;
import org.apache.lucene.gdata.utils.SimpleSaxErrorHandler;
import org.apache.xerces.parsers.SAXParser;
import org.xml.sax.SAXException;

/**
 * Reads the configuration file and creates the
 * {@link org.apache.lucene.gdata.server.registry.GDataServerRegistry} singleton
 * instance. All services and components will be instantiated and registered in
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
     *             if an IOException occurs while reading the config file
     * @throws SAXException -
     *             if the config file can not be parsed
     */
    static void buildRegistry() throws IOException, SAXException {
        String schemaFile = RegistryBuilder.class.getResource("/gdata-config.xsd").getFile();
        /*
         * Force using apache xerces parser for digester
         */
        SAXParser parser = new SAXParser();
        parser.setFeature("http://apache.org/xml/features/validation/schema-full-checking",true);
        parser.setFeature("http://apache.org/xml/features/validation/schema",true);
        parser.setFeature("http://xml.org/sax/features/validation",true); 
        parser.setProperty("http://apache.org/xml/properties/schema/external-noNamespaceSchemaLocation",schemaFile);
        Digester digester = new Digester(parser);
        buildFromConfiguration(digester, GDataServerRegistry
                .getRegistry(),schemaFile);

    }

    private static void buildFromConfiguration(Digester digester,
            GDataServerRegistry registry, String schemaURL) throws IOException, SAXException {
        digester.setValidating(true);
        digester.setSchema(schemaURL);
        digester.setErrorHandler(new SimpleSaxErrorHandler());
        digester.push(registry);
        /*
         * register services
         */
        digester.addObjectCreate("gdata/service", ProvidedServiceConfig.class);
        digester.addSetProperties("gdata/service");
        digester.addSetNext("gdata/service", "registerService");
        digester.addBeanPropertySetter("gdata/service/feed-class", "feedType");
        digester.addBeanPropertySetter("gdata/service/entry-class", "entryType");
        digester.addBeanPropertySetter("gdata/service/extension-profile",
                "extensionProfileClass");
        digester.addBeanPropertySetter("gdata/service/previewStyleSheet","xsltStylesheet");
        addIndexRule(digester);
        /*
         * load components and configurations
         */
        digester.addCallMethod("gdata/server-components/component",
                "registerComponent", 2, new Class[] { Class.class , ComponentConfiguration.class});
        digester.addCallParam("gdata/server-components/component/class",0);
            digester.addObjectCreate("gdata/server-components/component/configuration",ComponentConfiguration.class);
            digester.addCallMethod("gdata/server-components/component/configuration/property","set",2,new Class[]{String.class,String.class});
            digester.addCallParam("gdata/server-components/component/configuration/property",0,"name");
            digester.addCallParam("gdata/server-components/component/configuration/property",1);
        //second parameter on registerComponent -- top of the stack (Component configuration)
        digester.addCallParam("gdata/server-components/component/configuration",1,0);    
        digester.parse(RegistryBuilder.class
                .getResourceAsStream("/gdata-config.xml"));
        
    }
    
    
    private static void addIndexRule(Digester digester){
        digester.addObjectCreate("gdata/service/index-schema", IndexSchema.class);
        digester.addSetNext("gdata/service/index-schema", "setIndexSchema");
        digester.addSetProperties("gdata/service/index-schema");
        digester.addSetProperties("gdata/service/index-schema/index");
        digester.addBeanPropertySetter("gdata/service/index-schema/index/maxMergeDocs");
        digester.addBeanPropertySetter("gdata/service/index-schema/index/maxBufferedDocs");
        digester.addBeanPropertySetter("gdata/service/index-schema/index/maxFieldLength");
        digester.addBeanPropertySetter("gdata/service/index-schema/index/mergeFactor");
        digester.addBeanPropertySetter("gdata/service/index-schema/index/indexLocation");
        digester.addBeanPropertySetter("gdata/service/index-schema/index/useCompoundFile");
        digester.addCallMethod("gdata/service/index-schema/index/defaultAnalyzer", "serviceAnalyzer",1,new Class[]{Class.class});
        
        //call method on top of the stack addSchemaField
        digester.addCallMethod("gdata/service/index-schema/field","addSchemaField",1,new Class[]{IndexSchemaField.class});
        digester.addObjectCreate("gdata/service/index-schema/field",IndexSchemaField.class);
        //set parameter for method call -- parameter is IndexSchemaField
        digester.addCallParam("gdata/service/index-schema/field",0,0);
        digester.addSetProperties("gdata/service/index-schema/field");
        digester.addBeanPropertySetter("gdata/service/index-schema/field/path");
        digester.addBeanPropertySetter("gdata/service/index-schema/field/store","storeByName");
        digester.addBeanPropertySetter("gdata/service/index-schema/field/index","indexByName");
        digester.addBeanPropertySetter("gdata/service/index-schema/field/analyzer","analyzerClass");
        
//      call method on top of the stack addSchemaField
        digester.addCallMethod("gdata/service/index-schema/custom","addSchemaField",1,new Class[]{IndexSchemaField.class});
        digester.addObjectCreate("gdata/service/index-schema/custom",IndexSchemaField.class);
        //set parameter for method call -- parameter is IndexSchemaField
        digester.addCallParam("gdata/service/index-schema/custom",0,0);
        digester.addSetProperties("gdata/service/index-schema/custom");
        digester.addBeanPropertySetter("gdata/service/index-schema/custom/path");
        digester.addBeanPropertySetter("gdata/service/index-schema/custom/store","storeByName");
        digester.addBeanPropertySetter("gdata/service/index-schema/custom/index","indexByName");
        digester.addBeanPropertySetter("gdata/service/index-schema/custom/analyzer","analyzerClass");
        digester.addBeanPropertySetter("gdata/service/index-schema/custom/field-class","fieldClass");
//        digester.addCallMethod("gdata/service/index-schema/custom/field-class","fieldClass",1,new Class[]{Class.class});
     
     
//      call method on top of the stack addSchemaField
        digester.addCallMethod("gdata/service/index-schema/mixed","addSchemaField",1,new Class[]{IndexSchemaField.class});
        digester.addObjectCreate("gdata/service/index-schema/mixed",IndexSchemaField.class);
        //set parameter for method call -- parameter is IndexSchemaField
        digester.addCallParam("gdata/service/index-schema/mixed",0,0);
        digester.addSetProperties("gdata/service/index-schema/mixed");
        digester.addBeanPropertySetter("gdata/service/index-schema/mixed","type");
        digester.addBeanPropertySetter("gdata/service/index-schema/mixed/path");
        digester.addBeanPropertySetter("gdata/service/index-schema/mixed/store","storeByName");
        digester.addBeanPropertySetter("gdata/service/index-schema/mixed/index","indexByName");
        digester.addBeanPropertySetter("gdata/service/index-schema/mixed/contenttype","typePath");
        digester.addBeanPropertySetter("gdata/service/index-schema/mixed/analyzer","analyzerClass");
        
    }


}
