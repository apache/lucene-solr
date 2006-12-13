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
package org.apache.lucene.gdata.server.administration;

import java.io.IOException;
import java.io.Reader;
import java.net.URL;



import org.apache.commons.digester.Digester;
import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.utils.SimpleSaxErrorHandler;
import org.apache.xerces.parsers.SAXParser;
import org.xml.sax.SAXException;

/**
 * Helper class to create {@link org.apache.lucene.gdata.data.GDataAccount}
 * instances from a xml stream provided via a {@link Reader} instance.
 * 
 * @author Simon Willnauer
 * 
 */
public class AccountBuilder {

    /**
     * Reads the xml from the provided reader and binds the values to the 
     * @param reader - the reader to read the xml from
     * @return - the GDataAccount 
     * @throws IOException - if an IOException occurs
     * @throws SAXException - if the xml can not be parsed by the sax reader
     */
    public static GDataAccount buildAccount(final Reader reader) throws IOException,
            SAXException {
        if (reader == null)
            throw new IllegalArgumentException("Reader must not be null");
        URL resource = AccountBuilder.class.getResource("/gdata-account.xsd");
        if(resource == null)
            throw new RuntimeException("can not find xml schema file 'gdata-account.xsd' -- file must be present on the classpath");
        String schemaFile = resource.getFile();
        GDataAccount account = null;
        /*
         * Force using apache xerces parser for digester
         */
        SAXParser parser = new SAXParser();
        parser.setFeature("http://apache.org/xml/features/validation/schema-full-checking",true);
        parser.setFeature("http://apache.org/xml/features/validation/schema",true);
        parser.setFeature("http://xml.org/sax/features/validation",true); 
        parser.setProperty("http://apache.org/xml/properties/schema/external-noNamespaceSchemaLocation",schemaFile);
        Digester digester = new Digester(parser);
        digester.setValidating(true);
        digester.setErrorHandler(new SimpleSaxErrorHandler());
        digester.setSchema(schemaFile);
        digester.addObjectCreate("account", GDataAccount.class);
        digester.addBeanPropertySetter("account/account-name", "name");
        digester.addBeanPropertySetter("account/password", "password");
        digester.addBeanPropertySetter("account/account-role", "rolesAsInt");
        digester.addBeanPropertySetter("account/account-owner/name",
                "authorname");
        digester.addBeanPropertySetter("account/account-owner/email-address",
                "authorMail");
        digester.addBeanPropertySetter("account/account-owner/url",
                "authorLink");

        account = (GDataAccount) digester.parse(reader);
        return account;
    }
    
    
  
}
