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

package org.apache.lucene.gdata.search.index;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * @author Simon Willnauer
 * 
 */
class IndexLogReader {

     static Map<String,IndexAction> readIndexLog(File indexLogFile, Map<String,IndexAction> contentMap) throws SAXException,IOException{
        XMLReader reader = XMLReaderFactory.createXMLReader();
        
        Map<String, IndexAction> logContent = contentMap;
        if(logContent == null)
            logContent = new HashMap<String,IndexAction>(64);
        
        reader.setContentHandler(new IndexLogContentHandler(logContent));
        InputSource source = new InputSource(new FileInputStream(indexLogFile));
        try{
        reader.parse(source);
        }catch (SAXException e) {
            /*
             * try to append the Root element end
             * this happens if the server crashes.
             * If it dies while writing an entry the log file has to be fixed manually
             */
            IndexLogWriter.tryCloseRoot(indexLogFile);
            source = new InputSource(new FileInputStream(indexLogFile));
            reader.parse(source);
        }
        return logContent;
    }
    

    private static class IndexLogContentHandler implements ContentHandler {
        private final Map<String, IndexAction> logContent;
        private String currentID;
        private String currentAction;
        private boolean isId;
        private boolean isAction;
        IndexLogContentHandler(final Map<String, IndexAction> content) {
            this.logContent = content;
        }

        /**
         * @see org.xml.sax.ContentHandler#setDocumentLocator(org.xml.sax.Locator)
         */
        public void setDocumentLocator(Locator locator) {
        }

        /**
         * @see org.xml.sax.ContentHandler#startDocument()
         */
        public void startDocument() throws SAXException {
        }

        /**
         * @see org.xml.sax.ContentHandler#endDocument()
         */
        public void endDocument() throws SAXException {
        }

        /**
         * @see org.xml.sax.ContentHandler#startPrefixMapping(java.lang.String, java.lang.String)
         */
        public void startPrefixMapping(String prefix, String uri)
                throws SAXException {
        }

        /**
         * @see org.xml.sax.ContentHandler#endPrefixMapping(java.lang.String)
         */
        public void endPrefixMapping(String prefix) throws SAXException {
        }

        /**
         * @see org.xml.sax.ContentHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
         */
        public void startElement(String uri, String localName, String qName,
                Attributes atts) throws SAXException {
            if(localName.equals("entryid")){
                this.isId = true;
            }else if(localName.equals("action")){
                this.isAction = true;
            }
        }

        /**
         * @see org.xml.sax.ContentHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
         */
        public void endElement(String uri, String localName, String qName)
                throws SAXException {
            if(localName.equals("entryid")){
                this.isId = false;
            }else if(localName.equals("action")){
                this.isAction = false;
            }else if(localName.equals("indexentry")){
                this.logContent.put(this.currentID,IndexAction.valueOf(this.currentAction));
            }
        }

        /**
         * @see org.xml.sax.ContentHandler#characters(char[], int, int)
         */
        public void characters(char[] ch, int start, int length)
                throws SAXException {
            if(this.isId)
                this.currentID = new String(ch,start,length);
            if(this.isAction)
                this.currentAction = new String(ch,start,length);
            
        }

        /**
         * @see org.xml.sax.ContentHandler#ignorableWhitespace(char[], int, int)
         */
        public void ignorableWhitespace(char[] ch, int start, int length)
                throws SAXException {
        }

        /**
         * @see org.xml.sax.ContentHandler#processingInstruction(java.lang.String, java.lang.String)
         */
        public void processingInstruction(String target, String data)
                throws SAXException {
        }

        /**
         * @see org.xml.sax.ContentHandler#skippedEntity(java.lang.String)
         */
        public void skippedEntity(String name) throws SAXException {
        }
        
        

    }
    
   
}
