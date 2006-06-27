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

package org.apache.lucene.gdata.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.List;
import java.util.Set;

import org.apache.lucene.gdata.server.registry.ProvidedService;

import com.google.gdata.client.Service;
import com.google.gdata.data.BaseEntry;
import com.google.gdata.data.Content;
import com.google.gdata.data.DateTime;
import com.google.gdata.data.Entry;
import com.google.gdata.data.Extension;
import com.google.gdata.data.ExtensionProfile;
import com.google.gdata.data.Link;
import com.google.gdata.data.Source;
import com.google.gdata.data.TextConstruct;
import com.google.gdata.util.ParseException;
import com.google.gdata.util.ServiceException;
import com.google.gdata.util.XmlBlob;
import com.google.gdata.util.common.xml.XmlWriter;

/**
 * The GData-Server uses the GDATA-Client API for an interal representation of
 * entries. These entities have dynamic elements like Links being generated
 * using the requested URL.<br/> Some components of the server also need
 * additional infomation like the service type
 * {@link org.apache.lucene.gdata.server.registry.ProvidedService} of the entry
 * and the feedid a entry belongs to. All these information are
 * encapsulated in the ServerBaseEntry decorating a concrete sub class of <tt>BaseEntry</tt>. The actual
 * {@link com.google.gdata.data.BaseEntry} will be passed to the ServerBaseEntry
 * at creation time via the constructor. To use the ServerBaseFeed for generation a provided format like
 * RSS/ATOM the corresponding {@link com.google.gdata.data.ExtensionProfile} has
 * to be provided to the generation method.
 * <p> For a general overview of the generic BaseFeed class see the gdata-client API documentation</p>
 * 
 * @see com.google.gdata.data.ExtensionProfile
 * @see com.google.gdata.data.BaseFeed
 * 
 * @author Simon Willnauer
 * 
 */
public class ServerBaseEntry {
    private String feedId;

    private String serviceType;

    private ProvidedService serviceConfig;
    private BaseEntry entry;
    

    
   
    /**
     * @return - the provided service for the source entry 
     */
    public ProvidedService getServiceConfig() {
        return this.serviceConfig;
    }

    /**
     * @param serviceConfig - the provided service for the source entry
     */
    public void setServiceConfig(ProvidedService serviceConfig) {
        this.serviceConfig = serviceConfig;
        if (serviceConfig != null)
            this.serviceType = serviceConfig.getName();
    }

    /**
     * @return - the name of the service related of the feed containing this entry
     */
    public String getServiceType() {
        return this.serviceType;
    }

    /**
     *  Constructs a new ServerBaseEntry.
     *  To provide a concrete entry to decorate after object creation use {@link ServerBaseEntry#setEntry(BaseEntry)}  
     *  
     */
    @SuppressWarnings("unchecked")
    public ServerBaseEntry() {
        
        this.entry = new Entry();
    }

    /**
     * @param arg0 - the source entry
     */
    @SuppressWarnings("unchecked")
    public ServerBaseEntry(BaseEntry arg0) {
        this.entry = arg0;

    }


    /**
     * @param link - a link added to this entry
     */
    @SuppressWarnings("unchecked")
    public void addLink(final Link link) {
        this.entry.getLinks().add(link);
    }

    /**
     * @return - the id of the owning feed
     */
    public String getFeedId() {
        return this.feedId;
    }

    /**
     * @param feedId - the id of the owning feed
     */
    public void setFeedId(String feedId) {
        this.feedId = feedId;
    }
    /**
     * @return - the decorated entry
     */
    public BaseEntry getEntry(){
        return this.entry;
    }
    /**
     * @param entry - the entry to decorate
     */
    public void setEntry(BaseEntry entry){
        this.entry = entry;
    }

    /**
     * @see com.google.gdata.data.BaseEntry#addHtmlLink(java.lang.String, java.lang.String, java.lang.String)
     */
    public void addHtmlLink(String arg0, String arg1, String arg2) {
        
        this.entry.addHtmlLink(arg0, arg1, arg2);
    }



    /**
     * @see com.google.gdata.data.BaseEntry#generateAtom(com.google.gdata.util.common.xml.XmlWriter, com.google.gdata.data.ExtensionProfile)
     */
    
    public void generateAtom(XmlWriter arg0, ExtensionProfile arg1) throws IOException {
        
        this.entry.generateAtom(arg0, arg1);
    }

    /**
     * @see com.google.gdata.data.BaseEntry#generateRss(com.google.gdata.util.common.xml.XmlWriter, com.google.gdata.data.ExtensionProfile)
     */
    
    public void generateRss(XmlWriter arg0, ExtensionProfile arg1) throws IOException {
        
        this.entry.generateRss(arg0, arg1);
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getAuthors()
     */
    
    public List getAuthors() {
        
        return this.entry.getAuthors();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getCanEdit()
     */
    
    public boolean getCanEdit() {
        
        return this.entry.getCanEdit();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getCategories()
     */
    
    public Set getCategories() {
        
        return this.entry.getCategories();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getContent()
     */
    
    public Content getContent() {
        
        return this.entry.getContent();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getContributors()
     */
    
    public List getContributors() {
        
        return this.entry.getContributors();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getEditLink()
     */
    
    public Link getEditLink() {
        
        return this.entry.getEditLink();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getHtmlLink()
     */
    
    public Link getHtmlLink() {
        
        return this.entry.getHtmlLink();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getId()
     */
    
    public String getId() {
        
        return this.entry.getId();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getLink(java.lang.String, java.lang.String)
     */
    
    public Link getLink(String arg0, String arg1) {
        
        return this.entry.getLink(arg0, arg1);
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getLinks()
     */
    
    public List getLinks() {
        
        return this.entry.getLinks();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getPublished()
     */
    
    public DateTime getPublished() {
        
        return this.entry.getPublished();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getRights()
     */
    
    public TextConstruct getRights() {
        
        return this.entry.getRights();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getSelf()
     */
    
    public BaseEntry getSelf() throws IOException, ServiceException {
        
        return this.entry.getSelf();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getSelfLink()
     */
    
    public Link getSelfLink() {
        
        return this.entry.getSelfLink();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getService()
     */
    
    public Service getService() {
        
        return this.entry.getService();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getSource()
     */
    
    public Source getSource() {
        
        return this.entry.getSource();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getSummary()
     */
    
    public TextConstruct getSummary() {
        
        return this.entry.getSummary();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getTitle()
     */
    
    public TextConstruct getTitle() {
        
        return this.entry.getTitle();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getUpdated()
     */
    
    public DateTime getUpdated() {
        
        return this.entry.getUpdated();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#getVersionId()
     */
    
    public String getVersionId() {
        
        return this.entry.getVersionId();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#isDraft()
     */
    
    public boolean isDraft() {
        
        return this.entry.isDraft();
    }

    /**
     * @see com.google.gdata.data.BaseEntry#parseAtom(com.google.gdata.data.ExtensionProfile, java.io.InputStream)
     */
    
    public void parseAtom(ExtensionProfile arg0, InputStream arg1) throws IOException, ParseException {
        
        this.entry.parseAtom(arg0, arg1);
    }

    /**
     * @see com.google.gdata.data.BaseEntry#parseAtom(com.google.gdata.data.ExtensionProfile, java.io.Reader)
     */
    
    public void parseAtom(ExtensionProfile arg0, Reader arg1) throws IOException, ParseException {
        
        this.entry.parseAtom(arg0, arg1);
    }

    /**
     * @see com.google.gdata.data.BaseEntry#setCanEdit(boolean)
     */
    
    public void setCanEdit(boolean arg0) {
        
        this.entry.setCanEdit(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseEntry#setContent(com.google.gdata.data.Content)
     */
    
    public void setContent(Content arg0) {
        
        this.entry.setContent(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseEntry#setContent(com.google.gdata.data.TextConstruct)
     */
    
    public void setContent(TextConstruct arg0) {
        
        this.entry.setContent(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseEntry#setDraft(boolean)
     */
    
    public void setDraft(boolean arg0) {
        
        this.entry.setDraft(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseEntry#setId(java.lang.String)
     */
    
    public void setId(String arg0) {
        
        this.entry.setId(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseEntry#setPublished(com.google.gdata.data.DateTime)
     */
    
    public void setPublished(DateTime arg0) {
        
        this.entry.setPublished(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseEntry#setRights(com.google.gdata.data.TextConstruct)
     */
    
    public void setRights(TextConstruct arg0) {
        
        this.entry.setRights(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseEntry#setService(com.google.gdata.client.Service)
     */
    
    public void setService(Service arg0) {
        
        this.entry.setService(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseEntry#setSource(com.google.gdata.data.Source)
     */
    
    public void setSource(Source arg0) {
        
        this.entry.setSource(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseEntry#setSummary(com.google.gdata.data.TextConstruct)
     */
    
    public void setSummary(TextConstruct arg0) {
        
        this.entry.setSummary(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseEntry#setTitle(com.google.gdata.data.TextConstruct)
     */
    
    public void setTitle(TextConstruct arg0) {
        
        this.entry.setTitle(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseEntry#setUpdated(com.google.gdata.data.DateTime)
     */
    
    public void setUpdated(DateTime arg0) {
        
        this.entry.setUpdated(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseEntry#setVersionId(java.lang.String)
     */
    
    public void setVersionId(String arg0) {
        
        this.entry.setVersionId(arg0);
    }

 
    /**
     * @see com.google.gdata.data.ExtensionPoint#addExtension(com.google.gdata.data.Extension)
     */
    
    public void addExtension(Extension arg0) {
        
        this.entry.addExtension(arg0);
    }

    /**
     * @see com.google.gdata.data.ExtensionPoint#addRepeatingExtension(com.google.gdata.data.Extension)
     */
    
    public void addRepeatingExtension(Extension arg0) {
        
        this.entry.addRepeatingExtension(arg0);
    }

   

    /**
     * @see com.google.gdata.data.ExtensionPoint#generateCumulativeXmlBlob(com.google.gdata.data.ExtensionProfile)
     */
    
    public XmlBlob generateCumulativeXmlBlob(ExtensionProfile arg0) throws IOException {
        
        return this.entry.generateCumulativeXmlBlob(arg0);
    }

  
   
    /**
     * @see com.google.gdata.data.ExtensionPoint#getRepeatingExtension(java.lang.Class)
     */
    
    public <T extends Extension> List<T> getRepeatingExtension(Class<T> arg0) {
        
        return this.entry.getRepeatingExtension(arg0);
    }

    /**
     * @see com.google.gdata.data.ExtensionPoint#getXmlBlob()
     */
    
    public XmlBlob getXmlBlob() {
        
        return this.entry.getXmlBlob();
    }

  

    /**
     * @see com.google.gdata.data.ExtensionPoint#parseCumulativeXmlBlob(com.google.gdata.util.XmlBlob, com.google.gdata.data.ExtensionProfile, java.lang.Class)
     */
    
    public void parseCumulativeXmlBlob(XmlBlob arg0, ExtensionProfile arg1, Class arg2) throws IOException, ParseException {
        
        this.entry.parseCumulativeXmlBlob(arg0, arg1, arg2);
    }

    /**
     * @see com.google.gdata.data.ExtensionPoint#removeExtension(java.lang.Class)
     */
    
    public void removeExtension(Class arg0) {
        
        this.entry.removeExtension(arg0);
    }

    /**
     * @see com.google.gdata.data.ExtensionPoint#removeExtension(com.google.gdata.data.Extension)
     */
    
    public void removeExtension(Extension arg0) {
        
        this.entry.removeExtension(arg0);
    }

    /**
     * @see com.google.gdata.data.ExtensionPoint#removeRepeatingExtension(com.google.gdata.data.Extension)
     */
    
    public void removeRepeatingExtension(Extension arg0) {
        
        this.entry.removeRepeatingExtension(arg0);
    }

    /**
     * @see com.google.gdata.data.ExtensionPoint#setExtension(com.google.gdata.data.Extension)
     */
    
    public void setExtension(Extension arg0) {
        
        this.entry.setExtension(arg0);
    }

    /**
     * @see com.google.gdata.data.ExtensionPoint#setXmlBlob(com.google.gdata.util.XmlBlob)
     */
    
    public void setXmlBlob(XmlBlob arg0) {
        
        this.entry.setXmlBlob(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseEntry#declareExtensions(com.google.gdata.data.ExtensionProfile)
     */
    
    public void declareExtensions(ExtensionProfile arg0) {
        this.entry.declareExtensions(arg0);
    }
    
    
    
    
    
    

}
