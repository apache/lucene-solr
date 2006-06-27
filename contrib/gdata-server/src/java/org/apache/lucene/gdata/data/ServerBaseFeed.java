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
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.lucene.gdata.server.registry.ProvidedService;

import com.google.gdata.client.Service;
import com.google.gdata.data.BaseEntry;
import com.google.gdata.data.BaseFeed;
import com.google.gdata.data.Category;
import com.google.gdata.data.DateTime;
import com.google.gdata.data.Extension;
import com.google.gdata.data.ExtensionProfile;
import com.google.gdata.data.Feed;
import com.google.gdata.data.Generator;
import com.google.gdata.data.Link;
import com.google.gdata.data.Person;
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
 * {@link org.apache.lucene.gdata.server.registry.ProvidedService} of the feed.
 * All these information are
 * encapsulated in the ServerBaseFeed decoration a concrete subl class of <tt>BaseFeed</tt>. The type of the 
 * {@link com.google.gdata.data.BaseEntry} contained it this feed will be passed to the ServerBaseFeed
 * at creation time via the constructor. To retrieve the original entry call
 * {@link ServerBaseFeed#getFeed()} returns a
 * {@link com.google.gdata.data.BaseFeed} instance which can be casted into the
 * actual type. To use the ServerBaseEntry for generation a provided format like
 * RSS/ATOM the corresponding {@link com.google.gdata.data.ExtensionProfile} has
 * to be provided to the generation method.
 * 
 * @author Simon Willnauer
 * 
 */
public class ServerBaseFeed  {

    private String serviceType;

    private ProvidedService serviceConfig;
    
    private GDataAccount account;
    
    private BaseFeed feed;
    /**
     * @return Returns the account.
     */
    public GDataAccount getAccount() {
        return this.account;
    }

    /**
     * @param account The account to set.
     */
    public void setAccount(GDataAccount account) {
        this.account = account;
    }

    /**
     * Creates a new ServerBaseFeed and decorates a basic instance of {@link Feed}
     */
    @SuppressWarnings("unchecked")
    public ServerBaseFeed() {
        this.feed = new Feed();
        
    }
    /**
     * @param feed - the feed to decorate
     *            
     */
    @SuppressWarnings("unchecked")
    public ServerBaseFeed(BaseFeed feed) {
        this.feed = feed;
        
    }

    /**
     * @return Returns the feed.
     */
    public BaseFeed getFeed() {
        return this.feed;
    }

    /**
     * @param feed The feed to set.
     */
    public void setFeed(BaseFeed feed) {
        this.feed = feed;
    }

    /**
     * @see com.google.gdata.data.BaseFeed#declareExtensions(com.google.gdata.data.ExtensionProfile)
     */
    public void declareExtensions(ExtensionProfile extProfile) {

      this.feed.declareExtensions(extProfile);
    }

    /**
     * @param link -
     *            a link added to the link list of the feed
     */
    @SuppressWarnings("unchecked")
    public void addLink(final Link link) {
        this.feed.getLinks().add(link);
    }

    /**
     * @param collection -
     *            a collection of <code>Link</code> instance to be added to
     *            the feeds link list
     */
    public void addLinks(final Collection<Link> collection) {
        this.feed.getLinks().addAll(collection);
    }

    /**
     * @return - the name of the service related of the feed represented by this
     *         ServerBaseFeed
     */
    public String getServiceType() {
        return this.serviceType;
    }

    /**
     * @param serviceType -
     *            the name of the service related of the feed represented by
     *            this ServerBaseFeed
     */
    public void setServiceType(String serviceType) {
        this.serviceType = serviceType;
    }

    /**
     * @return - the provided service
     */
    public ProvidedService getServiceConfig() {
        return this.serviceConfig;
    }

    /**
     * @param serviceConfig - -
     *            the provided service
     */
    public void setServiceConfig(ProvidedService serviceConfig) {
        this.serviceConfig = serviceConfig;
        if (serviceConfig != null)
            this.serviceType = this.serviceConfig.getName();

    }

    /**
     * @param person -
     *            adds an author to the feed
     */
    public void addAuthor(final Person person) {
        this.feed.getAuthors().add(person);
    }
    
    /**
     * @see com.google.gdata.data.BaseFeed#createEntry()
     */
    
    public BaseEntry createEntry() {
        
        return this.feed.createEntry();
    }

    /**
     * @see com.google.gdata.data.BaseFeed#generateAtom(com.google.gdata.util.common.xml.XmlWriter, com.google.gdata.data.ExtensionProfile)
     */
    
    public void generateAtom(XmlWriter arg0, ExtensionProfile arg1) throws IOException {
        
        this.feed.generateAtom(arg0, arg1);
    }

    /**
     * @see com.google.gdata.data.BaseFeed#generateAtomColl(com.google.gdata.util.common.xml.XmlWriter)
     */
    
    public void generateAtomColl(XmlWriter arg0) throws IOException {
        
        this.feed.generateAtomColl(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseFeed#generateRss(com.google.gdata.util.common.xml.XmlWriter, com.google.gdata.data.ExtensionProfile)
     */
    
    public void generateRss(XmlWriter arg0, ExtensionProfile arg1) throws IOException {
        
        this.feed.generateRss(arg0, arg1);
    }

    /**
     * @see com.google.gdata.data.BaseFeed#getCanPost()
     */
    
    public boolean getCanPost() {
        
        return this.feed.getCanPost();
    }

    /**
     * @see com.google.gdata.data.BaseFeed#getEntries()
     */
    
    public List getEntries() {
        
        return this.feed.getEntries();
    }

    /**
     * @see com.google.gdata.data.BaseFeed#getEntryPostLink()
     */
    
    public Link getEntryPostLink() {
        
        return this.feed.getEntryPostLink();
    }

    /**
     * @see com.google.gdata.data.BaseFeed#getItemsPerPage()
     */
    
    public int getItemsPerPage() {
        
        return this.feed.getItemsPerPage();
    }

    /**
     * @see com.google.gdata.data.BaseFeed#getSelf()
     */
    
    public BaseFeed getSelf() throws IOException, ServiceException {
        
        return this.feed.getSelf();
    }

    /**
     * @see com.google.gdata.data.BaseFeed#getSelfLink()
     */
    
    public Link getSelfLink() {
        
        return this.feed.getSelfLink();
    }

    /**
     * @see com.google.gdata.data.BaseFeed#getService()
     */
    
    public Service getService() {
        
        return this.feed.getService();
    }

    /**
     * @see com.google.gdata.data.BaseFeed#getStartIndex()
     */
    
    public int getStartIndex() {
        
        return this.feed.getStartIndex();
    }

    /**
     * @see com.google.gdata.data.BaseFeed#getTotalResults()
     */
    
    public int getTotalResults() {
        
        return this.feed.getTotalResults();
    }

    /**
     * @see com.google.gdata.data.BaseFeed#insert(E)
     */
    
    public BaseEntry insert(BaseEntry arg0) throws ServiceException, IOException {
        
        return this.feed.insert(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseFeed#parseAtom(com.google.gdata.data.ExtensionProfile, java.io.InputStream)
     */
    
    public void parseAtom(ExtensionProfile arg0, InputStream arg1) throws IOException, ParseException {
        
        this.feed.parseAtom(arg0, arg1);
    }

    /**
     * @see com.google.gdata.data.BaseFeed#parseAtom(com.google.gdata.data.ExtensionProfile, java.io.Reader)
     */
    
    public void parseAtom(ExtensionProfile arg0, Reader arg1) throws IOException, ParseException {
        
        this.feed.parseAtom(arg0, arg1);
    }

    /**
     * @see com.google.gdata.data.BaseFeed#setCanPost(boolean)
     */
    
    public void setCanPost(boolean arg0) {
        
        this.feed.setCanPost(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseFeed#setItemsPerPage(int)
     */
    
    public void setItemsPerPage(int arg0) {
        
        this.feed.setItemsPerPage(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseFeed#setService(com.google.gdata.client.Service)
     */
    
    public void setService(Service arg0) {
        
        this.feed.setService(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseFeed#setStartIndex(int)
     */
    
    public void setStartIndex(int arg0) {
        
        this.feed.setStartIndex(arg0);
    }

    /**
     * @see com.google.gdata.data.BaseFeed#setTotalResults(int)
     */
    
    public void setTotalResults(int arg0) {
        
        this.feed.setTotalResults(arg0);
    }

    /**
     * @see com.google.gdata.data.Source#addHtmlLink(java.lang.String, java.lang.String, java.lang.String)
     */
    
    public void addHtmlLink(String arg0, String arg1, String arg2) {
        
        this.feed.addHtmlLink(arg0, arg1, arg2);
    }

    /**
     * @see com.google.gdata.data.Source#getAuthors()
     */
    
    public List<Person> getAuthors() {
        
        return this.feed.getAuthors();
    }

    /**
     * @see com.google.gdata.data.Source#getCategories()
     */
    
    public Set<Category> getCategories() {
        
        return this.feed.getCategories();
    }

    /**
     * @see com.google.gdata.data.Source#getContributors()
     */
    
    public List<Person> getContributors() {
        
        return this.feed.getContributors();
    }

    /**
     * @see com.google.gdata.data.Source#getGenerator()
     */
    
    public Generator getGenerator() {
        
        return this.feed.getGenerator();
    }

    /**
     * @see com.google.gdata.data.Source#getHtmlLink()
     */
    
    public Link getHtmlLink() {
        
        return this.feed.getHtmlLink();
    }

    /**
     * @see com.google.gdata.data.Source#getIcon()
     */
    
    public String getIcon() {
        
        return this.feed.getIcon();
    }

    /**
     * @see com.google.gdata.data.Source#getId()
     */
    
    public String getId() {
        
        return this.feed.getId();
    }

    /**
     * @see com.google.gdata.data.Source#getLink(java.lang.String, java.lang.String)
     */
    
    public Link getLink(String arg0, String arg1) {
        
        return this.feed.getLink(arg0, arg1);
    }

    /**
     * @see com.google.gdata.data.Source#getLinks()
     */
    
    public List<Link> getLinks() {
        
        return this.feed.getLinks();
    }

    /**
     * @see com.google.gdata.data.Source#getLogo()
     */
    
    public String getLogo() {
        
        return this.feed.getLogo();
    }

    /**
     * @see com.google.gdata.data.Source#getRights()
     */
    
    public TextConstruct getRights() {
        
        return this.feed.getRights();
    }

    /**
     * @see com.google.gdata.data.Source#getSubtitle()
     */
    
    public TextConstruct getSubtitle() {
        
        return this.feed.getSubtitle();
    }

    /**
     * @see com.google.gdata.data.Source#getTitle()
     */
    
    public TextConstruct getTitle() {
        
        return this.feed.getTitle();
    }

    /**
     * @see com.google.gdata.data.Source#getUpdated()
     */
    
    public DateTime getUpdated() {
        
        return this.feed.getUpdated();
    }

    /**
     * @see com.google.gdata.data.Source#setGenerator(com.google.gdata.data.Generator)
     */
    
    public void setGenerator(Generator arg0) {
        
        this.feed.setGenerator(arg0);
    }

    /**
     * @see com.google.gdata.data.Source#setIcon(java.lang.String)
     */
    
    public void setIcon(String arg0) {
        
        this.feed.setIcon(arg0);
    }

    /**
     * @see com.google.gdata.data.Source#setId(java.lang.String)
     */
    
    public void setId(String arg0) {
        
        this.feed.setId(arg0);
    }

    /**
     * @see com.google.gdata.data.Source#setLogo(java.lang.String)
     */
    
    public void setLogo(String arg0) {
        
        this.feed.setLogo(arg0);
    }

    /**
     * @see com.google.gdata.data.Source#setRights(com.google.gdata.data.TextConstruct)
     */
    
    public void setRights(TextConstruct arg0) {
        
        this.feed.setRights(arg0);
    }

    /**
     * @see com.google.gdata.data.Source#setSubtitle(com.google.gdata.data.TextConstruct)
     */
    
    public void setSubtitle(TextConstruct arg0) {
        
        this.feed.setSubtitle(arg0);
    }

    /**
     * @see com.google.gdata.data.Source#setTitle(com.google.gdata.data.TextConstruct)
     */
    
    public void setTitle(TextConstruct arg0) {
        
        this.feed.setTitle(arg0);
    }

    /**
     * @see com.google.gdata.data.Source#setUpdated(com.google.gdata.data.DateTime)
     */
    
    public void setUpdated(DateTime arg0) {
        
        this.feed.setUpdated(arg0);
    }

    /**
     * @see com.google.gdata.data.ExtensionPoint#addExtension(com.google.gdata.data.Extension)
     */
    
    public void addExtension(Extension arg0) {
        
        this.feed.addExtension(arg0);
    }

    /**
     * @see com.google.gdata.data.ExtensionPoint#addRepeatingExtension(com.google.gdata.data.Extension)
     */
    
    public void addRepeatingExtension(Extension arg0) {
        
        this.feed.addRepeatingExtension(arg0);
    }

    /**
     * @see com.google.gdata.data.ExtensionPoint#getExtension(java.lang.Class)
     */
    
    public <T extends Extension> T getExtension(Class<T> arg0) {
        
        return this.feed.getExtension(arg0);
    }

    /**
     * @see com.google.gdata.data.ExtensionPoint#getRepeatingExtension(java.lang.Class)
     */
    
    public <T extends Extension> List<T> getRepeatingExtension(Class<T> arg0) {
        
        return this.feed.getRepeatingExtension(arg0);
    }

    /**
     * @see com.google.gdata.data.ExtensionPoint#getXmlBlob()
     */
    
    public XmlBlob getXmlBlob() {
        
        return this.feed.getXmlBlob();
    }

    /**
     * @see com.google.gdata.data.ExtensionPoint#parseCumulativeXmlBlob(com.google.gdata.util.XmlBlob, com.google.gdata.data.ExtensionProfile, java.lang.Class)
     */
    
    public void parseCumulativeXmlBlob(XmlBlob arg0, ExtensionProfile arg1, Class arg2) throws IOException, ParseException {
        
        this.feed.parseCumulativeXmlBlob(arg0, arg1, arg2);
    }

    /**
     * @see com.google.gdata.data.ExtensionPoint#removeExtension(java.lang.Class)
     */
    
    public void removeExtension(Class arg0) {
        
        this.feed.removeExtension(arg0);
    }

    /**
     * @see com.google.gdata.data.ExtensionPoint#removeExtension(com.google.gdata.data.Extension)
     */
    
    public void removeExtension(Extension arg0) {
        
        this.feed.removeExtension(arg0);
    }

    /**
     * @see com.google.gdata.data.ExtensionPoint#removeRepeatingExtension(com.google.gdata.data.Extension)
     */
    
    public void removeRepeatingExtension(Extension arg0) {
        
        this.feed.removeRepeatingExtension(arg0);
    }

    /**
     * @see com.google.gdata.data.ExtensionPoint#setExtension(com.google.gdata.data.Extension)
     */
    
    public void setExtension(Extension arg0) {
        
        this.feed.setExtension(arg0);
    }

    /**
     * @see com.google.gdata.data.ExtensionPoint#setXmlBlob(com.google.gdata.util.XmlBlob)
     */
    
    public void setXmlBlob(XmlBlob arg0) {
        
        this.feed.setXmlBlob(arg0);
    }

}
