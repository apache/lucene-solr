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
package org.apache.lucene.gdata.server; 
 
import java.io.IOException; 
import java.io.Reader; 
 
import org.apache.lucene.gdata.server.registry.DataBuilderException; 
import org.apache.lucene.gdata.server.registry.FeedInstanceConfigurator; 
import org.apache.lucene.gdata.server.registry.GDataServerRegistry; 
 
import com.google.gdata.data.BaseEntry; 
import com.google.gdata.data.BaseFeed; 
import com.google.gdata.data.ExtensionProfile; 
import com.google.gdata.util.ParseException; 
 
/** 
 * {@link com.google.gdata.data.BaseFeed}, 
 * {@link com.google.gdata.data.BaseEntry} instances have to be build from a 
 * {@link java.io.Reader} instance as they come in from a client request or out 
 * of a storage. 
 * <p> 
 * To provide a generic builder class the {@link GDataEntityBuilder} requests 
 * the type of the feed / entry and the corresponding 
 * {@link com.google.gdata.data.ExtensionProfile} form the global 
 * {@link org.apache.lucene.gdata.server.registry.GDataServerRegistry} and builds the 
 * instances from the provided reader. 
 * </p> 
 *  
 * @author Simon Willnauer 
 *  
 */ 
public class GDataEntityBuilder { 
    private static final GDataServerRegistry REGISTRY = GDataServerRegistry.getRegistry(); // TODO find another way for getting the registered feeds 
 
    /** 
     * Builds a {@link BaseFeed} instance from the {@link Reader} provided by 
     * the {@link GDataRequest} 
     *  
     * @param request - 
     *            the request to build the instance from 
     * @return - a BaseFeed instance 
     * @throws FeedNotFoundException - 
     *             if the feed is not registered 
     * @throws IOException - 
     *             if an I/O Exception occures on the provided reader 
     * @throws ParseException - 
     *             if the feed could not be parsed 
     */ 
    public static BaseFeed buildFeed(final GDataRequest request) 
            throws FeedNotFoundException, IOException, ParseException { 
        if (request == null) 
            throw new IllegalArgumentException("request must not be null"); 
        return buildFeed(request.getFeedId(), request.getReader(),request.getExtensionProfile()); 
    } 
 
    /** 
     * Builds a {@link BaseFeed} from the provided {@link Reader} 
     *  
     * @param feedId - 
     *            the feed ID to request the feed type from the registry 
     * @param reader - 
     *            the reader to build the feed from 
     * @param profile - extension profile to parse the resource 
     * @return - a BaseFeed instance 
     * @throws FeedNotFoundException - 
     *             if the feed is not registered 
     * @throws IOException - 
     *             if an I/O Exception occures on the provided reader 
     * @throws ParseException - 
     *             if the feed could not be parsed 
     */ 
    public static BaseFeed buildFeed(final String feedId, final Reader reader,final ExtensionProfile profile) 
            throws FeedNotFoundException, ParseException, IOException { 
 
        BaseFeed retVal = null; 
        try { 
            retVal = (BaseFeed) createEntityInstance(feedId); 
        } catch (FeedNotFoundException e) { 
            throw e; 
        } catch (Exception e) { 
            DataBuilderException ex = new DataBuilderException( 
                    "Could not build Feed for Feed class ", e); 
            ex.setStackTrace(e.getStackTrace()); 
            throw ex; 
        } 
        retVal.parseAtom(profile, reader); 
 
        return retVal; 
    } 
 
    /** 
     * Builds a {@link BaseEntry} instance from the {@link Reader} provided by 
     * the {@link GDataRequest} 
     *  
     * @param request - 
     *            the request to build the instance from 
     * @return - a BaseEntry instance 
     * @throws FeedNotFoundException - 
     *             if the feed, requested by the client is not registered 
     * @throws IOException - 
     *             if an I/O Exception occures on the provided reader 
     * @throws ParseException - 
     *             if the entry could not be parsed 
     */ 
    public static BaseEntry buildEntry(final GDataRequest request) 
            throws FeedNotFoundException, IOException, ParseException { 
        if (request == null) 
            throw new IllegalArgumentException("request must not be null"); 
        return buildEntry(request.getFeedId(), request.getReader(),request.getExtensionProfile()); 
    } 
 
    /** 
     * Builds a {@link BaseFeed} instance from the {@link Reader} provided by 
     * the {@link GDataRequest} 
     * @param feedId - 
     *            the feed ID to request the feed type from the registry 
     * @param reader - 
     *            the reader to build the feed from  
     * @param profile - extension profile to parse the resource 
     * @return - a BaseFeed instance 
     * @throws FeedNotFoundException - 
     *             if the feed is not registered 
     * @throws IOException - 
     *             if an I/O Exception occures on the provided reader 
     * @throws ParseException - 
     *             if the entry could not be parsed 
     */ 
    public static BaseEntry buildEntry(final String feedId, final Reader reader,final ExtensionProfile profile) 
            throws FeedNotFoundException, ParseException, IOException { 
 
        BaseEntry retVal = null; 
        try { 
            retVal = ((BaseFeed) createEntityInstance(feedId)).createEntry(); 
        } catch (FeedNotFoundException e) { 
            throw e; 
        } catch (Exception e) { 
            DataBuilderException ex = new DataBuilderException( 
                    "Could not build Entry for Entry class ", e); 
            ex.setStackTrace(e.getStackTrace()); 
            throw ex; 
        } 
        retVal.parseAtom(new ExtensionProfile(), reader); 
        return retVal; 
    } 
 
    private static Object createEntityInstance(String feedId) 
            throws FeedNotFoundException, InstantiationException, 
            IllegalAccessException { 
        FeedInstanceConfigurator config = REGISTRY.getFeedConfigurator(feedId); 
        if (config == null) 
            throw new FeedNotFoundException( 
                    "No feed for requested feed ID found - " + feedId); 
        Class feedClass = config.getFeedType(); 
        return feedClass.newInstance(); 
    } 
 
} 
