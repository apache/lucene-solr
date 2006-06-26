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

import org.apache.lucene.gdata.server.registry.ProvidedService;

import com.google.gdata.data.BaseEntry;
import com.google.gdata.data.BaseFeed;
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
 * {@link org.apache.lucene.gdata.server.registry.GDataServerRegistry} and
 * builds the instances from the provided reader.
 * </p>
 * <p>
 * This build will not returne the abstract base classes.
 * </p>
 * 
 * @author Simon Willnauer
 * 
 */
public class GDataEntityBuilder {

    /**
     * Builds a {@link BaseFeed} instance from the {@link Reader} provided by
     * the {@link GDataRequest}
     * 
     * @param request -
     *            the request to build the instance from
     * @return - a BaseFeed instance
     * 
     * @throws IOException -
     *             if an I/O Exception occures on the provided reader
     * @throws ParseException -
     *             if the feed could not be parsed
     */
    public static BaseFeed buildFeed(final GDataRequest request)
            throws IOException, ParseException {
        if (request == null)
            throw new IllegalArgumentException("request must not be null");
        ProvidedService config = request.getConfigurator();
        return buildFeed(request.getReader(), config);
    }

    /**
     * Builds a {@link BaseFeed} from the provided {@link Reader}
     * 
     * 
     * @param reader -
     *            the reader to build the feed from
     * @param config -
     *            the feed instance config containing the extension profile to
     *            parse the resource
     * @return - a BaseFeed instance
     * 
     * @throws IOException -
     *             if an I/O Exception occures on the provided reader
     * @throws ParseException -
     *             if the feed could not be parsed
     */
    public static BaseFeed buildFeed(final Reader reader,
            final ProvidedService config) throws ParseException, IOException {

        BaseFeed retVal = null;
        retVal = createEntityInstance(config);
        retVal.parseAtom(config.getExtensionProfile(), reader);
      
        return retVal;
    }

    /**
     * Builds a {@link BaseEntry} instance from the {@link Reader} provided by
     * the {@link GDataRequest}
     * 
     * @param request -
     *            the request to build the instance from
     * @return - a BaseEntry instance
     * 
     * @throws IOException -
     *             if an I/O Exception occures on the provided reader
     * @throws ParseException -
     *             if the entry could not be parsed
     */
    public static BaseEntry buildEntry(final GDataRequest request)
            throws IOException, ParseException {
        if (request == null)
            throw new IllegalArgumentException("request must not be null");
        ProvidedService config = request.getConfigurator();
        return buildEntry(request.getReader(), config);
    }

    /**
     * Builds a {@link BaseFeed} instance from the {@link Reader} provided by
     * the {@link GDataRequest}
     * 
     * @param reader -
     *            the reader to build the feed from
     * @param config -
     *            the instance config containing the extension profile to parse
     *            the resource
     * @return - a BaseFeed instance
     * 
     * @throws IOException -
     *             if an I/O Exception occures on the provided reader
     * @throws ParseException -
     *             if the entry could not be parsed
     */
    public static BaseEntry buildEntry(final Reader reader,
            final ProvidedService config) throws ParseException, IOException {
       
        BaseEntry e = createEntityInstance(config).createEntry();
        e.parseAtom(config.getExtensionProfile(), reader);
        return e;
    }

    private static BaseFeed createEntityInstance(
            final ProvidedService config) {
        if(config.getFeedType() == null)
            throw new IllegalArgumentException("feedtype is null in ProvidedService");
        
        BaseFeed retVal = null;
        try {
            retVal = (BaseFeed) config.getFeedType().newInstance();
        } catch (Exception e) {
            throw new EntityBuilderException("Can't instanciate Feed for feedType "+config.getFeedType().getName(),e);
        }
        return retVal;
    }
    static class EntityBuilderException extends RuntimeException{

        /**
         * 
         */
        private static final long serialVersionUID = 7224011324202237951L;

        EntityBuilderException(String arg0) {
            super(arg0);
           
        }

        EntityBuilderException(String arg0, Throwable arg1) {
            super(arg0, arg1);
           
        }
        
    }
}
