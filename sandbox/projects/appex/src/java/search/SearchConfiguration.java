package search;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import org.apache.log4j.Category;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import search.util.DataUnformatFilter;
import search.contenthandler.FileContentHandlerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Configures the indexing process using an XML file.
 *
 * @author <a href="mailto:kelvin@relevanz.com">Kelvin Tan</a>
 */
public class SearchConfiguration
{
    public static final String TEXT_FIELD_TYPE = "text";
    public static final String KEYWORD_FIELD_TYPE = "keyword";
    public static final String UNINDEXED_FIELD_TYPE = "unindexed";
    public static final String UNSTORED_FIELD_TYPE = "unstored";

    /** Log4j category.
     */
    static Category cat = Category.getInstance(SearchConfiguration.class.getName());

    /**
     * Key in the config file to declare content handlers.
     */
    private static final String CONTENT_HANDLER_KEY = "Search.ContentHandlers";

    /**
     * Key in the config file to declare custom fields.
     */
    private static final String FIELD_KEY = "Search.Fields";

    /**
     * Map of content handlers.
     */
    private Map contentHandlers = new HashMap();

    /**
     * Map of (non-standard) custom fields to index.
     */
    private Map customFields = new HashMap();

    /**
     * Document object which represents the xml configuration file.
     */
    private Document doc;

    /**
     * Creates a new SearchConfiguration.
     *
     * @param configFile Name of the xml configuration file.
     */
    public SearchConfiguration(String configFile) throws IllegalConfigurationException
    {
        try
        {
            SAXBuilder builder = new SAXBuilder();
            DataUnformatFilter format = new DataUnformatFilter();
            builder.setXMLFilter(format);
            doc = builder.build(configFile);
        }
        catch (Exception e)
        {
            cat.error("Error creating XML parser:" + e.getMessage(), e);
        }
        loadContentHandlers();
        loadCustomFields();
    }

    public Map getContentHandlers()
    {
        return this.contentHandlers;
    }

    public Map getCustomFields()
    {
        return this.customFields;
    }

    /**
     * Loads the content handlers.
     */
    protected void loadContentHandlers() throws IllegalConfigurationException
    {
        String[] extensions = getChildPropertyAttributeValues(CONTENT_HANDLER_KEY, "extension");
        String[] handlers = getChildPropertyAttributeValues(CONTENT_HANDLER_KEY, "handler");
        if (extensions.length != handlers.length)
            throw new IllegalConfigurationException(
                    "Illegal configuration of Search Content Handlers!");
        for (int i = 0; i < extensions.length; i++)
        {
            contentHandlers.put(extensions[i], generateObject(handlers[i]));
        }
        String[] defaultExtension = getChildPropertyAttributeValues(CONTENT_HANDLER_KEY, "default");
        for (int i = 0; i < defaultExtension.length; i++)
        {
            if (defaultExtension[i] != null && defaultExtension[i].equals("true"))
            {
                contentHandlers.put(FileContentHandlerFactory.DEFAULT_HANDLER_KEY
                                    , generateObject(handlers[i]));
            }
        }
    }

    /**
     * Loads the custom fields to index.
     */
    protected void loadCustomFields() throws IllegalConfigurationException
    {
        String[] fields = getChildPropertyAttributeValues(FIELD_KEY, "name");
        String[] fieldtypes = getChildPropertyAttributeValues(FIELD_KEY, "type");
        if (fields.length != fieldtypes.length)
            throw new IllegalConfigurationException(
                    "Illegal configuration of custom search fields!");
        for (int i = 0; i < fields.length; i++)
        {
            customFields.put(fields[i], fieldtypes[i]);
        }
    }

    /**
     * Return attribute values for all child nodes.
     */
    private String[] getChildPropertyAttributeValues(String parent,
                                                     String attributeName)
    {
        String[] nodeName = parseNodeName(parent);
        Element element = doc.getRootElement();
        for (int i = 0; i < nodeName.length; i++)
        {
            element = element.getChild(nodeName[i]);
            if (element == null)
            {
                return new String[]{};
            }
        }
        List children = element.getChildren();
        int childCount = children.size();
        String[] childrenAttributeValue = new String[childCount];
        for (int i = 0; i < childCount; i++)
        {
            childrenAttributeValue[i] =
                    ((Element) children.get(i)).getAttributeValue(attributeName);
        }
        return childrenAttributeValue;
    }

    /**
     * Node names are in the form "x.y.z". Returns a String array
     * representation of the node elements.
     */
    private String[] parseNodeName(String nodeName)
    {
        StringTokenizer st = new StringTokenizer(nodeName, ".");
        String[] nodeElements = new String[st.countTokens()];
        int i = 0;
        while (st.hasMoreTokens())
        {
            nodeElements[i] = st.nextToken();
            ++i;
        }
        return nodeElements;
    }

    /**
     * Utility method to return an object based on its class name.
     * The object needs to have a constructor which accepts no parameters.
     *
     * @param className  Class name of object to be generated
     * @return Object
     */
    private static Object generateObject(String className)
    {
        Object o = null;
        try
        {
            Class c = Class.forName(className);
            o = c.newInstance();
        }
        catch (ClassNotFoundException cnfe)
        {
            cat.error(cnfe.getMessage() + " No class named '" + className + "' was found.", cnfe);
        }
        catch (InstantiationException ie)
        {
            cat.error(ie.getMessage() + " Class named '" + className + "' could not be  instantiated.", ie);
        }
        catch (IllegalAccessException iae)
        {
            cat.error(iae.getMessage() + " No access to class named '" + className + "'.", iae);
        }
        return o;
    }

}
