/*
 *  ====================================================================
 *  The Apache Software License, Version 1.1
 *
 *  Copyright (c) 2001 The Apache Software Foundation.  All rights
 *  reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions
 *  are met:
 *
 *  1. Redistributions of source code must retain the above copyright
 *  notice, this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the following disclaimer in
 *  the documentation and/or other materials provided with the
 *  distribution.
 *
 *  3. The end-user documentation included with the redistribution,
 *  if any, must include the following acknowledgment:
 *  "This product includes software developed by the
 *  Apache Software Foundation (http://www.apache.org/)."
 *  Alternately, this acknowledgment may appear in the software itself,
 *  if and wherever such third-party acknowledgments normally appear.
 *
 *  4. The names "Apache" and "Apache Software Foundation" and
 *  "Apache Lucene" must not be used to endorse or promote products
 *  derived from this software without prior written permission. For
 *  written permission, please contact apache@apache.org.
 *
 *  5. Products derived from this software may not be called "Apache",
 *  "Apache Lucene", nor may "Apache" appear in their name, without
 *  prior written permission of the Apache Software Foundation.
 *
 *  THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 *  WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 *  OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 *  ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 *  USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 *  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 *  OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 *  SUCH DAMAGE.
 *  ====================================================================
 *
 *  This software consists of voluntary contributions made by many
 *  individuals on behalf of the Apache Software Foundation.  For more
 *  information on the Apache Software Foundation, please see
 *  <http://www.apache.org/>.
 */
package de.lanlab.larm.storage;

import de.lanlab.larm.util.WebDocument;
import de.lanlab.larm.fetcher.URLMessage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Collection;

/**
 * @author    Clemens Marschner
 * @created   1. Juni 2002
 * @version   $ver: $
 */

public class StoragePipeline implements DocumentStorage, LinkStorage
{

    boolean isOpen;
    boolean isLinkStorageOpen;
    ArrayList docStorages;
    ArrayList linkStorages;


    /**
     * Constructor for the StoragePipeline object
     */
    public StoragePipeline()
    {
        isOpen = false;
        isLinkStorageOpen = false;
        docStorages = new ArrayList();
        linkStorages = new ArrayList();
    }


    /**
     * open all docStorages
     */
    public void open()
    {
        for (Iterator it = docStorages.iterator(); it.hasNext(); )
        {
            System.out.println("opening...");
            ((DocumentStorage) it.next()).open();
        }
        isOpen = true;
    }


    /**
     * store the doc into all docStorages
     * document is discarded if a storage.store() returns null
     *
     * @see de.lanlab.larm.storage.WebDocument#store
     * @param doc  Description of the Parameter
     * @return     Description of the Return Value
     */
    public WebDocument store(WebDocument doc)
    {
        for(Iterator it = docStorages.iterator(); it.hasNext();)
        {
            doc = ((DocumentStorage)it.next()).store(doc);
            if(doc == null)
            {
                break;
            }
        }
        return doc;
    }


    /**
     * Adds a feature to the Storage attribute of the StoragePipeline object
     *
     * @param storage  The feature to be added to the Storage attribute
     */
    public void addDocStorage(DocumentStorage storage)
    {
        if (isOpen)
        {
            throw new IllegalStateException("storage can't be added if pipeline is already open");
        }
        docStorages.add(storage);
    }

    /**
     * Adds a feature to the Storage attribute of the StoragePipeline object
     *
     * @param storage  The feature to be added to the Storage attribute
     */
    public void addLinkStorage(LinkStorage storage)
    {
        if (isOpen)
        {
            throw new IllegalStateException("storage can't be added if pipeline is already open");
        }
        linkStorages.add(storage);
    }


    public void openLinkStorage()
    {
        for (Iterator it = linkStorages.iterator(); it.hasNext(); )
        {
            ((LinkStorage) it.next()).openLinkStorage();
        }
        isLinkStorageOpen = true;
    }

    public Collection storeLinks(Collection c)
    {
        for(Iterator it = linkStorages.iterator(); it.hasNext();)
        {
            c =  ((LinkStorage)it.next()).storeLinks(c);
            if(c == null)
            {
                break;
            }
        }
        return c;
    }
}

