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

package de.lanlab.larm.fetcher;

import java.util.*;
import de.lanlab.larm.util.SimpleObservable;
import de.lanlab.larm.util.CachingQueue;
import de.lanlab.larm.util.UnderflowException;
import de.lanlab.larm.storage.LinkStorage;

/**
 *  this is a message handler that runs in its own thread.
 *  Messages can be put via <code>putMessage</code> or <code>putMessages</code>
 *  (use the latter whenever possible).<br>
 *  The messages are passed to the filters in the order in which the filters where
 *  added to the handler.<br>
 *  They can consume the message by returning null. Otherwise, they return a Message
 *  object, usually the one they got.<br>
 *  The filters will run synchronously within the message handler thread<br>
 *  This implements a chain of responsibility-style message handling
 * @version $Id$
 */
public class MessageHandler implements Runnable, LinkStorage
{

    /**
     * the queue where messages are put in.
     * Holds max. 2 x 5000 = 10.000 messages in RAM
     */
    private CachingQueue messageQueue = new CachingQueue("fetcherURLMessageQueue", 5000);

    /**
     * list of Observers
     */
    private LinkedList listeners = new LinkedList();

    /**
     * true as long as the thread is running
     */
    private boolean running = true;

    /**
     * the message handler thread
     */
    private Thread t;

    /**
     * flag for thread communication
     */
    boolean messagesWaiting = false;

    /**
     * true when a message is processed by the filters
     */
    boolean workingOnMessage = false;

    Object queueMonitor = new Object();

    SimpleObservable messageQueueObservable = new SimpleObservable();
    SimpleObservable messageProcessorObservable = new SimpleObservable();

    public boolean isWorkingOnMessage()
    {
        return workingOnMessage;
    }

    /**
     *  messageHandler-Thread erzeugen und starten
     */
    public MessageHandler()
    {
        t = new Thread(this,"MessageHandler Thread");
        t.setPriority(5);   // higher priority to prevent starving when a lot of fetcher threads are used
        t.start();
    }

    /**
     *   join messageHandler-Thread
     */
    public void finalize()
    {
        if(t != null)
        {
            try
            {
                t.join();
                t = null;
            }
            catch(InterruptedException e) {}
        }
    }

    /**
     *   registers a filter to the message handler
     *   @param MessageListener - the Listener
     */
    public void addListener(MessageListener m)
    {
        m.notifyAddedToMessageHandler(this);
        listeners.addLast(m);
    }

    /**
     *  registers a MessageQueueObserver
     *  It will be notified whenever a message is put into the Queue  (Parameter is Int(1)) oder
     *  removed (Parameter is Int(-1))
     *  @param o  the Observer
     */
    public void addMessageQueueObserver(Observer o)
    {
        messageQueueObservable.addObserver(o);
    }

    /**
     *  adds a message processorObeserver
     *  It will be notified when a message is consumed. In this case the parameter
     *  is the filter that consumed the message
     *  @param o  the Observer
     */
    public void addMessageProcessorObserver(Observer o)
    {
        messageProcessorObservable.addObserver(o);
    }


    /**
     *  insert one message into the queue
     */
    public void putMessage(Message msg)
    {
        messageQueue.insert(msg);
        messageQueueObservable.setChanged();
        messageQueueObservable.notifyObservers(new Integer(1));
        synchronized(queueMonitor)
        {
            messagesWaiting = true;
            queueMonitor.notify();
        }
    }

    /**
     *  add a collection of events to the message queue
     */
    public void putMessages(Collection msgs)
    {
        for(Iterator i = msgs.iterator(); i.hasNext();)
        {
          Message msg = (Message)i.next();
          messageQueue.insert(msg);
        }
        messageQueueObservable.setChanged();
        messageQueueObservable.notifyObservers(new Integer(1));
        synchronized(queueMonitor)
        {
            messagesWaiting = true;
            queueMonitor.notify();
        }
    }

    public Collection storeLinks(Collection links)
    {
        putMessages(links);
        return links;
    }


    /**
     *  the main messageHandler-Thread.
     */
    public void run()
    {
        while(running)
        {
            //System.out.println("MessageHandler-Thread started");

            synchronized(queueMonitor)
            {
                // wait for new messages
                workingOnMessage=false;
                try
                {
                    queueMonitor.wait();
                }
                catch(InterruptedException e)
                {
                    System.out.println("MessageHandler: Caught InterruptedException");
                }
                workingOnMessage=true;
            }
            //messagesWaiting = false;
            Message m;
            try
            {
                while(messagesWaiting)
                {
                    synchronized(this.queueMonitor)
                    {
                        m = (Message)messageQueue.remove();
                        if(messageQueue.size() == 0)
                        {
                            messagesWaiting = false;
                        }

                    }
                    //System.out.println("MessageHandler:run: Entferne erstes Element");

                    messageQueueObservable.setChanged();
                    messageQueueObservable.notifyObservers(new Integer(-1));      // Message processed

                    // now distribute them. The handlers get the messages in the order
                    // of insertion and have the right to change them

                    Iterator i = listeners.iterator();
                    while(i.hasNext())
                    {
                        try
                        {
                            MessageListener listener = (MessageListener)i.next();
                            m = (Message)listener.handleRequest(m);
                            if (m == null)
                            {
                                // handler has consumed the message
                                messageProcessorObservable.setChanged();
                                messageProcessorObservable.notifyObservers(listener);
                                break;
                            }
                        }
                        catch(ClassCastException e)
                        {
                          System.out.println("MessageHandler:run: ClassCastException(2): " + e.getMessage());
                        }
                    }
                }
            }
            catch (ClassCastException e)
            {
                System.out.println("MessageHandler:run: ClassCastException: " + e.getMessage());
            }
            catch (UnderflowException e)
            {
                messagesWaiting = false;
                // System.out.println("MessageHandler: messagesWaiting = true although nothing queued!");
                // @FIXME: here is still a multi threading issue. I don't get it why this happens.
                //         does someone want to draw a petri net of this? ;-)
            }
            catch (Exception e)
            {
                System.out.println("MessageHandler: " + e.getClass() + " " + e.getMessage());
                e.printStackTrace();
            }

        }
    }

    public int getQueued()
    {
        return messageQueue.size();
    }

    public void openLinkStorage()
    {
    }
}
