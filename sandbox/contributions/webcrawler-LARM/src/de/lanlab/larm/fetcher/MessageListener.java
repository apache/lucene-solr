/*
 *  LARM - LANLab Retrieval Machine
 *
 *  $history: $
 *
 *
 */
package de.lanlab.larm.fetcher;

/**
 * A Message Listener works on messages in a message queue Usually it returns
 * the message back into the queue. But it can also change the message or create
 * a new object. If it returns null, the message handler stops
 *
 * @author    Administrator
 * @created   24. November 2001
 */
public interface MessageListener
{
    /**
     * the handler
     *
     * @param message  the message to be handled
     * @return         Message  usually the original message
     *                 null: the message was consumed
     */
    public Message handleRequest(Message message);


    /**
     * will be called as soon as the Listener is added to the Message Queue
     *
     * @param handler  the Message Handler
     */
    public void notifyAddedToMessageHandler(MessageHandler handler);
}
