package org.apache.solr.store.blob.util;

import java.util.HashMap;
import java.util.Map;

/**
 * A kind of FIFO list that deduplicates entries based on a key associated with each entry and a merging function.
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class DeduplicatingList<K, V extends DeduplicatingList.Deduplicatable<K>> {

    /**
     * We need the "V" parameter of {@link DeduplicatingList} to be able to provide us the "K" parameter used as key.
     */
    public interface Deduplicatable<DK> {
        DK getDedupeKey();
    }

    /**
     * Given v1 and v2 (corresponding to type V in {@link DeduplicatingList}) to be stored in the list and
     * that have the same key (as per {@link Deduplicatable#getDedupeKey()}, parameter MK here), returns a merged value that
     * has the same key and that can replace the two entries in the list (based on domain specific knowledge that this is ok).<p>
     * The behaviour of this method is undefined and totally unpredictable if v1 and v2 do not have the same key. Be warned :)
     */
    public interface Merger<MK, MV extends DeduplicatingList.Deduplicatable<MK>> {
        MV merge(MV v1, MV v2);
    }

    /**
     * Building a linked list from scratch here to allow access to specific list entries (tracked in another
     * data structure) in constant time.
     * Instances must only be accessed with {@link #lock} held, as they are mutable.
     */
    static private class TaskListNode<TV> {
        TaskListNode next;
        TV elem;

        TaskListNode(TaskListNode next, TV elem) {
            this.next = next;
            this.elem = elem;
        }
    }

    /**
     * Guards access to all the rest of the data here.
     */
    private final Object lock = new Object();

    private final int almostMaxListSize;
    private final DeduplicatingList.Merger<K, V> merger;

    private TaskListNode<V> head = null;
    private TaskListNode<V> tail = null;

    private final Map<K, TaskListNode<V>> keyToListNode = new HashMap<>();

    private int size = 0;

    /**
     * Builds an empty list of a given maximum size.
     * @param almostMaxListSize The maximum number of "new" elements accepted in the list, excluding element reenqueues (of which
     *                     there are expected to be very few and related to number of processing threads in a thread pool for
     *                     example). When that number is reached, {@link #addDeduplicated} blocks until the List size
     *                     is reduced enough.
     */
    public DeduplicatingList(int almostMaxListSize, DeduplicatingList.Merger<K, V> merger) {
        this.almostMaxListSize = almostMaxListSize;
        this.merger = merger;
    }

    /**
     * Adds an entry to the tail of the list unless there's already an existing entry in the list the added entry can be merged with.
     * If that's the case, the added entry is merged instead of being added.
     * @param isReenqueue <ul><li>when <code>true</code>, the put is allowed to make the list exceed its {@link #almostMaxListSize}. In practice
     *                    we'll exceed at most by a few units.</li>
     *                    <li>when <code>false</code>, if the list is too full, the call blocks waiting until the insert can be done</li></ul>
     */
    public void addDeduplicated(final V value, boolean isReenqueue) throws InterruptedException {
        synchronized (lock) {
            TaskListNode<V> existingEntry = keyToListNode.get(value.getDedupeKey());

            if (existingEntry != null) {
                // We already have an entry for that core, merge.
                existingEntry.elem = merger.merge(existingEntry.elem, value);

                assert existingEntry.elem != null;
            } else {
                // Wait (if needed) until the insert can be done
                while (!isReenqueue && size >= almostMaxListSize) {
                    lock.wait();
                }

                addToTail(value);
            }
        }
    }

    /**
     * @return the value sitting at the head of the list or blocks waiting until there's one to return if the list is empty.
     */
    public V removeFirst() throws InterruptedException {
        synchronized (lock) {
            while (size == 0) {
                lock.wait();
            }

            return removeHead();
        }
    }

    /**
     * @return number of entries in the list
     */
    public int size() {
        synchronized (lock) {
            return size;
        }
    }

    /**
     * The key for the value being added should not already be present in the list.
     * The {@link #lock} must be held when calling this method.
     */
    private void addToTail(final V value) {
        TaskListNode<V> newNode = new TaskListNode<>(null, value);

        if (tail == null) {
            // List is empty
            assert head == null;
            assert size == 0;
            head = newNode;
            // Wake up threads blocked in getTask(), if any
            lock.notifyAll();
        } else {
            // List not empty (means nobody is blocked sleeping)
            assert head != null;
            tail.next = newNode;
        }

        TaskListNode<V> old = keyToListNode.put(value.getDedupeKey(), newNode);
        assert old == null;

        tail = newNode;
        size++;
    }

    /**
     * The {@link #lock} must be held while calling this method.
     * This method can only be called if the list is not empty.
     * @return the value at the head of the list (inserted before all other values into list).
     */
    private V removeHead() {
        assert size > 0;
        TaskListNode<V> oldHead = head;
        head = oldHead.next;
        if (head == null) {
            // list now empty, we've removed the last element
            assert size == 1;
            assert tail == oldHead;

            tail = null;
        }
        size--;
        TaskListNode<V> fromSet = keyToListNode.remove(oldHead.elem.getDedupeKey());
        assert oldHead == fromSet;

        return oldHead.elem;
    }
}
