
package de.lanlab.larm.threads;

//import java.util.Vector;
import java.util.*;

/**
 *  if you have many tasks to accomplish, you can do this with one of the
 *  following strategies:
 *  <uL>
 *    <li> do it one after another (single threaded). this may often be
 *    inefficient because most programs often wait for external resources
 *    <li> assign a new thread for each task (thread on demand). This will clog
 *    up the system if many tasks have to be accomplished synchronously
 *    <li> hold a number of tasks, and queue the requests if there are more
 *    tasks than threads (ThreadPool).
 *  </ul>
 *  This thread pool is based on an article in Java-Magazin 06/2000.
 *  synchronizations were removed unless necessary
 *
 *
 */
public class ThreadPool implements ThreadingStrategy, TaskReadyListener {
    private int maxThreads = MAX_THREADS;
    /**
     *  references to all threads are stored here
     */
    private HashMap allThreads = new HashMap();
    /**
     *  this vector takes all idle threads
     */
    private Vector idleThreads = new Vector();
    /**
     *  this vector takes all threads that are in operation (busy)
     */
    private Vector busyThreads = new Vector();

    /**
     *  if there are no idleThreads, tasks will go here
     */
    private TaskQueue queue = new TaskQueue();

    /**
     *  thread pool observers will be notified of status changes
     */
    private Vector threadPoolObservers = new Vector();

    private boolean isStopped = false;

    /**
     *  default maximum number of threads, if not given by the user
     */
    public final static int MAX_THREADS = 5;

    /**
     *  thread was created
     */
    public final static String THREAD_CREATE = "T_CREATE";
    /**
     *  thread was created
     */
    public final static String THREAD_START = "T_START";
    /**
     *  thread is running
     */
    public final static String THREAD_RUNNING = "T_RUNNING";
    /**
     *  thread was stopped
     */
    public final static String THREAD_STOP = "T_STOP";
    /**
     *  thread was destroyed
     */
    public final static String THREAD_END = "T_END";
    /**
     *  thread is idle
     */
    public final static String THREAD_IDLE = "T_IDLE";

    /**
     *  a task was added to the queue, because all threads were busy
     */
    public final static String THREADQUEUE_ADD = "TQ_ADD";

    /**
     *  a task was removed from the queue, because a thread had finished and was
     *  ready
     */
    public final static String THREADQUEUE_REMOVE = "TQ_REMOVE";

    /**
     *  this factory will create the tasks
     */
    ThreadFactory factory;


    /**
     *  this constructor will create the pool with MAX_THREADS threads and the
     *  default factory
     */
    public ThreadPool() {
        this(MAX_THREADS, new ThreadFactory());
    }


    /**
     *  this constructor will create the pool with the default Factory
     *
     *@param  max  the maximum number of threads
     */
    public ThreadPool(int max) {
        this(max, new ThreadFactory());
    }


    /**
     *  constructor
     *
     *@param  max      maximum number of threads
     *@param  factory  the thread factory with which the threads will be created
     */
    public ThreadPool(int max, ThreadFactory factory) {
        maxThreads = max;
        this.factory = factory;
    }


    /**
     *  this init method will create the tasks. It must be called by hand
     */
    public void init() {
        for (int i = 0; i < maxThreads; i++) {
            createThread(i);
        }
    }


    /**
     *  Description of the Method
     *
     *@param  i  Description of the Parameter
     */
    public void createThread(int i) {
        ServerThread s = factory.createServerThread(i);
        idleThreads.add(s);
        allThreads.put(new Integer(i), s);
        s.addTaskReadyListener(this);
        sendMessage(i, THREAD_CREATE, "");
        s.start();
        sendMessage(i, THREAD_IDLE, "");
    }


    // FIXME: synchronisationstechnisch buggy
    /**
     *  Description of the Method
     *
     *@param  i  Description of the Parameter
     */
    public void restartThread(int i) {
        sendMessage(i, THREAD_STOP, "");
        ServerThread t = (ServerThread) allThreads.get(new Integer(i));
        idleThreads.remove(t);
        busyThreads.remove(t);
        allThreads.remove(new Integer(i));
        t.interruptTask();
        t.interrupt();
        //t.join();
        // deprecated, I know, but the only way to overcome SUN's bugs
        t = null;
        createThread(i);
    }


    /**
     *  Description of the Method
     *
     *@param  t    Description of the Parameter
     *@param  key  Description of the Parameter
     */
    public synchronized void doTask(InterruptableTask t, Object key) {
        if (!idleThreads.isEmpty()) {
            ServerThread s = (ServerThread) idleThreads.firstElement();
            idleThreads.remove(s);
            busyThreads.add(s);
            sendMessage(s.getThreadNumber(), THREAD_START, t.getInfo());
            s.runTask(t);
            sendMessage(s.getThreadNumber(), THREAD_RUNNING, t.getInfo());
        } else {

            queue.insert(t);
            sendMessage(-1, THREADQUEUE_ADD, t.getInfo());
        }
    }


    /**
     *  this will interrupt all threads. Therefore the InterruptableTasks must
     *  attend on the interrupted-flag
     */
    public void interrupt() {
        Iterator tasks = queue.iterator();
        while (tasks.hasNext()) {
            InterruptableTask t = (InterruptableTask) tasks.next();
            t.interrupt();
            sendMessage(-1, THREADQUEUE_REMOVE, t.getInfo());
            // In der Hoffnung, dass alles klappt...
        }
        queue.clear();
        Iterator threads = busyThreads.iterator();
        while (threads.hasNext()) {
            ((ServerThread) threads.next()).interruptTask();
        }
    }


    /**
     *  this will interrupt the tasks and end all threads
     */
    public void stop() {
        isStopped = true;
        interrupt();
        Iterator threads = idleThreads.iterator();
        while (threads.hasNext()) {
            ((ServerThread) threads.next()).interruptTask();
        }
        idleThreads.clear();
    }


    /**
     *  wird von einem ServerThread aufgerufen, wenn dieser fertig ist
     *
     *@param  s  Description of the Parameter
     *@param:    ServerThread s - der aufrufende Thread
     */
    public synchronized void taskReady(ServerThread s) {
        if (isStopped) {
            s.interrupt();
            sendMessage(s.getThreadNumber(), THREAD_STOP, s.getTask().getInfo());
            busyThreads.remove(s);
        } else if (!queue.isEmpty()) {
            InterruptableTask t = (InterruptableTask) queue.remove();
            //queue.remove(t);
            sendMessage(-1, THREADQUEUE_REMOVE, t.getInfo());
            sendMessage(s.getThreadNumber(), THREAD_START, "");
            s.runTask(t);
            sendMessage(s.getThreadNumber(), THREAD_RUNNING, s.getTask().getInfo());
        } else {
            sendMessage(s.getThreadNumber(), THREAD_IDLE, "");
            idleThreads.add(s);
            busyThreads.remove(s);
        }
        synchronized (idleThreads) {
            idleThreads.notify();
        }

    }


    /**
     *  Description of the Method
     */
    public void waitForFinish() {
        synchronized (idleThreads) {
            while (busyThreads.size() != 0) {
                //System.out.println("busyThreads: " + busyThreads.size());
                try {
                    idleThreads.wait();
                } catch (InterruptedException e) {
                    System.out.println("Interrupted: " + e.getMessage());
                }
            }
            //System.out.println("busyThreads: " + busyThreads.size());
        }
    }


    /**
     *  Adds a feature to the ThreadPoolObserver attribute of the ThreadPool
     *  object
     *
     *@param  o  The feature to be added to the ThreadPoolObserver attribute
     */
    public void addThreadPoolObserver(ThreadPoolObserver o) {
        threadPoolObservers.add(o);
    }


    /**
     *  Description of the Method
     *
     *@param  threadNr  Description of the Parameter
     *@param  action    Description of the Parameter
     *@param  info      Description of the Parameter
     */
    protected void sendMessage(int threadNr, String action, String info) {

        Iterator Ie = threadPoolObservers.iterator();
        //System.out.println("ThreadPool: Sende " + action + " message an " + threadPoolObservers.size() + " Observers");
        if (threadNr != -1) {
            while (Ie.hasNext()) {
                ((ThreadPoolObserver) Ie.next()).threadUpdate(threadNr, action, info);
            }
        } else {
            while (Ie.hasNext()) {
                ((ThreadPoolObserver) Ie.next()).queueUpdate(info, action);
            }
        }
    }


    /**
     *  Gets the queueSize attribute of the ThreadPool object
     *
     *@return    The queueSize value
     */
    public synchronized int getQueueSize() {
        return this.queue.size();
    }


    /**
     *  Gets the idleThreadsCount attribute of the ThreadPool object
     *
     *@return    The idleThreadsCount value
     */
    public synchronized int getIdleThreadsCount() {
        return this.idleThreads.size();
    }


    /**
     *  Gets the busyThreadsCount attribute of the ThreadPool object
     *
     *@return    The busyThreadsCount value
     */
    public synchronized int getBusyThreadsCount() {
        return this.busyThreads.size();
    }


    /**
     *  Gets the threadCount attribute of the ThreadPool object
     *
     *@return    The threadCount value
     */
    public synchronized int getThreadCount() {
        return this.idleThreads.size() + this.busyThreads.size();
    }


    /**
     *  Gets the threadIterator attribute of the ThreadPool object
     *
     *@return    The threadIterator value
     */
    public Iterator getThreadIterator() {
        return allThreads.values().iterator();
        // return allThreads.iterator();
    }


    /**
     *  Description of the Method
     *
     *@param  queue  Description of the Parameter
     */
    public void setQueue(TaskQueue queue) {
        this.queue = queue;
    }

    public TaskQueue getTaskQueue()
    {
        return queue;
    }

}


