/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.util;

import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.ObjectPool;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.netty.util.internal.MathUtil.safeFindNextPositivePowerOfTwo;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Light-weight object pool based on a thread-local stack.
 *
 * @param <T> the type of the pooled object
 */
public abstract class Recycler<T> {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(Recycler.class);

    @SuppressWarnings("rawtypes")
    //一个空的Handler,表示该对象不会被池化
    private static final Handle NOOP_HANDLE = new Handle() {
        @Override
        public void recycle(Object object) {
            // NOOP
        }
    };
    //用于产生池化对象中的回收Id,主要用来标识池化对象被哪个线程回收
    private static final AtomicInteger ID_GENERATOR = new AtomicInteger(Integer.MIN_VALUE);
    //用于标识创建池化对象的线程Id 注意这里是static final字段 也就意味着所有的创建线程OWN_THREAD_ID都是相同的
    //这里主要用来区分创建线程与非创建线程。多个非创建线程拥有各自不同的Id
    //这里的视角只是针对池化对象来说的：区分创建它的线程，与其他回收线程
    private static final int OWN_THREAD_ID = ID_GENERATOR.getAndIncrement();
    //对象池中每个线程对应的Stack中可以存储池化对象的默认初始最大个数 默认为4096个对象
    private static final int DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD = 4 * 1024; // Use 4k instances as default.
    // 对象池中线程对应的Stack可以存储池化对象默认最大个数 4096
    private static final int DEFAULT_MAX_CAPACITY_PER_THREAD;
    // 初始容量 min(DEFAULT_MAX_CAPACITY_PER_THREAD, 256) 初始容量不超过256个
    private static final int INITIAL_CAPACITY;
    // 用于计算回收线程可帮助回收的最大容量因子  默认为2
    private static final int MAX_SHARED_CAPACITY_FACTOR;
    //每个回收线程最多可以帮助多少个创建线程回收对象 默认：cpu核数 * 2
    private static final int MAX_DELAYED_QUEUES_PER_THREAD;
    //回收线程对应的WeakOrderQueue节点中的Link链表中的节点存储待回收对象的容量 默认为16
    private static final int LINK_CAPACITY;
    //创建线程回收对象时的回收比例，默认是8，表示只回收1/8的对象。也就是产生8个对象回收一个对象到对象池中
    private static final int RATIO;
    //回收线程回收对象时的回收比例，默认也是8，同样也是为了避免回收线程回收队列疯狂增长 回收比例也是1/8
    private static final int DELAYED_QUEUE_RATIO;

    static {
        // In the future, we might have different maxCapacity for different object types.
        // e.g. io.netty.recycler.maxCapacity.writeTask
        //      io.netty.recycler.maxCapacity.outboundBuffer
        int maxCapacityPerThread = SystemPropertyUtil.getInt("io.netty.recycler.maxCapacityPerThread",
                SystemPropertyUtil.getInt("io.netty.recycler.maxCapacity", DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD));
        if (maxCapacityPerThread < 0) {
            maxCapacityPerThread = DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD;
        }

        DEFAULT_MAX_CAPACITY_PER_THREAD = maxCapacityPerThread;

        MAX_SHARED_CAPACITY_FACTOR = max(2,
                SystemPropertyUtil.getInt("io.netty.recycler.maxSharedCapacityFactor",
                        2));

        MAX_DELAYED_QUEUES_PER_THREAD = max(0,
                SystemPropertyUtil.getInt("io.netty.recycler.maxDelayedQueuesPerThread",
                        // We use the same value as default EventLoop number
                        NettyRuntime.availableProcessors() * 2));

        LINK_CAPACITY = safeFindNextPositivePowerOfTwo(
                max(SystemPropertyUtil.getInt("io.netty.recycler.linkCapacity", 16), 16));

        // By default we allow one push to a Recycler for each 8th try on handles that were never recycled before.
        // This should help to slowly increase the capacity of the recycler while not be too sensitive to allocation
        // bursts.
        RATIO = max(0, SystemPropertyUtil.getInt("io.netty.recycler.ratio", 8));
        DELAYED_QUEUE_RATIO = max(0, SystemPropertyUtil.getInt("io.netty.recycler.delayedQueue.ratio", RATIO));

        INITIAL_CAPACITY = min(DEFAULT_MAX_CAPACITY_PER_THREAD, 256);

        if (logger.isDebugEnabled()) {
            if (DEFAULT_MAX_CAPACITY_PER_THREAD == 0) {
                logger.debug("-Dio.netty.recycler.maxCapacityPerThread: disabled");
                logger.debug("-Dio.netty.recycler.maxSharedCapacityFactor: disabled");
                logger.debug("-Dio.netty.recycler.linkCapacity: disabled");
                logger.debug("-Dio.netty.recycler.ratio: disabled");
                logger.debug("-Dio.netty.recycler.delayedQueue.ratio: disabled");
            } else {
                logger.debug("-Dio.netty.recycler.maxCapacityPerThread: {}", DEFAULT_MAX_CAPACITY_PER_THREAD);
                logger.debug("-Dio.netty.recycler.maxSharedCapacityFactor: {}", MAX_SHARED_CAPACITY_FACTOR);
                logger.debug("-Dio.netty.recycler.linkCapacity: {}", LINK_CAPACITY);
                logger.debug("-Dio.netty.recycler.ratio: {}", RATIO);
                logger.debug("-Dio.netty.recycler.delayedQueue.ratio: {}", DELAYED_QUEUE_RATIO);
            }
        }
    }

    //创建线程持有对象池的最大容量 默认4096
    private final int maxCapacityPerThread;
    //所有回收线程可回收对象的总量(计算因子) 默认2，也就是说所有线程可回收对象的总量=4096/2=2048
    private final int maxSharedCapacityFactor;
    //创建线程的回收比例，默认是8
    private final int interval;
    //一个回收线程可帮助多少个创建线程回收对象
    private final int maxDelayedQueuesPerThread;
    //回收线程回收比例，默认是8
    private final int delayedQueueInterval;

    private final FastThreadLocal<Stack<T>> threadLocal = new FastThreadLocal<Stack<T>>() {
        @Override
        protected Stack<T> initialValue() {
            return new Stack<T>(Recycler.this, Thread.currentThread(), maxCapacityPerThread, maxSharedCapacityFactor,
                    interval, maxDelayedQueuesPerThread, delayedQueueInterval);
        }

        @Override
        protected void onRemoval(Stack<T> value) {
            // Let us remove the WeakOrderQueue from the WeakHashMap directly if its safe to remove some overhead
            if (value.threadRef.get() == Thread.currentThread()) {
               if (DELAYED_RECYCLED.isSet()) {
                   DELAYED_RECYCLED.get().remove(value);
               }
            }
        }
    };

    protected Recycler() {
        this(DEFAULT_MAX_CAPACITY_PER_THREAD);
    }

    protected Recycler(int maxCapacityPerThread) {
        this(maxCapacityPerThread, MAX_SHARED_CAPACITY_FACTOR);
    }

    protected Recycler(int maxCapacityPerThread, int maxSharedCapacityFactor) {
        this(maxCapacityPerThread, maxSharedCapacityFactor, RATIO, MAX_DELAYED_QUEUES_PER_THREAD);
    }

    protected Recycler(int maxCapacityPerThread, int maxSharedCapacityFactor,
                       int ratio, int maxDelayedQueuesPerThread) {
        this(maxCapacityPerThread, maxSharedCapacityFactor, ratio, maxDelayedQueuesPerThread,
                DELAYED_QUEUE_RATIO);
    }

    protected Recycler(int maxCapacityPerThread, int maxSharedCapacityFactor,
                       int ratio, int maxDelayedQueuesPerThread, int delayedQueueRatio) {
        interval = max(0, ratio);
        delayedQueueInterval = max(0, delayedQueueRatio);
        if (maxCapacityPerThread <= 0) {
            this.maxCapacityPerThread = 0;
            this.maxSharedCapacityFactor = 1;
            this.maxDelayedQueuesPerThread = 0;
        } else {
            this.maxCapacityPerThread = maxCapacityPerThread;
            this.maxSharedCapacityFactor = max(1, maxSharedCapacityFactor);
            this.maxDelayedQueuesPerThread = max(0, maxDelayedQueuesPerThread);
        }
    }

    @SuppressWarnings("unchecked")
    public final T get() {
        //如果对象池容量为0，则立马新创建一个对象返回，但是该对象不会回收进对象池
        // 也就是返回的对象在调用recycle方法时不会进行任何处理，也就不会回收进对象池
        if (maxCapacityPerThread == 0) {
            return newObject((Handle<T>) NOOP_HANDLE);
        }
        //获取当前线程 保存池化对象的stack
        Stack<T> stack = threadLocal.get();
        //从stack中pop出对象，handler是池化对象在对象池中的模型，包装了一些池化对象的回收信息和回收状态
        DefaultHandle<T> handle = stack.pop();
        //如果当前线程的stack中没有池化对象 则直接创建对象
        if (handle == null) {
            //初始化的handler对象recycleId和lastRecyclerId均为0
            handle = stack.newHandle();
            //newObject为对象池recycler的抽象方法，由使用者初始化内存池的时候 匿名提供
            handle.value = newObject(handle);
        }
        return (T) handle.value;
    }

    /**
     * @deprecated use {@link Handle#recycle(Object)}.
     */
    @Deprecated
    public final boolean recycle(T o, Handle<T> handle) {
        if (handle == NOOP_HANDLE) {
            return false;
        }

        DefaultHandle<T> h = (DefaultHandle<T>) handle;
        if (h.stack.parent != this) {
            return false;
        }

        h.recycle(o);
        return true;
    }

    final int threadLocalCapacity() {
        return threadLocal.get().elements.length;
    }

    final int threadLocalSize() {
        return threadLocal.get().size;
    }

    protected abstract T newObject(Handle<T> handle);

    public interface Handle<T> extends ObjectPool.Handle<T>  { }

    private static final class DefaultHandle<T> implements Handle<T> {

        /*
          为什么池化对象的回收还要分最近回收和最终回收呢？
          因为对象池中的池化对象回收可以分为两种情况：
          1. 由创建线程直接进行回收：这种回收情况就是一步到位，直接回收至创建线程对应的Stack中。所以这种情况下是不分阶段的。recycleId = lastRecycledId = OWN_THREAD_ID。
          2. 由回收线程帮助回收：这种回收情况下就要分步进行了，首先由回收线程将池化对象暂时存储在其创建线程对应Stack中的WeakOrderQueue链表中。此时并没有完成真正的对象回收。
          recycleId = 0，lastRecycledId = 回收线程Id（WeakOrderQueue#id）。当创建线程将WeakOrderQueue链表中的待回收对象转移至Stack结构中的数组栈之后，这时池化对象才算真正完成了回收动作。
          recycleId = lastRecycledId = 回收线程Id（WeakOrderQueue#id）。
         */

        /**
         * recycleId 与 lastRecycledId 之间的关系分为以下几种情况：<br/>
         * <ol>
         *     <li>
         *         recycleId = lastRecycledId = 0：表示池化对象刚刚被创建或者刚刚从对象池中取出即将被再次复用。这是池化对象的初始状态
         *     </li>
         *     <li>
         *          recycleId = lastRecycledId != 0：表示当前池化对象已经被回收至对应Stack结构里的数组栈中。可以直接被取出复用。可能是被其创建线程直接回收，也可能是被回收线程回收。
         *     </li>
         *     <li>
         *         recycleId != lastRecycledId：表示当前池化对象处于半回收状态。池化对象已经被业务线程处理完毕，并被回收线程回收至对应的WeakOrderQueue节点中。并等待创建线程将其最终转移至Stack结构中的数组栈中
         *     </li>
         * </ol>
         */

        //用于标识最近被哪个线程回收，被回收之前均是0
        int lastRecycledId;
        //用于标识最终被哪个线程回收，在没被回收前是0
        int recycleId;
        //是否已经被回收
        boolean hasBeenRecycled;
        //强引用关联创建handler的stack
        Stack<?> stack;
        //池化对象
        Object value;

        DefaultHandle(Stack<?> stack) {
            this.stack = stack;
        }

        @Override
        public void recycle(Object object) {
            if (object != value) {
                throw new IllegalArgumentException("object does not belong to handle");
            }

            Stack<?> stack = this.stack;
            //handler初次创建以及从对象池中获取到时  recycleId = lastRecycledId = 0（对象被回收之前）
            //创建线程回收对象后recycleId = lastRecycledId = OWN_THREAD_ID
            //回收线程回收对象后lastRecycledId = 回收线程Id,当对象被转移到stack中后 recycleId = lastRecycledId = 回收线程Id
            /*
                stack == null ：这种情况其实前边我们也有提到过，就是当池化对象对应的创建线程挂掉的时候，对应的Stack随后也被GC回收掉。那么这时就不需要在回收该池化对象了。
             */
            if (lastRecycledId != recycleId || stack == null) {
                throw new IllegalStateException("recycled already");
            }

            stack.push(this);
        }
    }

    //实现跨线程回收的核心，这里保存的是当前线程为其他线程回收的对象（由其他线程创建的池化对象）
    //key: 池化对象对应的创建线程stack  value: 当前线程代替该创建线程回收的池化对象 存放在weakOrderQueue中
    //这里的value即是 创建线程对应stack中的weakOrderQueue链表中的节点（每个节点表示其他线程为当前创建线程回收的对象）

    /*
        这里为什么要用WeakHashMap呢?
        其实我们前边多少也提到过了，考虑到一种极端的情况就是当创建线程挂掉并且被GC回收之后，其实这个创建线程对应的Stack结构已经没有用了，
        存储在Stack结构中的池化对象永远不会再被使用到，此时回收线程完全就没有必要在为挂掉的创建线程回收对象了。而这个Stack结构如果没有任何引用链存在的话，随后也会被GC回收。
        那么这个Stack结构在WeakHashMap中对应的Entry也会被自动删除。如果这里不采用WeakHashMap，那么回收线程为该Stack回收的对象就会一直停留在回收线程中。
     */

    private static final FastThreadLocal<Map<Stack<?>, WeakOrderQueue>> DELAYED_RECYCLED =
            new FastThreadLocal<Map<Stack<?>, WeakOrderQueue>>() {
        @Override
        protected Map<Stack<?>, WeakOrderQueue> initialValue() {
            return new WeakHashMap<Stack<?>, WeakOrderQueue>();
        }
    };

    // a queue that makes only moderate guarantees about visibility: items are seen in the correct order,
    // but we aren't absolutely guaranteed to ever see anything at all, thereby keeping the queue cheap to maintain
    private static final class WeakOrderQueue extends WeakReference<Thread> {

        static final WeakOrderQueue DUMMY = new WeakOrderQueue();

        // Let Link extend AtomicInteger for intrinsics. The Link itself will be used as writerIndex.
        // link结构是用于真正存储待回收对象的结构，继承AtomicInteger 本身可以用来当做writeindex使用
        /*
            writeIndex:
            Link结构继承于AtomicInteger类型，这就意味着Link结构本身就可以被当做一个writeIndex来使用，由于回收线程在向Link节点添加回收对象的时候需要修改writeIndex，
            于此同时创建线程在转移Link节点的时候需要读取writeIndex，所以writeIndex需要保证线程安全性，故采用AtomicInteger类型存储。
         */
        @SuppressWarnings("serial")
        static final class Link extends AtomicInteger {
            //数组用来存储待回收对象，容量为16
            final DefaultHandle<?>[] elements = new DefaultHandle[LINK_CAPACITY];
            /*
                创建线程在转移Link节点中的待回收对象时，通过这个readIndex来读取未被转移的对象。
                由于readIndex只会被创建线程使用，所以这里并不需要保证原子性和可见性。用一个普通的int变量存储就好
             */
            int readIndex;
            //weakOrderQueue中的存储结构时由link结构节点元素组成的链表结构
            // Link节点的next指针，用于指向链表中的下一个节点。
            Link next;

        }

        // Its important this does not hold any reference to either Stack or WeakOrderQueue.
        // weakOrderQueue内部link链表的头结点
        private static final class Head {
            //所有回收线程能够帮助创建线程回收对象的总容量 reserveSpaceForLink方法中会多线程操作该字段
            //用于指示当前回收线程是否继续为创建线程回收对象，所有回收线程都可以看到，这个值是所有回收线程共享的。
            // 以便可以保证所有回收线程回收的对象总量不能超过availableSharedCapacity
            private final AtomicInteger availableSharedCapacity;
            //link链表的头结点
            Link link;

            Head(AtomicInteger availableSharedCapacity) {
                this.availableSharedCapacity = availableSharedCapacity;
            }

            /**
             * Reclaim all used space and also unlink the nodes to prevent GC nepotism.
             */
            // 回收head节点的所有空间，并从链表中删除head节点，head指针指向下一节点
            void reclaimAllSpaceAndUnlink() {
                Link head = link;
                link = null;
                int reclaimSpace = 0;
                while (head != null) {
                    reclaimSpace += LINK_CAPACITY;
                    Link next = head.next;
                    // Unlink to help GC and guard against GC nepotism.
                    head.next = null;
                    head = next;
                }
                if (reclaimSpace > 0) {
                    reclaimSpace(reclaimSpace);
                }
            }

            private void reclaimSpace(int space) {
                //所有回收线程都可以看到，这个值是所有回收线程共享的。以便可以保证所有回收线程回收的对象总量不能超过availableSharedCapacity
                availableSharedCapacity.addAndGet(space);
            }

            //参数link为新的head节点，当前head指针指向的节点已经被回收完毕
            void relink(Link link) {
                //更新availableSharedCapacity，因为当前link节点中的待回收对象已经被转移完毕，所以需要增加availableSharedCapacity的值
                reclaimSpace(LINK_CAPACITY);
                //head指针指向新的头结点（第一个未被回收完毕的link节点）
                this.link = link;
            }

            /**
             * Creates a new {@link} and returns it if we can reserve enough space for it, otherwise it
             * returns {@code null}.
             */
            Link newLink() {
                return reserveSpaceForLink(availableSharedCapacity) ? new Link() : null;
            }

            //此处目的是为接下来要创建的link预留空间容量
            static boolean reserveSpaceForLink(AtomicInteger availableSharedCapacity) {
                for (;;) {
                    //获取stack中允许异线程回收对象的总容量（异线程还能为该stack收集多少对象）
                    int available = availableSharedCapacity.get();
                    //当availbale可供回收容量小于一个Link时，说明异线程回收对象已经达到上限，不能在为stack回收对象了
                    if (available < LINK_CAPACITY) {
                        return false;
                    }
                    //为Link预留到一个Link的空间容量，更新availableSharedCapacity
                    if (availableSharedCapacity.compareAndSet(available, available - LINK_CAPACITY)) {
                        return true;
                    }
                }
            }
        }

        // chain of data items
        //link链表的头结点，head指针始终指向第一个未被转移完毕的LinK节点
        private final Head head;
        //尾结点
        private Link tail;
        // pointer to another queue of delayed items for the same stack
        //站在stack的视角中，stack中包含一个weakOrderQueue的链表，每个回收线程为当前stack回收的对象存放在回收线程对应的weakOrderQueue中
        //这样通过stack中的这个weakOrderQueue链表，就可以找到其他线程为该创建线程回收的对象
        private WeakOrderQueue next;
        //回收线程回收Id,每个weakOrderQueue分配一个，同一个stack下的一个回收线程对应一个weakOrderQueue节点
        private final int id = ID_GENERATOR.getAndIncrement();
        //回收线程回收比例 默认是8
        private final int interval;
        //回收线程回收计数 回收1/8的对象
        private int handleRecycleCount;

        private WeakOrderQueue() {
            super(null);
            head = new Head(null);
            interval = 0;
        }

        //为了使stack进行GC,这里不会持有其所属stack的引用
        private WeakOrderQueue(Stack<?> stack, Thread thread) {
            //weakOrderQueue持有对应回收线程的弱引用
            super(thread);
            //创建尾结点
            tail = new Link();

            // Its important that we not store the Stack itself in the WeakOrderQueue as the Stack also is used in
            // the WeakHashMap as key. So just store the enclosed AtomicInteger which should allow to have the
            // Stack itself GCed.
            // 创建头结点  availableSharedCapacity = maxCapacity / maxSharedCapacityFactor
            // 此时availableSharedCapacity的值已经变化了，减去了一个link的大小
            head = new Head(stack.availableSharedCapacity);
            head.link = tail;
            interval = stack.delayedQueueInterval;
            handleRecycleCount = interval; // Start at interval so the first one will be recycled.
        }

        static WeakOrderQueue newQueue(Stack<?> stack, Thread thread) {
            // We allocated a Link so reserve the space
            // link是weakOrderQueue中存储回收对象的最小结构，此处是为接下来要创建的Link预订空间容量
            // 如果stack指定的availableSharedCapacity 小于 LINK_CAPACITY大小，则分配失败
            if (!Head.reserveSpaceForLink(stack.availableSharedCapacity)) {
                return null;
            }

            //如果还够容量来分配一个link那么就创建weakOrderQueue
            final WeakOrderQueue queue = new WeakOrderQueue(stack, thread);
            // Done outside of the constructor to ensure WeakOrderQueue.this does not escape the constructor and so
            // may be accessed while its still constructed.

            // 向stack中的weakOrderQueue链表中添加当前回收线程对应的weakOrderQueue节点（始终在头结点处添加节点 ）
            // 此处向stack中添加weakOrderQueue节点的操作被移到WeakOrderQueue构造器之外的目的是防止WeakOrderQueue.this指针
            // 逃逸避免被其他线程在其构造的过程中访问
            stack.setHead(queue);

            return queue;
        }

        WeakOrderQueue getNext() {
            return next;
        }

        void setNext(WeakOrderQueue next) {
            assert next != this;
            this.next = next;
        }

        void reclaimAllSpaceAndUnlink() {
            head.reclaimAllSpaceAndUnlink();
            this.next = null;
        }

        void add(DefaultHandle<?> handle) {
            //将handler中的lastRecycledId标记为当前weakOrderQueue中的Id,一个stack和一个回收线程对应一个weakOrderQueue节点
            //表示该池化对象 最近的一次是被当前回收线程回收的。
            handle.lastRecycledId = id;

            // While we also enforce the recycling ratio when we transfer objects from the WeakOrderQueue to the Stack
            // we better should enforce it as well early. Missing to do so may let the WeakOrderQueue grow very fast
            // without control

            // 控制异线程回收频率 只回收1/8的对象
            // 这里需要关注的细节是其实在scavengeSome方法中将weakOrderQueue中的待回收对象转移到创建线程的stack中时，Netty也会做回收频率的限制
            // 这里在回收线程回收的时候也会控制回收频率（总体控制两次）netty认为越早的做回收频率控制越好 这样可以避免weakOrderQueue中的容量迅速的增长从而失去控制
            if (handleRecycleCount < interval) {
                handleRecycleCount++;
                // Drop the item to prevent recycling to aggressive.
                return;
            }
            handleRecycleCount = 0;

            //从尾部link节点开始添加新的回收对象
            Link tail = this.tail;
            int writeIndex;

            //如果当前尾部link节点容量已满，就需要创建新的link节点
            if ((writeIndex = tail.get()) == LINK_CAPACITY) {
                //创建新的Link节点
                Link link = head.newLink();
                //如果availableSharedCapacity的容量不够了，则无法创建Link。丢弃待回收对象
                if (link == null) {
                    // Drop it.
                    return;
                }
                // We allocate a Link so reserve the space
                //更新尾结点
                this.tail = tail = tail.next = link;

                writeIndex = tail.get();
            }
            //将回收对象handler放入尾部link节点中
            tail.elements[writeIndex] = handle;
            //这里将stack置为null，是为了方便stack被回收。
            //如果Stack不再使用，期望被GC回收，发现handle中还持有stack的引用，那么就无法被GC回收，从而造成内存泄漏
            //在从对象池中再次取出该对象时，stack还会被重新赋予
            handle.stack = null;
            // we lazy set to ensure that setting stack to null appears before we unnull it in the owning thread;
            // this also means we guarantee visibility of an element in the queue if we see the index updated
            //注意这里用lazySet来延迟更新writeIndex。只有当writeIndex更新之后，在创建线程中才可以看到该待回收对象
            //保证线程最终可见而不保证立即可见的原因就是 其实这里Netty还是为了性能考虑避免执行内存屏障指令的开销。
            //况且这里也并不需要考虑线程的可见性，当创建线程调用scavengeSome从weakOrderQueue链表中回收对象时，看不到当前节点weakOrderQueue
            //新添加的对象也没关系，因为是多线程一起回收，所以继续找下一个节点就好。及时全没看到，大不了就在创建一个对象。主要还是为了提高weakOrderQueue的写入性能
            tail.lazySet(writeIndex + 1);
        }

        boolean hasFinalData() {
            return tail.readIndex != tail.get();
        }

        // transfer as many items as we can from this queue to the stack, returning true if any were transferred
        @SuppressWarnings("rawtypes")
        boolean transfer(Stack<?> dst) {
            //获取当前weakOrderQueue节点中的link链表头结点
            Link head = this.head.link;
            //头结点为null说明还没有待回收对象
            if (head == null) {
                return false;
            }
            //如果头结点中的待回收对象已经被转移完毕
            if (head.readIndex == LINK_CAPACITY) {
                //判断是否有后续Link节点
                if (head.next == null) {
                    //整个link链表没有待回收对象了已经
                    return false;
                }
                head = head.next;
                //当前Head节点已经被转移完毕，head指针向后移动，head指针始终指向第一个未被转移完毕的LinK节点
                this.head.relink(head);
            }

            final int srcStart = head.readIndex;
            //writeIndex
            int srcEnd = head.get();
            //该link节点可被转移的对象容量
            final int srcSize = srcEnd - srcStart;
            if (srcSize == 0) {
                return false;
            }
            // 获取创建线程stack中的当前回收对象数量总量
            final int dstSize = dst.size;
            // 待回收对象从weakOrderQueue中转移到stack后，stack的新容量 = 转移前stack容量 + 转移的待回收对象个数
            final int expectedCapacity = dstSize + srcSize;

            if (expectedCapacity > dst.elements.length) {
                //如果转移后的stack容量超过当前stack的容量 则对stack进行扩容
                final int actualCapacity = dst.increaseCapacity(expectedCapacity);
                //每次转移最多一个Link的容量
                //actualCapacity - dstSize表示扩容后的stack还有多少剩余空间
                srcEnd = min(srcStart + actualCapacity - dstSize, srcEnd);
            }

            if (srcStart != srcEnd) {
                //待转移对象集合 也就是Link节点中存储的元素
                final DefaultHandle[] srcElems = head.elements;
                //stack中存储转移对象数组
                final DefaultHandle[] dstElems = dst.elements;
                int newDstSize = dstSize;
                for (int i = srcStart; i < srcEnd; i++) {
                    DefaultHandle<?> element = srcElems[i];
                    //recycleId == 0 表示对象还没有被真正的回收到stack中
                    if (element.recycleId == 0) {
                        //设置recycleId 表明是被哪个weakOrderQueue回收的
                        element.recycleId = element.lastRecycledId;
                    } else if (element.recycleId != element.lastRecycledId) {
                        //既被创建线程回收 同时也被回收线程回收  回收多次 则停止转移
                        throw new IllegalStateException("recycled already");
                    }
                    //对象转移后需要置空Link节点对应的位置
                    srcElems[i] = null;
                    //这里从weakOrderQueue将待回收对象真正回收到所属stack之前 需要进行回收频率控制
                    if (dst.dropHandle(element)) {
                        // Drop the object.
                        continue;
                    }
                    //重新为defaultHandler设置其所属stack(初始创建该handler的线程对应的stack)
                    //该defaultHandler在被回收线程回收的时候，会将其stack置为null，防止极端情况下，创建线程挂掉，对应stack无法被GC
                    //这里为什么在回收的时候需要设置为null呢，因为极端情况下创建线程会死掉，那么stack就需要gc，如果这时候回收线程对应的
                    //WeakOrderQueue中还有stack的强引用，那对应的stack就回收不掉了，但实际上WeakOrderQueue也会因为创建线程而gc，所以需要设置为null
                    element.stack = dst;
                    //此刻，handler才真正的被回收到所属stack中
                    dstElems[newDstSize ++] = element;
                }

                if (srcEnd == LINK_CAPACITY && head.next != null) {
                    // Add capacity back as the Link is GCed.
                    // Add capacity back as the Link is GCed.
                    // 如果当前Link已经被回收完毕，且link链表还有后续节点，则更新head指针
                    // 这里更新head指针也是为了让当前head对应的link能够被gc
                    this.head.relink(head.next);
                }
                //更新当前回收Link的readIndex
                head.readIndex = srcEnd;
                if (dst.size == newDstSize) {
                    return false;
                }
                dst.size = newDstSize;
                return true;
            } else {
                // The destination stack is full already.
                return false;
            }
        }
    }

    private static final class Stack<T> {

        // we keep a queue of per-thread queues, which is appended to once only, each time a new thread other
        // than the stack owner recycles: when we run out of items in our stack we iterate this collection
        // to scavenge those that can be reused. this permits us to incur minimal thread synchronisation whilst
        // still recycling all items.
        // 创建线程保存池化对象的stack结构所属对象池recycler实例
        final Recycler<T> parent;

        // We store the Thread in a WeakReference as otherwise we may be the only ones that still hold a strong
        // Reference to the Thread itself after it died because DefaultHandle will hold a reference to the Stack.
        //
        // The biggest issue is if we do not use a WeakReference the Thread may not be able to be collected at all if
        // the user will store a reference to the DefaultHandle somewhere and never clear this reference (or not clear
        // it in a timely manner).
        //用弱引用来关联当前stack对应的创建线程 因为用户可能在某个地方引用了defaultHandler -> stack -> thread，可能存在这个引用链
        //当创建线程死掉之后 可能因为这个引用链的存在而导致thread无法被回收掉
        final WeakReference<Thread> threadRef;
        //所有回收线程能够帮助当前创建线程回收对象的总容量。默认=4096/2=2048
        // 当前创建线程对应的所有回收线程可以帮助当前创建线程回收的对象总量。比如图中thread2 , thread3 , thread4 这三个回收线程总共可以帮助 thread1 回收对象的总量。
        // availableSharedCapacity 在多个回收线程中是共享的，回收线程每回收一个对象它的值就会减1，当小于 LINK_CAPACITY(回收线程对应WeakOrderQueue节点的最小存储单元Link)时，
        // 回收线程将不能在为该stack回收对象了。该值的计算公式为前边介绍的 max(maxCapacity / maxSharedCapacityFactor, LINK_CAPACITY)。
        // 当创建线程从Stack结构中的WeakOrderQueue链表中转移待回收对象到数组栈中后，availableSharedCapacity 的值也会相应增加。
        // 说白了这个值就是用来指示回收线程还能继续回收多少对象。已达到控制回收线程回收对象的总体容量
        final AtomicInteger availableSharedCapacity;
        //当前Stack对应的创建线程作为其他创建线程的回收线程时可以帮助多少个线程回收其池化对象
        // 一个线程对于对象池来说，它可以是创建线程，也可以是回收线程，当该创建线程作为回收线程时，该值定义了最多可以为多少个创建线程回收对象。默认值为 CPU * 2。
        private final int maxDelayedQueues;
        //当前创建线程对应的stack结构中的最大容量。 默认4096个对象
        private final int maxCapacity;
        //当前创建线程回收对象时的回收比例，默认是8。
        private final int interval;
        //当前创建线程作为其他线程的回收线程时回收其他线程的池化对象比例
        private final int delayedQueueInterval;
        // 当前Stack中的数组栈 默认初始容量256，最大容量为4096
        // 用于存放对象池中的池化对象。当线程从对象池中获取对象时就是从这里获取。
        DefaultHandle<?>[] elements;
        //数组栈 栈顶指针
        int size;
        //回收对象计数 与 interval配合 实现只回收一定比例的池化对象
        // 回收对象计数。与 interval 配合达到控制回收对象比例的目的。从 0 开始每遇到一个回收对象就 +1 ，同时把对象丢弃。
        // 直到handleRecycleCount == interval时回收对象，然后归零。也就是前边我们说到的每创建8个对象才回收1个。避免 Stack 不可控制的迅速增长。
        private int handleRecycleCount;
        //多线程回收的设计，核心还是无锁化，避免多线程回收相互竞争
        //Stack结构中的WeakOrderQueue链表
        private WeakOrderQueue cursor, prev;
        private volatile WeakOrderQueue head;

        Stack(Recycler<T> parent, Thread thread, int maxCapacity, int maxSharedCapacityFactor,
              int interval, int maxDelayedQueues, int delayedQueueInterval) {
            this.parent = parent;
            threadRef = new WeakReference<Thread>(thread);
            this.maxCapacity = maxCapacity;
            availableSharedCapacity = new AtomicInteger(max(maxCapacity / maxSharedCapacityFactor, LINK_CAPACITY));
            elements = new DefaultHandle[min(INITIAL_CAPACITY, maxCapacity)];
            this.interval = interval;
            this.delayedQueueInterval = delayedQueueInterval;
            handleRecycleCount = interval; // Start at interval so the first one will be recycled.
            this.maxDelayedQueues = maxDelayedQueues;
        }

        // Marked as synchronized to ensure this is serialized.
        //整个recycler对象池唯一的一个同步方法，而且同步块非常小，逻辑简单，执行迅速
        synchronized void setHead(WeakOrderQueue queue) {
            //始终在weakOrderQueue链表头结点插入新的节点
            queue.setNext(head);
            head = queue;
        }

        int increaseCapacity(int expectedCapacity) {
            int newCapacity = elements.length;
            int maxCapacity = this.maxCapacity;
            do {
                newCapacity <<= 1;
            } while (newCapacity < expectedCapacity && newCapacity < maxCapacity);
            //扩容后的新容量为最接近指定容量expectedCapacity的最大2的次幂
            newCapacity = min(newCapacity, maxCapacity);
            if (newCapacity != elements.length) {
                elements = Arrays.copyOf(elements, newCapacity);
            }

            return newCapacity;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        DefaultHandle<T> pop() {
            //普通出栈操作，从栈顶弹出一个回收对象
            int size = this.size;
            if (size == 0) {
                //如果当前线程所属stack已经没有对象可用，则遍历stack中的weakOrderQueue链表（其他线程帮助回收的对象存放在这里）将这些待回收对象回收进stack
                if (!scavenge()) {
                    return null;
                }
                size = this.size;
                if (size <= 0) {
                    // 如果WeakOrderQueue链表中也没有待回收对象可转移
                    // 直接返回null 新创建一个对象
                    // double check, avoid races
                    return null;
                }
            }

            // 这里表示stack中已经有对象(原来就有或者从weakOrderQueue中回收而来的对象)
            // 对这些对象进行pop

            size --;
            DefaultHandle ret = elements[size];
            elements[size] = null;
            // As we already set the element[size] to null we also need to store the updated size before we do
            // any validation. Otherwise we may see a null value when later try to pop again without a new element
            // added before.
            this.size = size;

            if (ret.lastRecycledId != ret.recycleId) {
                // 这种情况表示对象至少被一个线程回收了，要么是创建线程，要么是回收线程
                throw new IllegalStateException("recycled multiple times");
            }
            //对象初次创建以及回收对象再次使用时  它的 recycleId = lastRecycleId = 0
            //因为对象回收后 recycleId = lastRecycledId = threadId
            //这里的threadId如果是创建线程回收那么就是OWN_THREAD_ID，如果不是那么就等于回收线程的ID
            //所以这里需要重新设置为0，表示对象已经再次使用
            ret.recycleId = 0;
            ret.lastRecycledId = 0;
            return ret;
        }

        private boolean scavenge() {
            // continue an existing scavenge, if any
            //从其他线程回收的weakOrderQueue里 转移 待回收对像 到当前线程的stack中
            if (scavengeSome()) {
                return true;
            }
            // 如果weakOrderQueue中没有待回收对象可转移，那么就重置stack中的cursor.prev
            // 因为在扫描weakOrderQueue链表的过程中，cursor已经发生变化了
            // reset our scavenge cursor
            prev = null;
            cursor = head;
            return false;
        }

        private boolean scavengeSome() {
            WeakOrderQueue prev;
            //获取当前线程stack 的weakOrderQueue链表指针（本次扫描起始节点）
            WeakOrderQueue cursor = this.cursor;
            //在stack初始化完成后，cursor，prev,head等指针全部是null，这里如果cursor == null 意味着当前stack第一次开始扫描weakOrderQueue链表
            if (cursor == null) {
                prev = null;
                cursor = head;
                if (cursor == null) {
                    //说明目前weakOrderQueue链表里还没有节点，并没有其他线程帮助回收的池化对象
                    return false;
                }
            } else {
                //获取prev指针，用于操作链表（删除当前cursor节点）
                prev = this.prev;
            }

            boolean success = false;
            //循环遍历weakOrderQueue链表 转移待回收对象
            do {
                //将weakOrderQueue链表中当前节点中包含的待回收对象，转移到当前stack中，一次转移一个link
                if (cursor.transfer(this)) {
                    success = true;
                    break;
                }
                //如果当前cursor节点没有待回收对象可转移，那么就继续遍历链表获取下一个weakOrderQueue节点
                WeakOrderQueue next = cursor.getNext();
                //如果当前weakOrderQueue对应的回收线程已经挂掉了，则
                if (cursor.get() == null) {
                    // If the thread associated with the queue is gone, unlink it, after
                    // performing a volatile read to confirm there is no data left to collect.
                    // We never unlink the first queue, as we don't want to synchronize on updating the head.
                    // 判断当前weakOrderQueue节点是否还有可回收对象
                    if (cursor.hasFinalData()) {
                        for (;;) {
                            //回收weakOrderQueue中最后一点可回收对象，因为对应的回收线程已经死掉了，这个weakOrderQueue不会再有任何对象了
                            if (cursor.transfer(this)) {
                                success = true;
                            } else {
                                break;
                            }
                        }
                    }
                    //回收线程以死，对应的weakOrderQueue节点中的最后一点待回收对象也已经回收完毕，就需要将当前节点从链表中删除。unlink当前cursor节点
                    //这里需要注意的是，netty永远不会删除第一个节点，因为更新头结点是一个同步方法，避免更新头结点而导致的竞争开销
                    // prev == null 说明当前cursor节点是头结点。不用unlink，如果不是头结点 就将其从链表中删除，因为这个节点不会再有线程来收集池化对象了
                    /*
                        之所以如果cursor是头结点不处理是因为设置头结点是一个同步方法，这里为了极致的性能所以不删除头结点，又因为整个Stack对于WeakOrderQueue
                        是头插法，所以下个回收线程进来后，此时的头结点就不是头结点了，那时候就可以删除了
                     */
                    if (prev != null) {
                        //确保当前weakOrderQueue节点在被GC之前，我们已经回收掉它所有的占用空间
                        // Ensure we reclaim all space before dropping the WeakOrderQueue to be GC'ed.
                        cursor.reclaimAllSpaceAndUnlink();
                        //利用prev指针删除cursor节点
                        prev.setNext(next);
                    }
                } else {
                    prev = cursor;
                }
                //向后移动prev,cursor指针继续遍历weakOrderQueue链表
                cursor = next;

            } while (cursor != null && !success);

            this.prev = prev;
            this.cursor = cursor;
            return success;
        }

        void push(DefaultHandle<?> item) {
            Thread currentThread = Thread.currentThread();
            //判断当前线程是否为创建线程  对象池的回收原则是谁创建，最终由谁回收。其他线程只是将回收对象放入weakOrderQueue中
            //最终是要回收到创建线程对应的stack中的
            if (threadRef.get() == currentThread) {
                // The current Thread is the thread that belongs to the Stack, we can try to push the object now.
                // 如果当前线程正是创建对象的线程，则直接进行回收 直接放入与创建线程关联的stack中
                pushNow(item);
            } else if (threadRef.get() == null) {
                // 当前引用的线程为Null，说明创建线程已经挂了，那么是不需要进行对象回收的
                // 这里设置将stack设置为null是避免stack无法被gc，因为创建线程已经挂了，对应的stack也应该被gc掉，如果这里还维护着强引用，那么就会造成内存泄漏
                item.stack = null;
            } else {
                // The current Thread is not the one that belongs to the Stack
                // (or the Thread that belonged to the Stack was collected already), we need to signal that the push
                // happens later.
                // 当前线程不是创建线程，则将回收对象放入创建线程对应的stack中的weakOrderQueue链表相应节点中（currentThread对应的节点）
                pushLater(item, currentThread);
            }
        }

        private void pushNow(DefaultHandle<?> item) {
            //池化对象被回收前 recycleId = lastRecycleId = 0
            //如果其中之一不为0 说明已经被回收了
            if ((item.recycleId | item.lastRecycledId) != 0) {
                throw new IllegalStateException("recycled already");
            }
            //此处是由创建线程回收，则将池化对象的recycleId与lastRecycleId设置为创建线程Id-OWN_THREAD_ID
            //注意这里的OWN_THREAD_ID是一个固定的值，是因为这里的视角是池化对象的视角，只需要区分创建线程和非创建线程即可。
            //对于一个池化对象来说创建线程只有一个 所以用一个固定的OWN_THREAD_ID来表示创建线程Id
            item.recycleId = item.lastRecycledId = OWN_THREAD_ID;

            int size = this.size;
            //如果当前池化对象的容量已经超过最大容量 则丢弃对象
            //为了避免池化对象的急速膨胀，这里只会回收1/8的对象，剩下的对象都需要丢弃
            if (size >= maxCapacity || dropHandle(item)) {
                // Hit the maximum capacity or should drop - drop the possibly youngest object.
                return;
            }
            //当前线程对应的stack容量已满但是还没超过最大容量限制，则对stack进行扩容
            if (size == elements.length) {
                //容量扩大两倍
                elements = Arrays.copyOf(elements, min(size << 1, maxCapacity));
            }
            //将对象回收至当前stack中
            elements[size] = item;
            //更新当前stack的栈顶指针
            this.size = size + 1;
        }

        private void pushLater(DefaultHandle<?> item, Thread thread) {
            //maxDelayQueues == 0 表示不支持对象的跨线程回收
            if (maxDelayedQueues == 0) {
                // We don't support recycling across threads and should just drop the item on the floor.
                //直接丢弃
                return;
            }

            // we don't want to have a ref to the queue as the value in our weak map
            // so we null it out; to ensure there are no races with restoring it later
            // we impose a memory ordering here (no-op on x86)
            //注意这里的视角切换，当前线程为回收线程
            Map<Stack<?>, WeakOrderQueue> delayedRecycled = DELAYED_RECYCLED.get();
            //获取当前回收对象属于的stack 由当前线程帮助其回收  注意这里是跨线程回收 当前线程并不是创建线程
            WeakOrderQueue queue = delayedRecycled.get(this);
            if (queue == null) {
                //maxDelayedQueues指示一个线程最多可以帮助多少个线程回收其创建的对象
                //delayedRecycled.size()表示当前线程已经帮助多少个线程回收对象
                if (delayedRecycled.size() >= maxDelayedQueues) {
                    // Add a dummy queue so we know we should drop the object
                    //如果超过指定帮助线程个数，则停止为其创建WeakOrderQueue，停止为其回收对象
                    //WeakOrderQueue.DUMMY这里是一个标识，后边遇到这个标识  就不会为其回收对象了
                    delayedRecycled.put(this, WeakOrderQueue.DUMMY);
                    return;
                }
                // Check if we already reached the maximum number of delayed queues and if we can allocate at all.
                // 创建为回收线程对应的WeakOrderQueue节点以便保存当前线程为其回收的对象
                if ((queue = newWeakOrderQueue(thread)) == null) {
                    // drop object
                    // 创建失败则丢弃对象
                    return;
                }
                delayedRecycled.put(this, queue);
            } else if (queue == WeakOrderQueue.DUMMY) {
                // drop object
                // 如果queue的值是WeakOrderQueue.DUMMY 表示当前已经超过了允许帮助的线程数 直接丢弃对象
                return;
            }
            //当前线程为对象的创建线程回收对象  放入对应的weakOrderQueue中
            queue.add(item);
        }

        /**
         * Allocate a new {@link WeakOrderQueue} or return {@code null} if not possible.
         */
        private WeakOrderQueue newWeakOrderQueue(Thread thread) {
            return WeakOrderQueue.newQueue(this, thread);
        }

        boolean dropHandle(DefaultHandle<?> handle) {
            if (!handle.hasBeenRecycled) {
                //回收计数handleRecycleCount 初始值为8 这样可以保证创建的第一个对象可以被池化回收
                //interval控制回收频率 8个对象回收一个
                if (handleRecycleCount < interval) {
                    handleRecycleCount++;
                    // Drop the object.
                    return true;
                }
                //回收一个对象后，回收计数清零
                handleRecycleCount = 0;
                //设置defaultHandler的回收标识为true
                handle.hasBeenRecycled = true;
            }
            return false;
        }

        DefaultHandle<T> newHandle() {
            return new DefaultHandle<T>(this);
        }
    }
}
