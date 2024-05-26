/*
 * Copyright 2015 The Netty Project
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
package io.netty.channel;

import static io.netty.util.internal.ObjectUtil.checkPositive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.UncheckedBooleanSupplier;

/**
 * Default implementation of {@link MaxMessagesRecvByteBufAllocator} which respects {@link ChannelConfig#isAutoRead()}
 * and also prevents overflow.
 */
public abstract class DefaultMaxMessagesRecvByteBufAllocator implements MaxMessagesRecvByteBufAllocator {
    private volatile int maxMessagesPerRead;
    private volatile boolean respectMaybeMoreData = true;

    public DefaultMaxMessagesRecvByteBufAllocator() {
        this(1);
    }

    public DefaultMaxMessagesRecvByteBufAllocator(int maxMessagesPerRead) {
        maxMessagesPerRead(maxMessagesPerRead);
    }

    @Override
    public int maxMessagesPerRead() {
        return maxMessagesPerRead;
    }

    @Override
    public MaxMessagesRecvByteBufAllocator maxMessagesPerRead(int maxMessagesPerRead) {
        checkPositive(maxMessagesPerRead, "maxMessagesPerRead");
        this.maxMessagesPerRead = maxMessagesPerRead;
        return this;
    }

    /**
     * Determine if future instances of {@link #newHandle()} will stop reading if we think there is no more data.
     * @param respectMaybeMoreData
     * <ul>
     *     <li>{@code true} to stop reading if we think there is no more data. This may save a system call to read from
     *          the socket, but if data has arrived in a racy fashion we may give up our {@link #maxMessagesPerRead()}
     *          quantum and have to wait for the selector to notify us of more data.</li>
     *     <li>{@code false} to keep reading (up to {@link #maxMessagesPerRead()}) or until there is no data when we
     *          attempt to read.</li>
     * </ul>
     * @return {@code this}.
     */
    public DefaultMaxMessagesRecvByteBufAllocator respectMaybeMoreData(boolean respectMaybeMoreData) {
        this.respectMaybeMoreData = respectMaybeMoreData;
        return this;
    }

    /**
     * Get if future instances of {@link #newHandle()} will stop reading if we think there is no more data.
     * @return
     * <ul>
     *     <li>{@code true} to stop reading if we think there is no more data. This may save a system call to read from
     *          the socket, but if data has arrived in a racy fashion we may give up our {@link #maxMessagesPerRead()}
     *          quantum and have to wait for the selector to notify us of more data.</li>
     *     <li>{@code false} to keep reading (up to {@link #maxMessagesPerRead()}) or until there is no data when we
     *          attempt to read.</li>
     * </ul>
     */
    public final boolean respectMaybeMoreData() {
        return respectMaybeMoreData;
    }

    /**
     * Focuses on enforcing the maximum messages per read condition for {@link #continueReading()}.
     */
    public abstract class MaxMessageHandle implements ExtendedHandle {
        private ChannelConfig config;
        //用于控制每次read loop里最大可以循环读取的次数，默认为16次
        //可在启动配置类ServerBootstrap中通过ChannelOption.MAX_MESSAGES_PER_READ选项设置。
        private int maxMessagePerRead;
        //用于统计read loop中总共接收的连接个数，NioSocketChannel中表示读取数据的次数
        //每次read loop循环后会调用allocHandle.incMessagesRead增加记录接收到的连接个数
        private int totalMessages;
        // 本次事件轮询总共读取的字节数
        // totalBytesRead字段主要记录sub reactor线程在处理客户端NioSocketChannel中OP_READ事件活跃时，总共在read loop中读取到的网络数据，
        // 而这里是main reactor线程在接收客户端连接所以这个字段并不会被设置。totalBytesRead字段的值在本文中永远会是0
        private int totalBytesRead;
        //表示本次read loop 尝试读取多少字节，byteBuffer剩余可写的字节数
        private int attemptedBytesRead;
        //本次read loop读取到的字节数
        private int lastBytesRead;
        // respectMaybeMoreData = true表示要对可能还有更多数据进行处理的这种情况要respect认真对待,如果本次循环读取到的数据已经装满ByteBuffer，表示后面可能还有数据，那么就要进行读取。如果ByteBuffer还没装满表示已经没有数据可读了那么就退出循环
        // respectMaybeMoreData = false表示对可能还有更多数据的这种情况不认真对待 not respect。不管本次循环读取数据ByteBuffer是否满载而归，都要继续进行读取，直到读取不到数据在退出循环，属于无脑读取
        private final boolean respectMaybeMoreData = DefaultMaxMessagesRecvByteBufAllocator.this.respectMaybeMoreData;
        private final UncheckedBooleanSupplier defaultMaybeMoreSupplier = new UncheckedBooleanSupplier() {
            @Override
            public boolean get() {
                //判断本次读取byteBuffer是否满载而归
                return attemptedBytesRead == lastBytesRead;
            }
        };

        /**
         * Only {@link ChannelConfig#getMaxMessagesPerRead()} is used.
         */
        @Override
        public void reset(ChannelConfig config) {
            this.config = config;
            //默认每次最多读取16次
            maxMessagePerRead = maxMessagesPerRead();
            totalMessages = totalBytesRead = 0;
        }

        @Override
        public ByteBuf allocate(ByteBufAllocator alloc) {
            return alloc.ioBuffer(guess());
        }

        @Override
        public final void incMessagesRead(int amt) {
            totalMessages += amt;
        }

        @Override
        public void lastBytesRead(int bytes) {
            lastBytesRead = bytes;
            if (bytes > 0) {
                totalBytesRead += bytes;
            }
        }

        @Override
        public final int lastBytesRead() {
            return lastBytesRead;
        }

        @Override
        public boolean continueReading() {
            return continueReading(defaultMaybeMoreSupplier);
        }

        @Override
        public boolean continueReading(UncheckedBooleanSupplier maybeMoreDataSupplier) {
            // maybeMoreDataSupplier 用于判断经过本次read loop读取数据后，ByteBuffer是否满载而归。如果是满载而归的话（attemptedBytesRead == lastBytesRead），表明可能NioSocketChannel里还有数据。如果不是满载而归，表明NioSocketChannel里没有数据了已经
            return config.isAutoRead() &&
                   (!respectMaybeMoreData || maybeMoreDataSupplier.get()) &&
                   // 当前读取次数是否已经超过16次，如果超过，就退出do(...)while()循环。进行下一轮OP_READ事件的轮询。因为每个Sub Reactor管理了多个NioSocketChannel，不能在一个NioSocketChannel上占用太多时间，要将机会均匀地分配给Sub Reactor所管理的所有NioSocketChannel
                   totalMessages < maxMessagePerRead &&
                   // 此字段表明 OP_READ 事件读取的字节，在 OP_ACCEPT 事件下永远为0，这就导致此方法一定返回false
                   // netty本意是一次最多接收16个连接，但是因为这里导致每次接收一个连接就重新reactor轮询了，一定程度上降低了吞吐
                   // bug: https://github.com/netty/netty/issues/11708 此bug在4.1.69.Final进行了修复
                   totalBytesRead > 0;
        }

        @Override
        public void readComplete() {
        }

        @Override
        public int attemptedBytesRead() {
            return attemptedBytesRead;
        }

        @Override
        public void attemptedBytesRead(int bytes) {
            attemptedBytesRead = bytes;
        }

        protected final int totalBytesRead() {
            return totalBytesRead < 0 ? Integer.MAX_VALUE : totalBytesRead;
        }
    }
}
