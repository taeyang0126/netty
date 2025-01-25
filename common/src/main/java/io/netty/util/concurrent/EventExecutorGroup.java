/*
 * Copyright 2012 The Netty Project
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
package io.netty.util.concurrent;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The {@link EventExecutorGroup} is responsible for providing the {@link EventExecutor}'s to use
 * via its {@link #next()} method. Besides this, it is also responsible for handling their
 * life-cycle and allows shutting them down in a global fashion.
 *
 */
public interface EventExecutorGroup extends ScheduledExecutorService, Iterable<EventExecutor> {

    /**
     * Returns {@code true} if and only if all {@link EventExecutor}s managed by this {@link EventExecutorGroup}
     * are being {@linkplain #shutdownGracefully() shut down gracefully} or was {@linkplain #isShutdown() shut down}.
     */
    boolean isShuttingDown();

    /**
     * Shortcut method for {@link #shutdownGracefully(long, long, TimeUnit)} with sensible default values.
     *
     * @return the {@link #terminationFuture()}
     */
    Future<?> shutdownGracefully();

    /**
     * Signals this executor that the caller wants the executor to be shut down.  Once this method is called,
     * {@link #isShuttingDown()} starts to return {@code true}, and the executor prepares to shut itself down.
     * Unlike {@link #shutdown()}, graceful shutdown ensures that no tasks are submitted for <i>'the quiet period'</i>
     * (usually a couple seconds) before it shuts itself down.  If a task is submitted during the quiet period,
     * it is guaranteed to be accepted and the quiet period will start over.
     *
     * @param quietPeriod the quiet period as described in the documentation
     *                    优雅关闭静默期，默认为 2s 。这个参数主要来保证 Netty 整个关闭过程中的优雅。在关闭流程开始后，
     *                    如果 Reactor 中还有遗留的异步任务需要执行，那么 Netty 就不能关闭，需要把所有异步任务执行完毕才可以。
     *                    当所有异步任务执行完毕后，Netty 为了实现更加优雅的关闭操作，一定要保障业务无损，
     *                    这时候就引入了静默期这个概念，如果在这个静默期内，用户没有新的任务向 Reactor 提交那么就开始关闭。
     *                    如果在这个静默期内，还有用户继续提交异步任务，那么就不能关闭，需要把静默期内用户提交的异步任务执行完毕才可以放心关闭
     *
     * @param timeout     the maximum amount of time to wait until the executor is {@linkplain #shutdown()}
     *                    优雅关闭超时时间，默认为 15s 。这个参数主要来保证 Netty 整个关闭过程的可控。我们知道一个生产级的优雅关闭方案既要保证优雅做到业务无损，更重要的是要保证关闭流程的可控，不能无限制的优雅下去。
     *                    导致长时间无法完成关闭动作。于是 Netty 就引入了这个参数，如果优雅关闭超时，那么无论此时有无异步任务需要执行都要开始关闭了
     *
     *                    regardless if a task was submitted during the quiet period
     * @param unit        the unit of {@code quietPeriod} and {@code timeout}
     *
     * @return the {@link #terminationFuture()}
     */
    Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit);

    /**
     * Returns the {@link Future} which is notified when all {@link EventExecutor}s managed by this
     * {@link EventExecutorGroup} have been terminated.
     */
    Future<?> terminationFuture();

    /**
     * @deprecated {@link #shutdownGracefully(long, long, TimeUnit)} or {@link #shutdownGracefully()} instead.
     */
    @Override
    @Deprecated
    void shutdown();

    /**
     * @deprecated {@link #shutdownGracefully(long, long, TimeUnit)} or {@link #shutdownGracefully()} instead.
     */
    @Override
    @Deprecated
    List<Runnable> shutdownNow();

    /**
     * Returns one of the {@link EventExecutor}s managed by this {@link EventExecutorGroup}.
     */
    EventExecutor next();

    @Override
    Iterator<EventExecutor> iterator();

    @Override
    Future<?> submit(Runnable task);

    @Override
    <T> Future<T> submit(Runnable task, T result);

    @Override
    <T> Future<T> submit(Callable<T> task);

    @Override
    ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit);

    @Override
    <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit);

    @Override
    ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit);

    @Override
    ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit);
}
