/* -*- mode: Java; c-basic-offset: 2; indent-tabs-mode: nil; coding: utf-8-unix -*-
 *
 * Copyright © 2017-2018 microBean.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.microbean.kubernetes.controller;

import java.io.IOException;

import java.time.Duration;
import java.time.Instant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicInteger;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.function.Consumer;
import java.util.function.Function;

import java.util.logging.Level;
import java.util.logging.Logger;

import io.fabric8.kubernetes.api.model.HasMetadata;

import net.jcip.annotations.Immutable;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/**
 * A {@link ResourceTrackingEventQueueConsumer} that {@linkplain
 * ResourceTrackingEventQueueConsumer#accept(EventQueue) consumes
 * <tt>EventQueue</tt> instances} by feeding each {@link
 * AbstractEvent} in the {@link EventQueue} being consumed to {@link
 * Consumer}s of {@link AbstractEvent}s that have been {@linkplain
 * #addConsumer(Consumer) registered}.
 *
 * <p>{@link EventDistributor} instances must be {@linkplain #close()
 * closed} and discarded after use.</p>
 *
 * @param <T> a type of Kubernetes resource
 *
 * @author <a href="https://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 *
 * @see #addConsumer(Consumer)
 *
 * @see #removeConsumer(Consumer)
 *
 * @see ResourceTrackingEventQueueConsumer#accept(AbstractEvent)
 */
@Immutable
@ThreadSafe
public final class EventDistributor<T extends HasMetadata> extends ResourceTrackingEventQueueConsumer<T> implements AutoCloseable {


  /*
   * Instance fields.
   */


  @GuardedBy("readLock && writeLock")
  private final Collection<Pump<T>> pumps;

  @GuardedBy("readLock && writeLock")
  private final Collection<Pump<T>> synchronizingPumps;

  private final Duration synchronizationInterval;

  private final Lock readLock;

  private final Lock writeLock;


  /*
   * Constructors.
   */


  /**
   * Creates a new {@link EventDistributor}.
   *
   * @param knownObjects a mutable {@link Map} of Kubernetes resources
   * that contains or will contain Kubernetes resources known to this
   * {@link EventDistributor} and whatever mechanism (such as a {@link
   * Controller}) is feeding it; may be {@code null}
   *
   * @see #EventDistributor(Map, Duration)
   */
  public EventDistributor(final Map<Object, T> knownObjects) {
    this(knownObjects, null);
  }

  /**
   * Creates a new {@link EventDistributor}.
   *
   * @param knownObjects a mutable {@link Map} of Kubernetes resources
   * that contains or will contain Kubernetes resources known to this
   * {@link EventDistributor} and whatever mechanism (such as a {@link
   * Controller}) is feeding it; may be {@code null}
   *
   * @param synchronizationInterval a {@link Duration} representing
   * the interval after which an attempt to synchronize might happen;
   * may be {@code null} in which case no synchronization will occur
   *
   * @see
   * ResourceTrackingEventQueueConsumer#ResourceTrackingEventQueueConsumer(Map)
   */
  public EventDistributor(final Map<Object, T> knownObjects, final Duration synchronizationInterval) {
    super(knownObjects);
    final ReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
    this.pumps = new ArrayList<>();
    this.synchronizingPumps = new ArrayList<>();
    this.synchronizationInterval = synchronizationInterval;
  }


  /*
   * Instance methods.
   */


  /**
   * Adds the supplied {@link Consumer} to this {@link
   * EventDistributor} as a listener that will be notified of each
   * {@link AbstractEvent} this {@link EventDistributor} receives.
   *
   * <p>The supplied {@link Consumer}'s {@link
   * Consumer#accept(Object)} method may be called later on a separate
   * thread of execution.</p>
   *
   * @param consumer a {@link Consumer} of {@link AbstractEvent}s; may
   * be {@code null} in which case no action will be taken
   *
   * @see #addConsumer(Consumer, Function)
   *
   * @see #removeConsumer(Consumer)
   */
  public final void addConsumer(final Consumer<? super AbstractEvent<? extends T>> consumer) {
    this.addConsumer(consumer, null);
  }

  /**
   * Adds the supplied {@link Consumer} to this {@link
   * EventDistributor} as a listener that will be notified of each
   * {@link AbstractEvent} this {@link EventDistributor} receives.
   *
   * <p>The supplied {@link Consumer}'s {@link
   * Consumer#accept(Object)} method may be called later on a separate
   * thread of execution.</p>
   *
   * @param consumer a {@link Consumer} of {@link AbstractEvent}s; may
   * be {@code null} in which case no action will be taken
   *
   * @param errorHandler a {@link Function} to handle any {@link
   * Throwable}s encountered; may be {@code null} in which case a
   * default error handler will be used instead
   *
   * @see #removeConsumer(Consumer)
   */
  public final void addConsumer(final Consumer<? super AbstractEvent<? extends T>> consumer, final Function<? super Throwable, Boolean> errorHandler) {
    if (consumer != null) {
      this.writeLock.lock();
      try {
        final Pump<T> pump = new Pump<>(this.synchronizationInterval, consumer, errorHandler);
        pump.start();
        this.pumps.add(pump);
        this.synchronizingPumps.add(pump);
      } finally {
        this.writeLock.unlock();
      }
    }
  }

  /**
   * Removes any {@link Consumer} {@linkplain Object#equals(Object)
   * equal to} a {@link Consumer} previously {@linkplain
   * #addConsumer(Consumer) added} to this {@link EventDistributor}.
   *
   * @param consumer the {@link Consumer} to remove; may be {@code
   * null} in which case no action will be taken
   *
   * @see #addConsumer(Consumer)
   */
  public final void removeConsumer(final Consumer<? super AbstractEvent<? extends T>> consumer) {
    if (consumer != null) {
      this.writeLock.lock();
      try {
        final Iterator<? extends Pump<?>> iterator = this.pumps.iterator();
        assert iterator != null;
        while (iterator.hasNext()) {
          final Pump<?> pump = iterator.next();
          if (pump != null && consumer.equals(pump.getEventConsumer())) {
            pump.close();
            iterator.remove();
            break;
          }
        }
      } finally {
        this.writeLock.unlock();
      }
    }
  }

  /**
   * Releases resources held by this {@link EventDistributor} during
   * its execution.
   */
  @Override
  public final void close() {
    this.writeLock.lock();
    try {
      this.pumps.stream()
        .forEach(pump -> {
            pump.close();
          });
      this.synchronizingPumps.clear();
      this.pumps.clear();
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * Returns {@code true} if this {@link EventDistributor} should
   * <em>synchronize</em> with its upstream source.
   *
   * <h2>Design Notes</h2>
   *
   * <p>The Kubernetes {@code tools/cache} package spreads
   * synchronization out among the reflector, controller, event cache
   * and event processor constructs for no seemingly good reason.
   * They should probably be consolidated, particularly in an
   * object-oriented environment such as Java.</p>
   *
   * @return {@code true} if synchronization should occur; {@code
   * false} otherwise
   *
   * @see EventCache#synchronize()
   */
  public final boolean shouldSynchronize() {
    boolean returnValue = false;
    this.writeLock.lock();
    try {
      this.synchronizingPumps.clear();
      final Instant now = Instant.now();
      this.pumps.stream()
        .filter(pump -> pump.shouldSynchronize(now))
        .forEach(pump -> {
            this.synchronizingPumps.add(pump);
            pump.determineNextSynchronizationInterval(now);
          });
      returnValue = !this.synchronizingPumps.isEmpty();
    } finally {
      this.writeLock.unlock();
    }
    return returnValue;
  }

  /**
   * Consumes the supplied {@link AbstractEvent} by forwarding it to
   * the {@link Consumer#accept(Object)} method of each {@link
   * Consumer} {@linkplain #addConsumer(Consumer) registered} with
   * this {@link EventDistributor}.
   *
   * @param event the {@link AbstractEvent} to forward; may be {@code
   * null} in which case no action is taken
   *
   * @see #addConsumer(Consumer)
   *
   * @see ResourceTrackingEventQueueConsumer#accept(AbstractEvent)
   */
  @Override
  protected final void accept(final AbstractEvent<? extends T> event) {
    if (event != null) {
      if (event instanceof SynchronizationEvent) {
        this.accept((SynchronizationEvent<? extends T>)event);
      } else if (event instanceof Event) {
        this.accept((Event<? extends T>)event);
      } else {
        assert false : "Unexpected event type: " + event.getClass();
      }
    }
  }

  private final void accept(final SynchronizationEvent<? extends T> event) {
    this.readLock.lock();
    try {
      if (!this.synchronizingPumps.isEmpty()) {
        this.synchronizingPumps.stream()
          .forEach(pump -> pump.accept(event));
      }
    } finally {
      this.readLock.unlock();
    }
  }

  private final void accept(final Event<? extends T> event) {
    this.readLock.lock();
    try {
      if (!this.pumps.isEmpty()) {
        this.pumps.stream()
          .forEach(pump -> pump.accept(event));
      }
    } finally {
      this.readLock.unlock();
    }
  }


  /*
   * Inner and nested classes.
   */


  /**
   * A {@link Consumer} of {@link AbstractEvent} instances that puts
   * them on an internal queue and, in a separate thread, removes them
   * from the queue and forwards them to the "real" {@link Consumer}
   * supplied at construction time.
   *
   * <p>A {@link Pump} differs from a simple {@link Consumer} of
   * {@link AbstractEvent} instances in that it has its own
   * {@linkplain #getSynchronizationInterval() synchronization
   * interval}, and interposes a blocking queue in between the
   * reception of an {@link AbstractEvent} and its eventual broadcast.</p>
   *
   * @author <a href="https://about.me/lairdnelson"
   * target="_parent">Laird Nelson</a>
   */
  private static final class Pump<T extends HasMetadata> implements Consumer<AbstractEvent<? extends T>>, AutoCloseable {

    private final Logger logger;

    private final Consumer<? super AbstractEvent<? extends T>> eventConsumer;

    private final Function<? super Throwable, Boolean> errorHandler;

    private volatile boolean closing;

    private volatile Instant nextSynchronizationInstant;

    private volatile Duration synchronizationInterval;

    @GuardedBy("this")
    private ScheduledExecutorService executor;

    @GuardedBy("this")
    private Future<?> task;

    private volatile Future<?> errorHandlingTask;

    final BlockingQueue<AbstractEvent<? extends T>> queue;

    private Pump(final Duration synchronizationInterval, final Consumer<? super AbstractEvent<? extends T>> eventConsumer) {
      this(synchronizationInterval, eventConsumer, null);
    }

    private Pump(final Duration synchronizationInterval, final Consumer<? super AbstractEvent<? extends T>> eventConsumer, final Function<? super Throwable, Boolean> errorHandler) {
      super();
      final String cn = this.getClass().getName();
      this.logger = Logger.getLogger(cn);
      assert this.logger != null;
      final String mn = "<init>";
      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.entering(cn, mn, new Object[] { synchronizationInterval, eventConsumer, errorHandler });
      }

      // TODO: this should be extensible
      this.queue = new LinkedBlockingQueue<>();
      this.eventConsumer = Objects.requireNonNull(eventConsumer);
      if (errorHandler == null) {
        this.errorHandler = t -> {
          if (this.logger.isLoggable(Level.SEVERE)) {
            this.logger.logp(Level.SEVERE, this.getClass().getName(), "<pumpTask>", t.getMessage(), t);
          }
          return true;
        };
      } else {
        this.errorHandler = errorHandler;
      }
      this.setSynchronizationInterval(synchronizationInterval);

      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.exiting(cn, mn);
      }
    }

    private final void start() {
      final String cn = this.getClass().getName();
      final String mn = "start";
      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.entering(cn, mn);
      }

      synchronized (this) {

        if (this.executor == null) {
          assert this.task == null;
          assert this.errorHandlingTask == null;

          this.executor = this.createScheduledThreadPoolExecutor();
          if (this.executor == null) {
            throw new IllegalStateException("createScheduledThreadPoolExecutor() == null");
          }

          // Schedule a hopefully never-ending task to pump events from
          // our queue to the supplied eventConsumer.  We *schedule* this,
          // even though it will never end, instead of simply *executing*
          // it, so that if for any reason it exits (by definition an
          // error case) it will get restarted.  Cancelling a scheduled
          // task will also cancel all resubmissions of it, so this is the
          // most robust thing to do.  The delay of one second is
          // arbitrary.
          this.task = this.executor.scheduleWithFixedDelay(() -> {
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  this.getEventConsumer().accept(this.queue.take());
                } catch (final InterruptedException interruptedException) {
                  Thread.currentThread().interrupt();
                } catch (final RuntimeException runtimeException) {
                  if (!this.errorHandler.apply(runtimeException)) {
                    throw runtimeException;
                  }
                } catch (final Error error) {
                  if (!this.errorHandler.apply(error)) {
                    throw error;
                  }
                }
              }
            }, 0L, 1L, TimeUnit.SECONDS);
          assert this.task != null;

          this.errorHandlingTask = this.executor.submit(() -> {
              try {
                while (!Thread.currentThread().isInterrupted()) {
                  // The task is basically never-ending, so this will
                  // block too, unless there's an exception.  That's
                  // the whole point.
                  this.task.get();
                }
              } catch (final CancellationException ok) {
                // The task was cancelled.  Possibly redundantly,
                // cancel it for sure.  This is an expected and normal
                // condition.
                this.task.cancel(true);
              } catch (final ExecutionException executionException) {
                // The task encountered an exception while executing.
                // Although we got an ExecutionException, the task is
                // still in a non-cancelled state.  We need to cancel
                // it now to (potentially) have it removed from the
                // executor queue.
                this.task.cancel(true);
                final Future<?> errorHandlingTask = this.errorHandlingTask;
                if (errorHandlingTask != null) {
                  errorHandlingTask.cancel(true); // cancel ourselves, too!
                }
                // Apply the actual error-handling logic to the
                // exception.
                // TODO: This should have already been done by the
                // task itself...
                this.errorHandler.apply(executionException.getCause());
              } catch (final InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
              }
              if (Thread.currentThread().isInterrupted()) {
                // The current thread was interrupted, probably
                // because everything is closing up shop.  Cancel
                // everything and go home.
                this.task.cancel(true);
                final Future<?> errorHandlingTask = this.errorHandlingTask;
                if (errorHandlingTask != null) {
                  errorHandlingTask.cancel(true); // cancel ourselves, too!
                }
              }              
            });
        }

      }

      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.entering(cn, mn);
      }
    }

    private final ScheduledExecutorService createScheduledThreadPoolExecutor() {
      final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(2, new PumpThreadFactory());
      executor.setRemoveOnCancelPolicy(true);
      return executor;
    }

    private final Consumer<? super AbstractEvent<? extends T>> getEventConsumer() {
      return this.eventConsumer;
    }

    /**
     * Adds the supplied {@link AbstractEvent} to an internal {@link
     * BlockingQueue}.  A task will have already been scheduled to
     * consume it.
     *
     * @param event the {@link AbstractEvent} to add; may be {@code
     * null} in which case no action is taken
     */
    @Override
    public final void accept(final AbstractEvent<? extends T> event) {
      final String cn = this.getClass().getName();
      final String mn = "accept";
      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.entering(cn, mn, event);
      }
      if (this.closing) {
        throw new IllegalStateException();
      }
      if (event != null) {
        final boolean added = this.queue.add(event);
        assert added;
      }
      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.exiting(cn, mn);
      }
    }

    @Override
    public final void close() {
      final String cn = this.getClass().getName();
      final String mn = "close";
      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.entering(cn, mn);
      }

      synchronized (this) {
        if (!this.closing) {
          try {
            assert this.executor != null;
            assert this.task != null;
            assert this.errorHandlingTask != null;
            this.closing = true;
            
            // Stop accepting new tasks.
            this.executor.shutdown();
            
            // Cancel our regular task.
            this.task.cancel(true);
            this.task = null;
            
            // Cancel our task that surfaces errors from the regular task.
            this.errorHandlingTask.cancel(true);
            this.errorHandlingTask = null;
            
            try {
              // Wait for our executor to shut down normally, and shut
              // it down forcibly if it doesn't.
              if (!this.executor.awaitTermination(60, TimeUnit.SECONDS)) {
                this.executor.shutdownNow();
                if (!this.executor.awaitTermination(60, TimeUnit.SECONDS)) {
                  if (this.logger.isLoggable(Level.WARNING)) {
                    this.logger.logp(Level.WARNING, cn, mn, "this.executor.awaitTermination() failed");
                  }
                }
              }
            } catch (final InterruptedException interruptedException) {
              this.executor.shutdownNow();
              Thread.currentThread().interrupt();
            }
            this.executor = null;
          } finally {
            this.closing = false;
          }
        }
      }

      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.exiting(cn, mn);
      }
    }


    /*
     * Synchronization-related methods.  It seems odd that one of these
     * listeners would need to report details about synchronization, but
     * that's what the Go code does.  Maybe this functionality could be
     * relocated "higher up".
     */


    private final boolean shouldSynchronize(final Instant now) {
      final String cn = this.getClass().getName();
      final String mn = "shouldSynchronize";
      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.entering(cn, mn, now);
      }
      final boolean returnValue;
      if (this.closing) {
        returnValue = false;
      } else {
        final Duration interval = this.getSynchronizationInterval();
        if (interval == null || interval.isZero()) {
          returnValue = false;
        } else if (now == null) {
          returnValue = Instant.now().compareTo(this.nextSynchronizationInstant) >= 0;
        } else {
          returnValue = now.compareTo(this.nextSynchronizationInstant) >= 0;
        }
      }
      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.exiting(cn, mn, Boolean.valueOf(returnValue));
      }
      return returnValue;
    }

    private final void determineNextSynchronizationInterval(final Instant now) {
      final String cn = this.getClass().getName();
      final String mn = "determineNextSynchronizationInterval";
      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.entering(cn, mn, now);
      }
      final Duration synchronizationInterval = this.getSynchronizationInterval();
      if (synchronizationInterval == null) {
        if (now == null) {
          this.nextSynchronizationInstant = Instant.now();
        } else {
          this.nextSynchronizationInstant = now;
        }
      } else if (now == null) {
        this.nextSynchronizationInstant = Instant.now().plus(synchronizationInterval);
      } else {
        this.nextSynchronizationInstant = now.plus(synchronizationInterval);
      }
      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.entering(cn, mn);
      }
    }

    public final void setSynchronizationInterval(final Duration synchronizationInterval) {
      this.synchronizationInterval = synchronizationInterval;
    }

    public final Duration getSynchronizationInterval() {
      return this.synchronizationInterval;
    }


    /*
     * Inner and nested classes.
     */
    
    
    /**
     * A {@link ThreadFactory} that {@linkplain #newThread(Runnable)
     * produces new <code>Thread</code>s} with sane names.
     *
     * @author <a href="https://about.me/lairdnelson"
     * target="_parent">Laird Nelson</a>
     */
    private static final class PumpThreadFactory implements ThreadFactory {

      private final ThreadGroup group;

      private final AtomicInteger threadNumber = new AtomicInteger(1);

      private PumpThreadFactory() {
        final SecurityManager s = System.getSecurityManager();
        if (s == null) {
          this.group = Thread.currentThread().getThreadGroup();
        } else {
          this.group = s.getThreadGroup();
        }
      }

      @Override
      public final Thread newThread(final Runnable runnable) {
        final Thread returnValue = new Thread(this.group, runnable, "event-pump-thread-" + this.threadNumber.getAndIncrement(), 0);
        if (returnValue.isDaemon()) {
          returnValue.setDaemon(false);
        }
        if (returnValue.getPriority() != Thread.NORM_PRIORITY) {
          returnValue.setPriority(Thread.NORM_PRIORITY);
        }
        return returnValue;
      }
    }

  }

}
