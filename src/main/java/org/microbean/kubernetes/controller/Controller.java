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

import java.io.Closeable;
import java.io.IOException;

import java.time.Duration;

import java.util.Map;
import java.util.Objects;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import java.util.function.Consumer;
import java.util.function.Function;

import java.util.logging.Level;
import java.util.logging.Logger;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;

import io.fabric8.kubernetes.client.KubernetesClientException; // for javadoc only
import io.fabric8.kubernetes.client.Watcher;

import io.fabric8.kubernetes.client.dsl.Listable;
import io.fabric8.kubernetes.client.dsl.VersionWatchable;

import net.jcip.annotations.Immutable;
import net.jcip.annotations.ThreadSafe;

import org.microbean.development.annotation.Blocking;
import org.microbean.development.annotation.NonBlocking;

/**
 * A convenient combination of a {@link Reflector}, a {@link
 * VersionWatchable} and {@link Listable} implementation, an
 * (internal) {@link EventQueueCollection}, a {@link Map} of known
 * Kubernetes resources and an {@link EventQueue} {@link Consumer}
 * that {@linkplain Reflector#start() mirrors Kubernetes cluster
 * events} into a {@linkplain EventQueueCollection collection of
 * <code>EventQueue</code>s} and {@linkplain
 * EventQueueCollection#start(Consumer) arranges for their consumption
 * and processing}.
 *
 * <p>{@linkplain #start() Starting} a {@link Controller} {@linkplain
 * EventQueueCollection#start(Consumer) starts the
 * <code>Consumer</code>} supplied at construction time, and
 * {@linkplain Reflector#start() starts the embedded
 * <code>Reflector</code>}.  {@linkplain #close() Closing} a {@link
 * Controller} {@linkplain Reflector#close() closes its embedded
 * <code>Reflector</code>} and {@linkplain
 * EventQueueCollection#close() causes the <code>Consumer</code>
 * supplied at construction time to stop receiving
 * <code>Event</code>s}.</p>
 *
 * <p>Several {@code protected} methods in this class exist to make
 * customization easier; none require overriding and their default
 * behavior is usually just fine.</p>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>Instances of this class are safe for concurrent use by multiple
 * threads.</p>
 *
 * <h2>Design Notes</h2>
 *
 * <p>This class loosely models a combination of a <a
 * href="https://github.com/kubernetes/client-go/blob/79cb21f5b3b1dd8f8b23bd3f79925b4fda4e2562/tools/cache/controller.go#L82-L86">{@code
 * Controller} type</a> and a <a
 * href="https://github.com/kubernetes/client-go/blob/79cb21f5b3b1dd8f8b23bd3f79925b4fda4e2562/tools/cache/shared_informer.go#L66-L71">{@code
 * SharedIndexInformer} type</a> as found in <a
 * href="https://github.com/kubernetes/client-go/blob/master/tools/cache/controller.go">{@code
 * controller.go}</a> and <a
 * href="https://github.com/kubernetes/client-go/blob/master/tools/cache/shared_informer.go">{@code
 * shared_informer.go}</a> respectively.</p>
 *
 * @param <T> a Kubernetes resource type
 *
 * @author <a href="https://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 *
 * @see Reflector
 *
 * @see EventQueueCollection
 *
 * @see ResourceTrackingEventQueueConsumer
 *
 * @see #start()
 *
 * @see #close()
 */
@Immutable
@ThreadSafe
public class Controller<T extends HasMetadata> implements Closeable {


  /*
   * Instance fields.
   */


  /**
   * A {@link Logger} used by this {@link Controller}.
   *
   * <p>This field is never {@code null}.</p>
   *
   * @see #createLogger()
   */
  protected final Logger logger;

  /**
   * The {@link Reflector} used by this {@link Controller} to mirror
   * Kubernetes events.
   *
   * <p>This field is never {@code null}.</p>
   */
  private final Reflector<T> reflector;

  /**
   * The {@link EventQueueCollection} used by the {@link #reflector
   * Reflector} and by the {@link Consumer} supplied at construction
   * time.
   *
   * <p>This field is never {@code null}.</p>
   *
   * @see EventQueueCollection#add(Object, AbstractEvent.Type,
   * HasMetadata)
   *
   * @see EventQueueCollection#replace(Collection, Object)
   *
   * @see EventQueueCollection#synchronize()
   *
   * @see EventQueueCollection#start(Consumer)
   */
  private final EventQueueCollection<T> eventQueueCollection;

  private final EventQueueCollection.SynchronizationAwaitingPropertyChangeListener synchronizationAwaiter;

  /**
   * A {@link Consumer} of {@link EventQueue}s that processes {@link
   * Event}s produced, ultimately, by the {@link #reflector
   * Reflector}.
   *
   * <p>This field is never {@code null}.</p>
   */
  private final Consumer<? super EventQueue<? extends T>> eventQueueConsumer;

  
  /*
   * Constructors.
   */


  /**
   * Creates a new {@link Controller} but does not {@linkplain
   * #start() start it}.
   *
   * @param <X> a {@link Listable} and {@link VersionWatchable} that
   * will be used by the embedded {@link Reflector}; must not be
   * {@code null}
   *
   * @param operation a {@link Listable} and a {@link
   * VersionWatchable} that produces Kubernetes events; must not be
   * {@code null}
   *
   * @param eventQueueConsumer the {@link Consumer} that will process
   * each {@link EventQueue} as it becomes ready; must not be {@code
   * null}
   *
   * @exception NullPointerException if {@code operation} or {@code
   * eventQueueConsumer} is {@code null}
   *
   * @see #Controller(Listable, ScheduledExecutorService, Duration,
   * Map, Consumer)
   *
   * @see #start()
   */
  @SuppressWarnings("rawtypes")
  public <X extends Listable<? extends KubernetesResourceList>
                    & VersionWatchable<? extends Closeable,
                                                 Watcher<T>>> Controller(final X operation,
                                                                         final Consumer<? super EventQueue<? extends T>> eventQueueConsumer) {
    this(operation, null, null, null, eventQueueConsumer);
  }

  /**
   * Creates a new {@link Controller} but does not {@linkplain
   * #start() start it}.
   *
   * @param <X> a {@link Listable} and {@link VersionWatchable} that
   * will be used by the embedded {@link Reflector}; must not be
   * {@code null}
   *
   * @param operation a {@link Listable} and a {@link
   * VersionWatchable} that produces Kubernetes events; must not be
   * {@code null}
   *
   * @param knownObjects a {@link Map} containing the last known state
   * of Kubernetes resources the embedded {@link EventQueueCollection}
   * is caching events for; may be {@code null} if this {@link
   * Controller} is not interested in tracking deletions of objects;
   * if non-{@code null} <strong>will be synchronized on by this
   * class</strong> during retrieval and traversal operations
   *
   * @param eventQueueConsumer the {@link Consumer} that will process
   * each {@link EventQueue} as it becomes ready; must not be {@code
   * null}
   *
   * @exception NullPointerException if {@code operation} or {@code
   * eventQueueConsumer} is {@code null}
   *
   * @see #Controller(Listable, ScheduledExecutorService, Duration,
   * Map, Consumer)
   *
   * @see #start()
   */
  @SuppressWarnings("rawtypes")
  public <X extends Listable<? extends KubernetesResourceList>
                    & VersionWatchable<? extends Closeable,
                                                 Watcher<T>>> Controller(final X operation,
                                                                         final Map<Object, T> knownObjects,
                                                                         final Consumer<? super EventQueue<? extends T>> eventQueueConsumer) {
    this(operation, null, null, knownObjects, eventQueueConsumer);
  }

  /**
   * Creates a new {@link Controller} but does not {@linkplain
   * #start() start it}.
   *
   * @param <X> a {@link Listable} and {@link VersionWatchable} that
   * will be used by the embedded {@link Reflector}; must not be
   * {@code null}
   *
   * @param operation a {@link Listable} and a {@link
   * VersionWatchable} that produces Kubernetes events; must not be
   * {@code null}
   *
   * @param synchronizationInterval a {@link Duration} representing
   * the time in between one {@linkplain EventCache#synchronize()
   * synchronization operation} and another; may be {@code null} in
   * which case no synchronization will occur
   *
   * @param eventQueueConsumer the {@link Consumer} that will process
   * each {@link EventQueue} as it becomes ready; must not be {@code
   * null}
   *
   * @exception NullPointerException if {@code operation} or {@code
   * eventQueueConsumer} is {@code null}
   *
   * @see #Controller(Listable, ScheduledExecutorService, Duration,
   * Map, Consumer)
   *
   * @see #start()
   */
  @SuppressWarnings("rawtypes")
  public <X extends Listable<? extends KubernetesResourceList>
                    & VersionWatchable<? extends Closeable,
                                                 Watcher<T>>> Controller(final X operation,
                                                                         final Duration synchronizationInterval,
                                                                         final Consumer<? super EventQueue<? extends T>> eventQueueConsumer) {
    this(operation, null, synchronizationInterval, null, eventQueueConsumer);
  }

  /**
   * Creates a new {@link Controller} but does not {@linkplain
   * #start() start it}.
   *
   * @param <X> a {@link Listable} and {@link VersionWatchable} that
   * will be used by the embedded {@link Reflector}; must not be
   * {@code null}
   *
   * @param operation a {@link Listable} and a {@link
   * VersionWatchable} that produces Kubernetes events; must not be
   * {@code null}
   *
   * @param synchronizationInterval a {@link Duration} representing
   * the time in between one {@linkplain EventCache#synchronize()
   * synchronization operation} and another; may be {@code null} in
   * which case no synchronization will occur
   *
   * @param knownObjects a {@link Map} containing the last known state
   * of Kubernetes resources the embedded {@link EventQueueCollection}
   * is caching events for; may be {@code null} if this {@link
   * Controller} is not interested in tracking deletions of objects;
   * if non-{@code null} <strong>will be synchronized on by this
   * class</strong> during retrieval and traversal operations
   *
   * @param eventQueueConsumer the {@link Consumer} that will process
   * each {@link EventQueue} as it becomes ready; must not be {@code
   * null}
   *
   * @exception NullPointerException if {@code operation} or {@code
   * eventQueueConsumer} is {@code null}
   *
   * @see #Controller(Listable, ScheduledExecutorService, Duration,
   * Map, Consumer)
   *
   * @see #start()
   */
  @SuppressWarnings("rawtypes")
  public <X extends Listable<? extends KubernetesResourceList>
                    & VersionWatchable<? extends Closeable,
                                                 Watcher<T>>> Controller(final X operation,
                                                                         final Duration synchronizationInterval,
                                                                         final Map<Object, T> knownObjects,
                                                                         final Consumer<? super EventQueue<? extends T>> eventQueueConsumer) {
    this(operation, null, synchronizationInterval, knownObjects, eventQueueConsumer);
  }

  /**
   * Creates a new {@link Controller} but does not {@linkplain
   * #start() start it}.
   *
   * @param <X> a {@link Listable} and {@link VersionWatchable} that
   * will be used by the embedded {@link Reflector}; must not be
   * {@code null}
   *
   * @param operation a {@link Listable} and a {@link
   * VersionWatchable} that produces Kubernetes events; must not be
   * {@code null}
   *
   * @param synchronizationExecutorService the {@link
   * ScheduledExecutorService} that will be passed to the {@link
   * Reflector} constructor; may be {@code null} in which case a
   * default {@link ScheduledExecutorService} may be used instead
   *
   * @param synchronizationInterval a {@link Duration} representing
   * the time in between one {@linkplain EventCache#synchronize()
   * synchronization operation} and another; may be {@code null} in
   * which case no synchronization will occur
   *
   * @param knownObjects a {@link Map} containing the last known state
   * of Kubernetes resources the embedded {@link EventQueueCollection}
   * is caching events for; may be {@code null} if this {@link
   * Controller} is not interested in tracking deletions of objects;
   * if non-{@code null} <strong>will be synchronized on by this
   * class</strong> during retrieval and traversal operations
   *
   * @param eventQueueConsumer the {@link Consumer} that will process
   * each {@link EventQueue} as it becomes ready; must not be {@code
   * null}
   *
   * @exception NullPointerException if {@code operation} or {@code
   * eventQueueConsumer} is {@code null}
   *
   * @see #start()
   */
  @SuppressWarnings("rawtypes")
  public <X extends Listable<? extends KubernetesResourceList>
                    & VersionWatchable<? extends Closeable,
                                                 Watcher<T>>> Controller(final X operation,
                                                                         final ScheduledExecutorService synchronizationExecutorService,
                                                                         final Duration synchronizationInterval,
                                                                         final Map<Object, T> knownObjects,
                                                                         final Consumer<? super EventQueue<? extends T>> eventQueueConsumer) {
    this(operation, synchronizationExecutorService, synchronizationInterval, null, knownObjects, eventQueueConsumer);
  }

  /**
   * Creates a new {@link Controller} but does not {@linkplain
   * #start() start it}.
   *
   * @param <X> a {@link Listable} and {@link VersionWatchable} that
   * will be used by the embedded {@link Reflector}; must not be
   * {@code null}
   *
   * @param operation a {@link Listable} and a {@link
   * VersionWatchable} that produces Kubernetes events; must not be
   * {@code null}
   *
   * @param synchronizationExecutorService the {@link
   * ScheduledExecutorService} that will be passed to the {@link
   * Reflector} constructor; may be {@code null} in which case a
   * default {@link ScheduledExecutorService} may be used instead
   *
   * @param synchronizationInterval a {@link Duration} representing
   * the time in between one {@linkplain EventCache#synchronize()
   * synchronization operation} and another; may be {@code null} in
   * which case no synchronization will occur
   *
   * @param errorHandler a {@link Function} that accepts a {@link
   * Throwable} and returns a {@link Boolean} indicating whether the
   * error was handled or not; used to handle truly unanticipated
   * errors from within a {@link ScheduledExecutorService} used
   * during {@linkplain EventCache#synchronize() synchronization} and
   * event consumption activities; may be {@code null}
   *
   * @param knownObjects a {@link Map} containing the last known state
   * of Kubernetes resources the embedded {@link EventQueueCollection}
   * is caching events for; may be {@code null} if this {@link
   * Controller} is not interested in tracking deletions of objects;
   * if non-{@code null} <strong>will be synchronized on by this
   * class</strong> during retrieval and traversal operations
   *
   * @param eventQueueConsumer the {@link Consumer} that will process
   * each {@link EventQueue} as it becomes ready; must not be {@code
   * null}
   *
   * @exception NullPointerException if {@code operation} or {@code
   * eventQueueConsumer} is {@code null}
   *
   * @see #start()
   */
  @SuppressWarnings("rawtypes")
  public <X extends Listable<? extends KubernetesResourceList>
                    & VersionWatchable<? extends Closeable,
                                                 Watcher<T>>> Controller(final X operation,
                                                                         final ScheduledExecutorService synchronizationExecutorService,
                                                                         final Duration synchronizationInterval,
                                                                         final Function<? super Throwable, Boolean> errorHandler,
                                                                         final Map<Object, T> knownObjects,
                                                                         final Consumer<? super EventQueue<? extends T>> eventQueueConsumer) {
    super();
    this.logger = this.createLogger();
    if (this.logger == null) {
      throw new IllegalStateException("createLogger() == null");
    }
    final String cn = this.getClass().getName();
    final String mn = "<init>";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { operation, synchronizationExecutorService, synchronizationInterval, errorHandler, knownObjects, eventQueueConsumer });
    }
    this.eventQueueConsumer = Objects.requireNonNull(eventQueueConsumer);
    this.eventQueueCollection = new ControllerEventQueueCollection(knownObjects, errorHandler, 16, 0.75f);
    this.synchronizationAwaiter = new EventQueueCollection.SynchronizationAwaitingPropertyChangeListener();
    this.eventQueueCollection.addPropertyChangeListener(this.synchronizationAwaiter);
    this.reflector = new ControllerReflector(operation, synchronizationExecutorService, synchronizationInterval, errorHandler);
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }


  /*
   * Instance methods.
   */


  /**
   * Returns a {@link Logger} for use by this {@link Controller}.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * <p>Overrides of this method must not return {@code null}.</p>
   *
   * @return a non-{@code null} {@link Logger}
   */
  protected Logger createLogger() {
    return Logger.getLogger(this.getClass().getName());
  }

  /**
   * Blocks until the {@link EventQueueCollection} affiliated with
   * this {@link Controller} {@linkplain
   * EventQueueCollection#isSynchronized() has synchronized}.
   *
   * @exception InterruptedException if the current {@link Thread} was
   * interrupted
   */
  @Blocking
  public final void awaitEventCacheSynchronization() throws InterruptedException {
    this.synchronizationAwaiter.await();
  }

  /**
   * Blocks for the desired amount of time until the {@link
   * EventQueueCollection} affiliated with this {@link Controller}
   * {@linkplain EventQueueCollection#isSynchronized() has
   * synchronized} or the amount of time has elapsed.
   *
   * @param timeout the amount of time to wait
   *
   * @param timeUnit the {@link TimeUnit} designating the amount of
   * time to wait; must not be {@code null}
   *
   * @return {@code false} if the waiting time elapsed before the
   * event cache synchronized; {@code true} otherwise
   *
   * @exception InterruptedException if the current {@link Thread} was
   * interrupted
   *
   * @exception NullPointerException if {@code timeUnit} is {@code
   * null}
   *
   * @see EventQueueCollection.SynchronizationAwaitingPropertyChangeListener
   */
  @Blocking
  public final boolean awaitEventCacheSynchronization(final long timeout, final TimeUnit timeUnit) throws InterruptedException {
    return this.synchronizationAwaiter.await(timeout, timeUnit);
  }
  
  /**
   * {@linkplain EventQueueCollection#start(Consumer) Starts the
   * embedded <code>EventQueueCollection</code> consumption machinery}
   * and then {@linkplain Reflector#start() starts the embedded
   * <code>Reflector</code>}.
   *
   * @exception IOException if {@link Reflector#start()} throws an
   * {@link IOException}
   *
   * @exception KubernetesClientException if the {@linkplain Reflector
   * embedded <code>Reflector</code>} could not be started
   *
   * @see EventQueueCollection#start(Consumer)
   *
   * @see Reflector#start()
   */
  @NonBlocking
  public final void start() throws IOException {
    final String cn = this.getClass().getName();
    final String mn = "start";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn);
    }

    // Start the consumer that is going to drain our associated
    // EventQueueCollection.
    if (this.logger.isLoggable(Level.INFO)) {
      this.logger.logp(Level.INFO, cn, mn, "Starting {0}", this.eventQueueConsumer);
    }
    final Future<?> eventQueueConsumerTask = this.eventQueueCollection.start(this.eventQueueConsumer);
    assert eventQueueConsumerTask != null;

    // Start the Reflector--the machinery that is going to connect to
    // Kubernetes and "reflect" its (relevant) contents into the
    // EventQueueCollection.
    if (this.logger.isLoggable(Level.INFO)) {
      this.logger.logp(Level.INFO, cn, mn, "Starting {0}", this.reflector);
    }
    try {
      this.reflector.start();
    } catch (final IOException | RuntimeException | Error reflectorStartFailure) {
      try {
        // TODO: this is problematic, I think; reflector.close() means
        // that (potentially) it will never be able to restart it.
        // The Go code appears to make some feints in the direction of
        // restartability, and then just basically gives up.  I think
        // we can do better here.
        this.reflector.close();
      } catch (final Throwable suppressMe) {
        reflectorStartFailure.addSuppressed(suppressMe);
      }
      eventQueueConsumerTask.cancel(true);
      assert eventQueueConsumerTask.isDone();
      try {
        this.eventQueueCollection.close();
      } catch (final Throwable suppressMe) {
        reflectorStartFailure.addSuppressed(suppressMe);
      }
      throw reflectorStartFailure;
    }
    
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * {@linkplain Reflector#close() Closes the embedded
   * <code>Reflector</code>} and then {@linkplain
   * EventQueueCollection#close() closes the embedded
   * <code>EventQueueCollection</code>}, handling exceptions
   * appropriately.
   *
   * @exception IOException if the {@link Reflector} could not
   * {@linkplain Reflector#close() close} properly
   *
   * @see Reflector#close()
   *
   * @see EventQueueCollection#close()
   */
  @Override
  public final void close() throws IOException {
    final String cn = this.getClass().getName();
    final String mn = "close";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn);
    }
    Exception throwMe = null;    
    try {
      if (this.logger.isLoggable(Level.INFO)) {
        this.logger.logp(Level.INFO, cn, mn, "Closing {0}", this.reflector);
      }
      this.reflector.close();
    } catch (final Exception everything) {
      throwMe = everything;
    }

    try {
      if (this.logger.isLoggable(Level.INFO)) {
        this.logger.logp(Level.INFO, cn, mn, "Closing {0}", this.eventQueueCollection);
      }
      this.eventQueueCollection.close();
    } catch (final RuntimeException | Error runtimeException) {
      if (throwMe == null) {
        throw runtimeException;
      }
      throwMe.addSuppressed(runtimeException);
    }

    if (throwMe instanceof IOException) {
      throw (IOException)throwMe;
    } else if (throwMe instanceof RuntimeException) {
      throw (RuntimeException)throwMe;
    } else if (throwMe != null) {
      throw new IllegalStateException(throwMe.getMessage(), throwMe);
    }

    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * Returns if the embedded {@link Reflector} should {@linkplain
   * Reflector#shouldSynchronize() synchronize}.
   *
   * <p>This implementation returns {@code true}.</p>
   *
   * @return {@code true} if the embedded {@link Reflector} should
   * {@linkplain Reflector#shouldSynchronize() synchronize}; {@code
   * false} otherwise
   */
  protected boolean shouldSynchronize() {
    final String cn = this.getClass().getName();
    final String mn = "shouldSynchronize";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn);
    }
    final boolean returnValue = true;
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn, Boolean.valueOf(returnValue));
    }
    return returnValue;
  }

  /**
   * Invoked after the embedded {@link Reflector} {@linkplain
   * Reflector#onClose() closes}.
   *
   * <p>This implementation does nothing.</p>
   *
   * @see Reflector#close()
   *
   * @see Reflector#onClose()
   */
  protected void onClose() {

  }

  /**
   * Returns a key that can be used to identify the supplied {@link
   * HasMetadata}.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * <p>Overrides of this method must not return {@code null}.</p>
   *
   * <p>The default implementation of this method returns the return
   * value of invoking the {@link HasMetadatas#getKey(HasMetadata)}
   * method.</p>
   *
   * @param resource the Kubernetes resource for which a key is
   * desired; must not be {@code null}
   *
   * @return a non-{@code null} key for the supplied {@link
   * HasMetadata}
   *
   * @exception NullPointerException if {@code resource} is {@code
   * null}
   */
  protected Object getKey(final T resource) {
    final String cn = this.getClass().getName();
    final String mn = "getKey";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, resource);
    }
    final Object returnValue = HasMetadatas.getKey(Objects.requireNonNull(resource));
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn, returnValue);
    }
    return returnValue;
  }

  /**
   * Creates a new {@link Event} when invoked.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * <p>Overrides of this method must not return {@code null}.</p>
   *
   * <p>Overrides of this method must return a new {@link Event} or
   * subclass with each invocation.</p>
   *
   * @param source the source of the new {@link Event}; must not be
   * {@code null}
   *
   * @param eventType the {@link Event.Type} for the new {@link
   * Event}; must not be {@code null}
   *
   * @param resource the {@link HasMetadata} that the new {@link
   * Event} concerns; must not be {@code null}
   *
   * @return a new, non-{@code null} {@link Event}
   *
   * @exception NullPointerException if any of the parameters is
   * {@code null}
   */
  protected Event<T> createEvent(final Object source, final Event.Type eventType, final T resource) {
    final String cn = this.getClass().getName();
    final String mn = "createEvent";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { source, eventType, resource });
    }
    final Event<T> returnValue = new Event<>(Objects.requireNonNull(source), Objects.requireNonNull(eventType), null, Objects.requireNonNull(resource));
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn, returnValue);
    }
    return returnValue;
  }

  /**
   * Creates a new {@link EventQueue} when invoked.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * <p>Overrides of this method must not return {@code null}.</p>
   *
   * <p>Overrides of this method must return a new {@link EventQueue}
   * or subclass with each invocation.</p>
   *
   * @param key the key to create the new {@link EventQueue} with;
   * must not be {@code null}
   *
   * @return a new, non-{@code null} {@link EventQueue}
   *
   * @exception NullPointerException if {@code key} is {@code null}
   */
  protected EventQueue<T> createEventQueue(final Object key) {
    final String cn = this.getClass().getName();
    final String mn = "createEventQueue";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, key);
    }
    final EventQueue<T> returnValue = new EventQueue<>(key);
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn, returnValue);
    }
    return returnValue;
  }


  /*
   * Inner and nested classes.
   */


  /**
   * An {@link EventQueueCollection} that delegates its overridable
   * methods to their equivalents in the {@link Controller} class.
   *
   * @author <a href="https://about.me/lairdnelson"
   * target="_parent">Laird Nelson</a>
   *
   * @see EventQueueCollection
   *
   * @see EventCache
   */
  private final class ControllerEventQueueCollection extends EventQueueCollection<T> {


    /*
     * Constructors.
     */

    
    private ControllerEventQueueCollection(final Map<?, ? extends T> knownObjects,
                                           final Function<? super Throwable, Boolean> errorHandler,
                                           final int initialCapacity,
                                           final float loadFactor) {
      super(knownObjects, errorHandler, initialCapacity, loadFactor);
    }


    /*
     * Instance methods.
     */

    
    @Override
    protected final Event<T> createEvent(final Object source, final Event.Type eventType, final T resource) {
      return Controller.this.createEvent(source, eventType, resource);
    }
    
    @Override
    protected final EventQueue<T> createEventQueue(final Object key) {
      return Controller.this.createEventQueue(key);
    }
    
    @Override
    protected final Object getKey(final T resource) {
      return Controller.this.getKey(resource);
    }
    
  }

  
  /**
   * A {@link Reflector} that delegates its overridable
   * methods to their equivalents in the {@link Controller} class.
   *
   * @author <a href="https://about.me/lairdnelson"
   * target="_parent">Laird Nelson</a>
   *
   * @see Reflector
   */
  private final class ControllerReflector extends Reflector<T> {


    /*
     * Constructors.
     */

    
    @SuppressWarnings("rawtypes")
    private <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> ControllerReflector(final X operation,
                                                                                                                                           final ScheduledExecutorService synchronizationExecutorService,
                                                                                                                                           final Duration synchronizationInterval, final Function<? super Throwable, Boolean> synchronizationErrorHandler) {
      super(operation, Controller.this.eventQueueCollection, synchronizationExecutorService, synchronizationInterval, synchronizationErrorHandler);
    }


    /*
     * Instance methods.
     */
    

    /**
     * Invokes the {@link Controller#shouldSynchronize()} method and
     * returns its result.
     *
     * @return the result of invoking the {@link
     * Controller#shouldSynchronize()} method
     *
     * @see Controller#shouldSynchronize()
     */
    @Override
    protected final boolean shouldSynchronize() {
      return Controller.this.shouldSynchronize();
    }
    
    /**
     * Invokes the {@link Controller#onClose()} method.
     *
     * @see Controller#onClose()
     */
    @Override
    protected final void onClose() {
      Controller.this.onClose();
    }
  }
  
}
