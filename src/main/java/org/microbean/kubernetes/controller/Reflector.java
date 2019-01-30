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

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import java.time.Duration;

import java.time.temporal.ChronoUnit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Map;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import java.util.function.Function;

import java.util.logging.Level;
import java.util.logging.Logger;

import io.fabric8.kubernetes.client.DefaultKubernetesClient; // for javadoc only
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch; // for javadoc only
import io.fabric8.kubernetes.client.Watcher;

import io.fabric8.kubernetes.client.dsl.base.BaseOperation;
import io.fabric8.kubernetes.client.dsl.base.OperationSupport;

import io.fabric8.kubernetes.client.dsl.Listable;
import io.fabric8.kubernetes.client.dsl.Versionable;
import io.fabric8.kubernetes.client.dsl.VersionWatchable;
import io.fabric8.kubernetes.client.dsl.Watchable;

import io.fabric8.kubernetes.client.dsl.internal.CustomResourceOperationsImpl;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ListMeta;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import okhttp3.OkHttpClient;

import org.microbean.development.annotation.Hack;
import org.microbean.development.annotation.Issue;
import org.microbean.development.annotation.NonBlocking;

/**
 * A pump of sorts that continuously "pulls" logical events out of
 * Kubernetes and {@linkplain EventCache#add(Object, AbstractEvent.Type,
 * HasMetadata) adds them} to an {@link EventCache} so as to logically
 * "reflect" the contents of Kubernetes into the cache.
 *
 * <h2>Thread Safety</h2>
 *
 * <p>Instances of this class are safe for concurrent use by multiple
 * {@link Thread}s.</p>
 *
 * <h2>Design Notes</h2>
 *
 * <p>This class loosely models the <a
 * href="https://github.com/kubernetes/client-go/blob/9b03088ac34f23d8ac912f623f2ae73274c38ce8/tools/cache/reflector.go#L47">{@code
 * Reflector} type in the {@code tools/cache} package of the {@code
 * client-go} subproject of Kubernetes</a>.</p>
 *
 * @param <T> a type of Kubernetes resource
 *
 * @author <a href="https://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 *
 * @see EventCache
 */
@ThreadSafe
public class Reflector<T extends HasMetadata> implements Closeable {


  /*
   * Instance fields.
   */


  /**
   * The operation that was supplied at construction time.
   *
   * <p>This field is never {@code null}.</p>
   *
   * <p>It is guaranteed that the value of this field may be
   * assignable to a reference of type {@link Listable Listable&lt;?
   * extends KubernetesResourceList&gt;} or to a reference of type
   * {@link VersionWatchable VersionWatchable&lt;? extends Closeable,
   * Watcher&lt;T&gt;&gt;}.</p>
   *
   * @see Listable
   *
   * @see VersionWatchable
   */
  private final Object operation;

  /**
   * The resource version that a successful watch operation processed.
   *
   * @see #setLastSynchronizationResourceVersion(Object)
   *
   * @see WatchHandler#eventReceived(Watcher.Action, HasMetadata)
   */
  private volatile Object lastSynchronizationResourceVersion;

  /**
   * The {@link ScheduledExecutorService} in charge of scheduling
   * repeated invocations of the {@link #synchronize()} method.
   *
   * <p>This field may be {@code null}.</p>
   *
   * <h2>Thread Safety</h2>
   *
   * <p>This field is not safe for concurrent use by multiple threads
   * without explicit synchronization on it.</p>
   *
   * @see #synchronize()
   */
  @GuardedBy("this")
  private ScheduledExecutorService synchronizationExecutorService;

  /**
   * A {@link Function} that consumes a {@link Throwable} and returns
   * {@code true} if the error represented by that {@link Throwable}
   * was handled in some way.
   *
   * <p>This field may be {@code null}.</p>
   */
  private final Function<? super Throwable, Boolean> synchronizationErrorHandler;

  /**
   * A {@link ScheduledFuture} representing the task that is scheduled
   * to repeatedly invoke the {@link #synchronize()} method.
   *
   * <p>This field may be {@code null}.</p>
   *
   * <h2>Thread Safety</h2>
   *
   * <p>This field is not safe for concurrent use by multiple threads
   * without explicit synchronization on it.</p>
   *
   * @see #synchronize()
   */
  @GuardedBy("this")
  private ScheduledFuture<?> synchronizationTask;

  /**
   * A flag tracking whether the {@link
   * #synchronizationExecutorService} should be shut down when this
   * {@link Reflector} is {@linkplain #close() closed}.  If the
   * creator of this {@link Reflector} supplied an explicit {@link
   * ScheduledExecutorService} at construction time, then it will not
   * be shut down.
   */
  private final boolean shutdownSynchronizationExecutorServiceOnClose;

  /**
   * How many seconds to wait in between scheduled invocations of the
   * {@link #synchronize()} method.  If the value of this field is
   * less than or equal to zero then no synchronization will take
   * place.
   */
  private final long synchronizationIntervalInSeconds;

  /**
   * The watch operation currently in effect.
   *
   * <p>This field may be {@code null} at any point.</p>
   *
   * <h2>Thread Safety</h2>
   *
   * <p>This field is not safe for concurrent use by multiple threads
   * without explicit synchronization on it.</p>
   */
  @GuardedBy("this")
  private Closeable watch;

  /**
   * An {@link EventCache} (often an {@link EventQueueCollection})
   * whose contents will be added to to reflect the current state of
   * Kubernetes.
   *
   * <p>This field is never {@code null}.</p>
   */
  @GuardedBy("itself")
  private final EventCache<T> eventCache;

  /**
   * A {@link Logger} for use by this {@link Reflector}.
   *
   * <p>This field is never {@code null}.</p>
   *
   * @see #createLogger()
   */
  protected final Logger logger;


  /*
   * Constructors.
   */


  /**
   * Creates a new {@link Reflector}.
   *
   * @param <X> a type that is both an appropriate kind of {@link
   * Listable} and {@link VersionWatchable}, such as the kind of
   * operation returned by {@link
   * DefaultKubernetesClient#configMaps()} and the like
   *
   * @param operation a {@link Listable} and a {@link
   * VersionWatchable} that can report information from a Kubernetes
   * cluster; must not be {@code null}
   *
   * @param eventCache an {@link EventCache} <strong>that will be
   * synchronized on</strong> and into which {@link Event}s will be
   * logically "reflected"; must not be {@code null}
   *
   * @exception NullPointerException if {@code operation} or {@code
   * eventCache} is {@code null}
   *
   * @exception IllegalStateException if the {@link #createLogger()}
   * method returns {@code null}
   *
   * @see #Reflector(Listable, EventCache, ScheduledExecutorService,
   * Duration, Function)
   *
   * @see #start()
   */
  @SuppressWarnings("rawtypes") // kubernetes-client's implementations of KubernetesResourceList use raw types
  public <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> Reflector(final X operation,
                                                                                                                              final EventCache<T> eventCache) {
    this(operation, eventCache, null, null, null);
  }

  /**
   * Creates a new {@link Reflector}.
   *
   * @param <X> a type that is both an appropriate kind of {@link
   * Listable} and {@link VersionWatchable}, such as the kind of
   * operation returned by {@link
   * DefaultKubernetesClient#configMaps()} and the like
   *
   * @param operation a {@link Listable} and a {@link
   * VersionWatchable} that can report information from a Kubernetes
   * cluster; must not be {@code null}
   *
   * @param eventCache an {@link EventCache} <strong>that will be
   * synchronized on</strong> and into which {@link Event}s will be
   * logically "reflected"; must not be {@code null}
   *
   * @param synchronizationInterval a {@link Duration} representing
   * the time in between one {@linkplain EventCache#synchronize()
   * synchronization operation} and another; interpreted with a
   * granularity of seconds; may be {@code null} or semantically equal
   * to {@code 0} seconds in which case no synchronization will occur
   *
   * @exception NullPointerException if {@code operation} or {@code
   * eventCache} is {@code null}
   *
   * @exception IllegalStateException if the {@link #createLogger()}
   * method returns {@code null}
   *
   * @see #Reflector(Listable, EventCache, ScheduledExecutorService,
   * Duration, Function)
   *
   * @see #start()
   */
  @SuppressWarnings("rawtypes") // kubernetes-client's implementations of KubernetesResourceList use raw types
  public <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> Reflector(final X operation,
                                                                                                                              final EventCache<T> eventCache,
                                                                                                                              final Duration synchronizationInterval) {
    this(operation, eventCache, null, synchronizationInterval, null);
  }

  /**
   * Creates a new {@link Reflector}.
   *
   * @param <X> a type that is both an appropriate kind of {@link
   * Listable} and {@link VersionWatchable}, such as the kind of
   * operation returned by {@link
   * DefaultKubernetesClient#configMaps()} and the like
   *
   * @param operation a {@link Listable} and a {@link
   * VersionWatchable} that can report information from a Kubernetes
   * cluster; must not be {@code null}
   *
   * @param eventCache an {@link EventCache} <strong>that will be
   * synchronized on</strong> and into which {@link Event}s will be
   * logically "reflected"; must not be {@code null}
   *
   * @param synchronizationExecutorService a {@link
   * ScheduledExecutorService} to be used to tell the supplied {@link
   * EventCache} to {@linkplain EventCache#synchronize() synchronize}
   * on a schedule; may be {@code null} in which case no
   * synchronization will occur
   *
   * @param synchronizationInterval a {@link Duration} representing
   * the time in between one {@linkplain EventCache#synchronize()
   * synchronization operation} and another; may be {@code null} in
   * which case no synchronization will occur
   *
   * @exception NullPointerException if {@code operation} or {@code
   * eventCache} is {@code null}
   *
   * @exception IllegalStateException if the {@link #createLogger()}
   * method returns {@code null}
   *
   * @see #Reflector(Listable, EventCache, ScheduledExecutorService,
   * Duration, Function)
   *
   * @see #start()
   */
  @SuppressWarnings("rawtypes") // kubernetes-client's implementations of KubernetesResourceList use raw types
  public <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> Reflector(final X operation,
                                                                                                                              final EventCache<T> eventCache,
                                                                                                                              final ScheduledExecutorService synchronizationExecutorService,
                                                                                                                              final Duration synchronizationInterval) {
    this(operation, eventCache, synchronizationExecutorService, synchronizationInterval, null);
  }

  /**
   * Creates a new {@link Reflector}.
   *
   * @param <X> a type that is both an appropriate kind of {@link
   * Listable} and {@link VersionWatchable}, such as the kind of
   * operation returned by {@link
   * DefaultKubernetesClient#configMaps()} and the like
   *
   * @param operation a {@link Listable} and a {@link
   * VersionWatchable} that can report information from a Kubernetes
   * cluster; must not be {@code null}
   *
   * @param eventCache an {@link EventCache} <strong>that will be
   * synchronized on</strong> and into which {@link Event}s will be
   * logically "reflected"; must not be {@code null}
   *
   * @param synchronizationExecutorService a {@link
   * ScheduledExecutorService} to be used to tell the supplied {@link
   * EventCache} to {@linkplain EventCache#synchronize() synchronize}
   * on a schedule; may be {@code null} in which case no
   * synchronization will occur
   *
   * @param synchronizationInterval a {@link Duration} representing
   * the time in between one {@linkplain EventCache#synchronize()
   * synchronization operation} and another; may be {@code null} in
   * which case no synchronization will occur
   *
   * @param synchronizationErrorHandler a {@link Function} that
   * consumes a {@link Throwable} and returns a {@link Boolean}
   * indicating whether the error represented by the {@link Throwable}
   * in question was handled or not; may be {@code null}
   *
   * @exception NullPointerException if {@code operation} or {@code
   * eventCache} is {@code null}
   *
   * @exception IllegalStateException if the {@link #createLogger()}
   * method returns {@code null}
   *
   * @see #start()
   */
  @SuppressWarnings("rawtypes") // kubernetes-client's implementations of KubernetesResourceList use raw types
  public <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> Reflector(final X operation,
                                                                                                                              final EventCache<T> eventCache,
                                                                                                                              final ScheduledExecutorService synchronizationExecutorService,
                                                                                                                              final Duration synchronizationInterval,
                                                                                                                              final Function<? super Throwable, Boolean> synchronizationErrorHandler) {
    super();
    this.logger = this.createLogger();
    if (this.logger == null) {
      throw new IllegalStateException("createLogger() == null");
    }
    final String cn = this.getClass().getName();
    final String mn = "<init>";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { operation, eventCache, synchronizationExecutorService, synchronizationInterval });
    }
    Objects.requireNonNull(operation);
    this.eventCache = Objects.requireNonNull(eventCache);
    // TODO: research: maybe: operation.withField("metadata.resourceVersion", "0")?
    this.operation = operation.withResourceVersion("0");

    if (synchronizationInterval == null) {
      this.synchronizationIntervalInSeconds = 0L;
    } else {
      this.synchronizationIntervalInSeconds = synchronizationInterval.get(ChronoUnit.SECONDS);
    }
    if (this.synchronizationIntervalInSeconds <= 0L) {
      this.synchronizationExecutorService = null;
      this.shutdownSynchronizationExecutorServiceOnClose = false;
      this.synchronizationErrorHandler = null;
    } else {
      this.synchronizationExecutorService = synchronizationExecutorService;
      this.shutdownSynchronizationExecutorServiceOnClose = synchronizationExecutorService == null;
      if (synchronizationErrorHandler == null) {
        this.synchronizationErrorHandler = t -> {
          if (this.logger.isLoggable(Level.SEVERE)) {
            this.logger.logp(Level.SEVERE,
                             this.getClass().getName(), "<synchronizationTask>",
                             t.getMessage(), t);
          }
          return true;
        };
      } else {
        this.synchronizationErrorHandler = synchronizationErrorHandler;
      }
    }

    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }


  /*
   * Instance methods.
   */


  /**
   * Returns a {@link Logger} that will be used for this {@link
   * Reflector}.
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
   * Notionally closes this {@link Reflector} by terminating any
   * {@link Thread}s that it has started and invoking the {@link
   * #onClose()} method while holding this {@link Reflector}'s
   * monitor.
   *
   * @exception IOException if an error occurs
   *
   * @see #onClose()
   */
  @Override
  public synchronized final void close() throws IOException {
    final String cn = this.getClass().getName();
    final String mn = "close";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn);
    }
    
    try {
      this.closeSynchronizationExecutorService();
      if (this.watch != null) {
        this.watch.close();
      }
    } finally {
      this.onClose();
    }
    
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * {@linkplain Future#cancel(boolean) Cancels} scheduled invocations
   * of the {@link #synchronize()} method.
   *
   * <p>This method is invoked by the {@link
   * #closeSynchronizationExecutorService()} method.</p>
   *
   * @see #setUpSynchronization()
   *
   * @see #closeSynchronizationExecutorService()
   */
  private synchronized final void cancelSynchronization() {
    final String cn = this.getClass().getName();
    final String mn = "cancelSynchronization";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn);
    }
    
    if (this.synchronizationTask != null) {
      this.synchronizationTask.cancel(true /* interrupt the task */);
      this.synchronizationTask = null; // very important; see setUpSynchronization()
    }
    
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * {@linkplain #cancelSynchronization Cancels scheduled invocations
   * of the <code>synchronize()</code> method} and, when appropriate,
   * shuts down the {@link ScheduledExecutorService} responsible for
   * the scheduling.
   *
   * @see #cancelSynchronization()
   *
   * @see #setUpSynchronization()
   */
  private synchronized final void closeSynchronizationExecutorService() {
    final String cn = this.getClass().getName();
    final String mn = "closeSynchronizationExecutorService";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn);
    }

    this.cancelSynchronization();

    if (this.synchronizationExecutorService != null && this.shutdownSynchronizationExecutorServiceOnClose) {

      // Stop accepting new tasks.  Not that any will be showing up
      // anyway, but it's the right thing to do.
      this.synchronizationExecutorService.shutdown();

      try {
        if (!this.synchronizationExecutorService.awaitTermination(60L, TimeUnit.SECONDS)) {
          this.synchronizationExecutorService.shutdownNow();
          if (!this.synchronizationExecutorService.awaitTermination(60L, TimeUnit.SECONDS)) {
            if (this.logger.isLoggable(Level.WARNING)) {
              this.logger.logp(Level.WARNING,
                               cn, mn,
                               "synchronizationExecutorService did not terminate cleanly after 60 seconds");
            }
          }
        }
      } catch (final InterruptedException interruptedException) {
        this.synchronizationExecutorService.shutdownNow();
        Thread.currentThread().interrupt();
      }

    }

    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * As the name implies, sets up <em>synchronization</em>, which is
   * the act of the downstream event cache telling its associated
   * event listeners that there are items remaining to be processed,
   * and returns a {@link Future} reprsenting the scheduled, repeating
   * task.
   *
   * <p>This method schedules repeated invocations of the {@link
   * #synchronize()} method.</p>
   *
   * <p>This method may return {@code null}.</p>
   *
   * @return a {@link Future} representing the scheduled repeating
   * synchronization task, or {@code null} if no such task was
   * scheduled
   *
   * @see #synchronize()
   *
   * @see EventCache#synchronize()
   */
  private synchronized final Future<?> setUpSynchronization() {
    final String cn = this.getClass().getName();
    final String mn = "setUpSynchronization";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn);
    }

    if (this.synchronizationIntervalInSeconds > 0L) {
      if (this.synchronizationExecutorService == null || this.synchronizationExecutorService.isTerminated()) {
        this.synchronizationExecutorService = Executors.newScheduledThreadPool(1);
        if (this.synchronizationExecutorService instanceof ScheduledThreadPoolExecutor) {
          ((ScheduledThreadPoolExecutor)this.synchronizationExecutorService).setRemoveOnCancelPolicy(true);
        }
      }
      if (this.synchronizationTask == null) {
        if (this.logger.isLoggable(Level.INFO)) {
          this.logger.logp(Level.INFO,
                           cn, mn,
                           "Scheduling downstream synchronization every {0} seconds",
                           Long.valueOf(this.synchronizationIntervalInSeconds));
        }
        this.synchronizationTask = this.synchronizationExecutorService.scheduleWithFixedDelay(this::synchronize, 0L, this.synchronizationIntervalInSeconds, TimeUnit.SECONDS);
      }
      assert this.synchronizationExecutorService != null;
      assert this.synchronizationTask != null;
    }

    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn, this.synchronizationTask);
    }
    return this.synchronizationTask;
  }

  /**
   * Calls {@link EventCache#synchronize()} on this {@link
   * Reflector}'s {@linkplain #eventCache affiliated
   * <code>EventCache</code>}.
   *
   * <p>This method is normally invoked on a schedule by this {@link
   * Reflector}'s {@linkplain #synchronizationExecutorService
   * affiliated <code>ScheduledExecutorService</code>}.</p>
   *
   * @see #setUpSynchronization()
   *
   * @see #shouldSynchronize()
   */
  private final void synchronize() {
    final String cn = this.getClass().getName();
    final String mn = "synchronize";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn);
    }

    if (this.shouldSynchronize()) {
      if (this.logger.isLoggable(Level.FINE)) {
        this.logger.logp(Level.FINE,
                         cn, mn,
                         "Synchronizing event cache with its downstream consumers");
      }
      Throwable throwable = null;
      synchronized (this.eventCache) {
        try {

          // Tell the EventCache to run a synchronization operation.
          // This will have the effect of adding SynchronizationEvents
          // of type MODIFICATION to the EventCache.
          this.eventCache.synchronize();
          
        } catch (final Throwable e) {
          assert e instanceof RuntimeException || e instanceof Error;
          throwable = e;
        }
      }
      if (throwable != null && !this.synchronizationErrorHandler.apply(throwable)) {
        if (throwable instanceof RuntimeException) {
          throw (RuntimeException)throwable;
        } else if (throwable instanceof Error) {
          throw (Error)throwable;
        } else {
          assert !(throwable instanceof Exception) : "Signature changed for EventCache#synchronize()";
        }
      }
    }

    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * Returns whether, at any given moment, this {@link Reflector}
   * should cause its {@link EventCache} to {@linkplain
   * EventCache#synchronize() synchronize}.
   *
   * <p>The default implementation of this method returns {@code true}
   * if this {@link Reflector} was constructed with an explicit
   * synchronization interval or {@link ScheduledExecutorService} or
   * both.</p>
   *
   * <h2>Design Notes</h2>
   *
   * <p>This code follows the Go code in the Kubernetes {@code
   * client-go/tools/cache} package.  One thing that becomes clear
   * when looking at all of this through an object-oriented lens is
   * that it is the {@link EventCache} (the {@code delta_fifo}, in the
   * Go code) that is ultimately in charge of synchronizing.  It is
   * not clear why in the Go code this is a function of a reflector.
   * In an object-oriented world, perhaps the {@link EventCache}
   * itself should be in charge of resynchronization schedules, but we
   * choose to follow the Go code's division of responsibilities
   * here.</p>
   *
   * @return {@code true} if this {@link Reflector} should cause its
   * {@link EventCache} to {@linkplain EventCache#synchronize()
   * synchronize}; {@code false} otherwise
   */
  protected boolean shouldSynchronize() {
    final String cn = this.getClass().getName();
    final String mn = "shouldSynchronize";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn);
    }
    final boolean returnValue;
    synchronized (this) {
      returnValue = this.synchronizationExecutorService != null;
    }
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn, Boolean.valueOf(returnValue));
    }
    return returnValue;
  }

  // Not used; not used in the Go code either?!
  private final Object getLastSynchronizationResourceVersion() {
    return this.lastSynchronizationResourceVersion;
  }

  /**
   * Records the last resource version processed by a successful watch
   * operation.
   *
   * @param resourceVersion the resource version in question; may be
   * {@code null}
   *
   * @see WatchHandler#eventReceived(Watcher.Action, HasMetadata)
   */
  private final void setLastSynchronizationResourceVersion(final Object resourceVersion) {
    // lastSynchronizationResourceVersion is volatile; this is an
    // atomic assignment
    this.lastSynchronizationResourceVersion = resourceVersion;
  }

  /**
   * Using the {@code operation} supplied at construction time,
   * {@linkplain Listable#list() lists} appropriate Kubernetes
   * resources, and then, on a separate {@link Thread}, {@linkplain
   * VersionWatchable sets up a watch} on them, calling {@link
   * EventCache#replace(Collection, Object)} and {@link
   * EventCache#add(Object, AbstractEvent.Type, HasMetadata)} methods
   * as appropriate.
   *
   * <p><strong>For convenience only</strong>, this method returns a
   * {@link Future} representing any scheduled synchronization task
   * created as a result of the user's having supplied a {@link
   * Duration} at construction time.  The return value may be (and
   * usually is) safely ignored.  Invoking {@link
   * Future#cancel(boolean)} on the returned {@link Future} will
   * result in the scheduled synchronization task being cancelled
   * irrevocably.  <strong>Notably, invoking {@link
   * Future#cancel(boolean)} on the returned {@link Future} will
   * <em>not</em> {@linkplain #close() close} this {@link
   * Reflector}.</strong>
   *
   * <p>This method never returns {@code null}.</p>
   *
   * <p>The calling {@link Thread} is not blocked by invocations of
   * this method.</p>
   *
   * <h2>Implementation Notes</h2>
   *
   * <p>This method loosely models the <a
   * href="https://github.com/kubernetes/client-go/blob/dcf16a0f3b52098c3d4c1467b6c80c3e88ff65fb/tools/cache/reflector.go#L128-L137">{@code
   * Run} function in {@code reflector.go} together with the {@code
   * ListAndWatch} function in the same file</a>.</p>
   *
   * @return a {@link Future} representing a scheduled synchronization
   * operation; never {@code null}
   *
   * @exception IOException if a watch has previously been established
   * and could not be {@linkplain Watch#close() closed}
   *
   * @exception KubernetesClientException if the initial attempt to
   * {@linkplain Listable#list() list} Kubernetes resources fails
   *
   * @see #close()
   */
  @NonBlocking
  public final Future<?> start() throws IOException {
    final String cn = this.getClass().getName();
    final String mn = "start";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn);
    }

    Future<?> returnValue = null;
    synchronized (this) {

      try {

        // If somehow we got called while a watch already exists, then
        // close the old watch (we'll replace it).  Note that,
        // critically, the onClose() method of our watch handler sets
        // this reference to null, so if the watch is in the process
        // of being closed, this little block won't be executed.
        if (this.watch != null) {
          final Closeable watch = this.watch;
          this.watch = null;
          if (logger.isLoggable(Level.FINE)) {
            logger.logp(Level.FINE,
                        cn, mn,
                        "Closing pre-existing watch");
          }
          watch.close();
          if (logger.isLoggable(Level.FINE)) {
            logger.logp(Level.FINE,
                        cn, mn,
                        "Closed pre-existing watch");
          }
        }

        // Run a list operation, and get the resourceVersion of that list.
        if (logger.isLoggable(Level.FINE)) {
          logger.logp(Level.FINE,
                      cn, mn,
                      "Listing Kubernetes resources using {0}", this.operation);
        }
        @Issue(id = "13", uri = "https://github.com/microbean/microbean-kubernetes-controller/issues/13")
        @SuppressWarnings("unchecked")
        final KubernetesResourceList<? extends T> list = ((Listable<? extends KubernetesResourceList<? extends T>>)this.operation).list();
        assert list != null;

        final ListMeta metadata = list.getMetadata();
        assert metadata != null;

        final String resourceVersion = metadata.getResourceVersion();
        assert resourceVersion != null;

        // Using the results of that list operation, do a full replace
        // on the EventCache with them.
        final Collection<? extends T> replacementItems;
        final Collection<? extends T> items = list.getItems();
        if (items == null || items.isEmpty()) {
          replacementItems = Collections.emptySet();
        } else {
          replacementItems = Collections.unmodifiableCollection(new ArrayList<>(items));
        }
        
        if (logger.isLoggable(Level.FINE)) {
          logger.logp(Level.FINE, cn, mn, "Replacing resources in the event cache");
        }
        synchronized (this.eventCache) {
          this.eventCache.replace(replacementItems, resourceVersion);
        }
        if (logger.isLoggable(Level.FINE)) {
          logger.logp(Level.FINE, cn, mn, "Done replacing resources in the event cache");
        }

        // Record the resource version we captured during our list
        // operation.
        this.setLastSynchronizationResourceVersion(resourceVersion);

        // Now that we've vetted that our list operation works (i.e. no
        // syntax errors, no connectivity problems) we can schedule
        // synchronizations if necessary.
        //
        // A synchronization is an operation where, if allowed, our
        // eventCache goes through its set of known objects and--for
        // any that are not enqueued for further processing
        // already--fires a *synchronization* event of type
        // MODIFICATION.  This happens on a schedule, not in reaction
        // to an event.  This allows its downstream processors a
        // chance to try to bring system state in line with desired
        // state, even if no events have occurred (kind of like a
        // heartbeat).  See
        // https://engineering.bitnami.com/articles/a-deep-dive-into-kubernetes-controllers.html#resyncperiod.
        this.setUpSynchronization();
        returnValue = this.synchronizationTask;

        // If there wasn't a synchronizationTask, then that means the
        // user who created this Reflector didn't want any
        // synchronization to happen.  We return a "dummy" Future that
        // is already "completed" (isDone() returns true) to avoid
        // having to return null.  The returned Future can be
        // cancelled with no effect.
        if (returnValue == null) {
          final FutureTask<?> futureTask = new FutureTask<Void>(() -> {}, null);
          futureTask.run(); // just sets "doneness"
          assert futureTask.isDone();
          assert !futureTask.isCancelled();
          returnValue = futureTask;
        }

        assert returnValue != null;

        // Now that we've taken care of our list() operation, set up our
        // watch() operation.
        if (logger.isLoggable(Level.FINE)) {
          logger.logp(Level.FINE,
                      cn, mn,
                      "Watching Kubernetes resources with resource version {0} using {1}",
                      new Object[] { resourceVersion, this.operation });
        }
        @SuppressWarnings("unchecked")
        final Versionable<? extends Watchable<? extends Closeable, Watcher<T>>> versionableOperation =
          (Versionable<? extends Watchable<? extends Closeable, Watcher<T>>>)this.operation;
        this.watch = versionableOperation.withResourceVersion(resourceVersion).watch(new WatchHandler());
        if (logger.isLoggable(Level.FINE)) {
          logger.logp(Level.FINE,
                      cn, mn,
                      "Established watch: {0}", this.watch);
        }

      } catch (final IOException | RuntimeException | Error exception) {
        this.cancelSynchronization();
        if (this.watch != null) {
          try {
            // TODO: haven't seen it, but reason hard about deadlock
            // here; see
            // WatchHandler#onClose(KubernetesClientException) which
            // *can* call start() (this method) with the monitor.  I
            // *think* we're in the clear here:
            // onClose(KubernetesClientException) will only (re-)call
            // start() if the supplied KubernetesClientException is
            // non-null.  In this case, it should be, because this is
            // an ordinary close() call.
            this.watch.close();
          } catch (final Throwable suppressMe) {
            exception.addSuppressed(suppressMe);
          }
          this.watch = null;
        }
        throw exception;
      }
    }
    
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn, returnValue);
    }
    return returnValue;
  }

  /**
   * Invoked when {@link #close()} is invoked.
   *
   * <p>The default implementation of this method does nothing.</p>
   *
   * <p>Overrides of this method must consider that they will be
   * invoked with this {@link Reflector}'s monitor held.</p>
   *
   * <p>Overrides of this method must not call the {@link #close()}
   * method.</p>
   *
   * @see #close()
   */
  protected synchronized void onClose() {

  }


  /*
   * Inner and nested classes.
   */


  /**
   * A {@link Watcher} of Kubernetes resources.
   *
   * @author <a href="https://about.me/lairdnelson"
   * target="_parent">Laird Nelson</a>
   *
   * @see Watcher
   */
  private final class WatchHandler implements Watcher<T> {


    /*
     * Constructors.
     */


    /**
     * Creates a new {@link WatchHandler}.
     */
    private WatchHandler() {
      super();
      final String cn = this.getClass().getName();
      final String mn = "<init>";
      if (logger.isLoggable(Level.FINER)) {
        logger.entering(cn, mn);
        logger.exiting(cn, mn);
      }
    }


    /*
     * Instance methods.
     */


    /**
     * Calls the {@link EventCache#add(Object, AbstractEvent.Type,
     * HasMetadata)} method on the enclosing {@link Reflector}'s
     * associated {@link EventCache} with information harvested from
     * the supplied {@code resource}, and using an {@link
     * AbstractEvent.Type} selected appropriately given the supplied
     * {@link Watcher.Action}.
     *
     * @param action the kind of Kubernetes event that happened; must
     * not be {@code null}
     *
     * @param resource the {@link HasMetadata} object that was
     * affected; must not be {@code null}
     *
     * @exception NullPointerException if {@code action} or {@code
     * resource} was {@code null}
     *
     * @exception IllegalStateException if another error occurred
     */
    @Override
    public final void eventReceived(final Watcher.Action action, final T resource) {
      final String cn = this.getClass().getName();
      final String mn = "eventReceived";
      if (logger.isLoggable(Level.FINER)) {
        logger.entering(cn, mn, new Object[] { action, resource });
      }
      Objects.requireNonNull(action);
      Objects.requireNonNull(resource);

      final ObjectMeta metadata = resource.getMetadata();
      assert metadata != null;

      final Event.Type eventType;
      switch (action) {
      case ADDED:
        eventType = Event.Type.ADDITION;
        break;
      case MODIFIED:
        eventType = Event.Type.MODIFICATION;
        break;
      case DELETED:
        eventType = Event.Type.DELETION;
        break;
      case ERROR:
        // Uh...the Go code has:
        //
        //   if event.Type == watch.Error {
        //     return apierrs.FromObject(event.Object)
        //   }
        //
        // Now, apierrs.FromObject is here:
        // https://github.com/kubernetes/apimachinery/blob/kubernetes-1.9.2/pkg/api/errors/errors.go#L80-L88
        // This is looking for a Status object.  But
        // WatchConnectionHandler will never forward on such a thing:
        // https://github.com/fabric8io/kubernetes-client/blob/v3.1.8/kubernetes-client/src/main/java/io/fabric8/kubernetes/client/dsl/internal/WatchConnectionManager.java#L246-L258
        //
        // So it follows that if by some chance we get here, resource
        // will definitely be a HasMetadata.  We go back to the Go
        // code again, and remember that if the type is Error, the
        // equivalent of this watch handler simply returns and goes home.
        //
        // Now, if we were to throw a RuntimeException here, which is
        // the idiomatic equivalent of returning and going home, this
        // would cause a watch reconnect:
        // https://github.com/fabric8io/kubernetes-client/blob/v3.1.8/kubernetes-client/src/main/java/io/fabric8/kubernetes/client/dsl/internal/WatchConnectionManager.java#L159-L205
        // ...up to the reconnect limit.
        //
        // ...which is fine, but I'm not sure that in an error case a
        // WatchEvent will ever HAVE a HasMetadata as its payload.
        // Which means MAYBE we'll never get here.  But if we do, all
        // we can do is throw a RuntimeException...which ends up
        // reducing to the same case as the default case below, so we
        // fall through.
      default:
        eventType = null;
        throw new IllegalStateException();
      }
      assert eventType != null;

      // Add an Event of the proper kind to our EventCache.  This is
      // the heart of this method.
      if (logger.isLoggable(Level.FINE)) {
        logger.logp(Level.FINE,
                    cn, mn,
                    "Adding event to cache: {0} {1}", new Object[] { eventType, resource });
      }
      synchronized (eventCache) {
        eventCache.add(Reflector.this, eventType, resource);
      }

      // Record the most recent resource version we're tracking to be
      // that of this last successful watch() operation.  We set it
      // earlier during a list() operation.
      setLastSynchronizationResourceVersion(metadata.getResourceVersion());

      if (logger.isLoggable(Level.FINER)) {
        logger.exiting(cn, mn);
      }
    }

    /**
     * Invoked when the Kubernetes client connection closes.
     *
     * @param exception any {@link KubernetesClientException} that
     * caused this closing to happen; may be {@code null}
     */
    @Override
    public final void onClose(final KubernetesClientException exception) {
      final String cn = this.getClass().getName();
      final String mn = "onClose";
      if (logger.isLoggable(Level.FINER)) {
        logger.entering(cn, mn, exception);
      }

      synchronized (Reflector.this) {
        // Don't close Reflector.this.watch before setting it to null
        // here; after all we're being called because it's in the
        // process of closing already!
        Reflector.this.watch = null;
      }

      if (exception != null) {
        if (logger.isLoggable(Level.WARNING)) {
          logger.logp(Level.WARNING,
                      cn, mn,
                      exception.getMessage(), exception);
        }
        // See
        // https://github.com/kubernetes/client-go/blob/5f85fe426e7aa3c1df401a7ae6c1ba837bd76be9/tools/cache/reflector.go#L204.
        if (logger.isLoggable(Level.INFO)) {
          logger.logp(Level.INFO, cn, mn, "Restarting Reflector");
        }
        try {
          Reflector.this.start();
        } catch (final Throwable suppressMe) {
          if (logger.isLoggable(Level.SEVERE)) {
            logger.logp(Level.SEVERE,
                        cn, mn,
                        "Failed to restart Reflector", suppressMe);
          }
          exception.addSuppressed(suppressMe);
        }
      }

      if (logger.isLoggable(Level.FINER)) {
        logger.exiting(cn, mn, exception);
      }
    }

  }

}
