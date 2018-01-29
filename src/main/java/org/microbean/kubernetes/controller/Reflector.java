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

import java.time.temporal.ChronoUnit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Map;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.fabric8.kubernetes.client.DefaultKubernetesClient; // for javadoc only
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;

import io.fabric8.kubernetes.client.dsl.Listable;
import io.fabric8.kubernetes.client.dsl.VersionWatchable;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ListMeta;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.microbean.development.annotation.NonBlocking;

/**
 * A pump of sorts that continuously "pulls" logical events out of
 * Kubernetes and {@linkplain EventCache#add(Object, Event.Type,
 * HasMetadata) adds them} to an {@link EventCache} so as to logically
 * "reflect" the contents of Kubernetes into the cache.
 *
 * <h2>Thread Safety</h2>
 *
 * <p>Instances of this class are safe for concurrent use by multiple
 * {@link Thread}s.</p>
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
   * The resource version 
   */
  private volatile Object lastSynchronizationResourceVersion;

  private final ScheduledExecutorService synchronizationExecutorService;

  @GuardedBy("this")
  private ScheduledFuture<?> synchronizationTask;

  private final boolean shutdownSynchronizationExecutorServiceOnClose;

  private final Duration synchronizationInterval;

  @GuardedBy("this")
  private Closeable watch;

  @GuardedBy("itself")
  private final EventCache<T> eventCache;


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
   * @see #start()
   */
  @SuppressWarnings("rawtypes") // kubernetes-client's implementations of KubernetesResourceList use raw types
  public <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> Reflector(final X operation,
                                                                                                                              final EventCache<T> eventCache) {
    this(operation, eventCache, null, null);
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
   * synchronization operation} and another; may be {@code null} in
   * which case no synchronization will occur
   *
   * @see #start()
   */
  @SuppressWarnings("rawtypes") // kubernetes-client's implementations of KubernetesResourceList use raw types
  public <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> Reflector(final X operation,
                                                                                                                              final EventCache<T> eventCache,
                                                                                                                              final Duration synchronizationInterval) {
    this(operation, eventCache, null, synchronizationInterval);
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
   * @see #start()
   */
  @SuppressWarnings("rawtypes") // kubernetes-client's implementations of KubernetesResourceList use raw types
  public <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> Reflector(final X operation,
                                                                                                                              final EventCache<T> eventCache,
                                                                                                                              final ScheduledExecutorService synchronizationExecutorService,
                                                                                                                              final Duration synchronizationInterval) {
    super();
    Objects.requireNonNull(operation);
    this.eventCache = Objects.requireNonNull(eventCache);
    // TODO: research: maybe: operation.withField("metadata.resourceVersion", "0")?    
    this.operation = operation.withResourceVersion("0");
    this.synchronizationInterval = synchronizationInterval;
    if (synchronizationExecutorService == null) {
      if (synchronizationInterval == null) {
        this.synchronizationExecutorService = null;
        this.shutdownSynchronizationExecutorServiceOnClose = false;
      } else {
        this.synchronizationExecutorService = Executors.newScheduledThreadPool(1);
        this.shutdownSynchronizationExecutorServiceOnClose = true;
      }
    } else {
      this.synchronizationExecutorService = synchronizationExecutorService;
      this.shutdownSynchronizationExecutorServiceOnClose = false;
    }
  }


  /*
   * Instance methods.
   */
  

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
    try {
      final ScheduledFuture<?> synchronizationTask = this.synchronizationTask;
      if (synchronizationTask != null) {
        synchronizationTask.cancel(false);
      }
      this.closeSynchronizationExecutorService();
      if (this.watch != null) {
        this.watch.close();
      }
    } finally {
      this.onClose();
    }
  }

  private synchronized final void closeSynchronizationExecutorService() {
    if (this.synchronizationExecutorService != null && this.shutdownSynchronizationExecutorServiceOnClose) {
      this.synchronizationExecutorService.shutdown();
      try {
        if (!this.synchronizationExecutorService.awaitTermination(60L, TimeUnit.SECONDS)) {
          this.synchronizationExecutorService.shutdownNow();
          if (!this.synchronizationExecutorService.awaitTermination(60L, TimeUnit.SECONDS)) {
            this.synchronizationExecutorService.shutdownNow();
          }
        }
      } catch (final InterruptedException interruptedException) {
        this.synchronizationExecutorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  private synchronized final void setUpSynchronization() {
    if (this.synchronizationExecutorService != null) {
      
      final Duration synchronizationDuration = this.getSynchronizationInterval();
      final long seconds;
      if (synchronizationDuration == null) {
        seconds = 0L;
      } else {
        seconds = synchronizationDuration.get(ChronoUnit.SECONDS);
      }
        
      if (seconds > 0L) {
        final ScheduledFuture<?> job = this.synchronizationExecutorService.scheduleWithFixedDelay(() -> {
            try {
              if (shouldSynchronize()) {
                synchronized (eventCache) {
                  eventCache.synchronize();
                }
              }
            } catch (final RuntimeException runtimeException) {
              // TODO: log or...?
              throw runtimeException;
            }
          }, 0L, seconds, TimeUnit.SECONDS);
        assert job != null;
        this.synchronizationTask = job;
      }
      
    }
  }

  /**
   * Whether, at any given moment, this {@link Reflector} should cause
   * its {@link EventCache} to {@linkplain EventCache#synchronize()
   * synchronize}.
   *
   * <h2>Design Notes</h2>
   *
   * <p>This code follows the Go code in the Kubernetes {@code
   * client-go/tools/cache} package.  One thing that becomes clear
   * when looking at all of this through an object-oriented lens is
   * that it is the {@link EventCache} (the {@code delta_fifo}, in the
   * Go code) that is ultimately in charge of synchronizing.  It is
   * not clear why this is a function of a reflector.  In an
   * object-oriented world, perhaps the {@link EventCache} itself
   * should be in charge of resynchronization schedules.</p>
   *
   * @return {@code true} if this {@link Reflector} should cause its
   * {@link EventCache} to {@linkplain EventCache#synchronize()
   * synchronize}; {@code false} otherwise
   */
  protected boolean shouldSynchronize() {
    return this.synchronizationExecutorService != null;
  }
  
  private final Duration getSynchronizationInterval() {
    return this.synchronizationInterval;
  }

  private final Object getLastSynchronizationResourceVersion() {
    return this.lastSynchronizationResourceVersion;
  }
  
  private final void setLastSynchronizationResourceVersion(final Object resourceVersion) {
    this.lastSynchronizationResourceVersion = resourceVersion;
  }

  /**
   * Using the {@code operation} supplied at construction time,
   * {@linkplain Listable#list() lists} appropriate Kubernetes
   * resources, and then, on a separate {@link Thread}, {@linkplain
   * VersionWatchable sets up a watch} on them, calling {@link
   * EventCache#replace(Collection, Object)} and {@link
   * EventCache#add(Object, Event.Type, HasMetadata)} methods as
   * appropriate.
   *
   * <p>The calling {@link Thread} is not blocked by invocations of
   * this method.</p>
   *
   * @see #close()
   */
  @NonBlocking
  public synchronized final void start() {
    if (this.watch == null) {

      // Run a list operation, and get the resourceVersion of that list.
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
      synchronized (eventCache) {
        this.eventCache.replace(replacementItems, resourceVersion);
      }

      // Record the resource version we captured during our list
      // operation.
      this.setLastSynchronizationResourceVersion(resourceVersion);

      // Now that we've vetted that our list operation works (i.e. no
      // syntax errors, no connectivity problems) we can schedule
      // resynchronizations if necessary.
      this.setUpSynchronization();

      // Now that we've taken care of our list() operation, set up our
      // watch() operation.
      try {
        @SuppressWarnings("unchecked")
        final Closeable temp = ((VersionWatchable<? extends Closeable, Watcher<T>>)operation).withResourceVersion(resourceVersion).watch(new WatchHandler());
        assert temp != null;
        this.watch = temp;
      } finally {
        this.closeSynchronizationExecutorService();
      }
    }
  }

  /**
   * Invoked when {@link #close()} is invoked.
   *
   * <p>The default implementation of this method does nothing.</p>
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
    }


    /*
     * Instance methods.
     */
    

    /**
     * Calls the {@link EventCache#add(Object, Event.Type,
     * HasMetadata)} method on the enclosing {@link Reflector}'s
     * associated {@link EventCache} with information harvested from
     * the supplied {@code resource}, and using an {@link Event.Type}
     * selected appropriately given the supplied {@link
     * Watcher.Action}.
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
        // TODO: Uh...the Go code has:
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

      // Add an Event of the proper kind to our EventCache.
      if (eventType != null) {
        synchronized (eventCache) {
          eventCache.add(Reflector.this, eventType, resource);
        }
      }

      // Record the most recent resource version we're tracking to be
      // that of this last successful watch() operation.  We set it
      // earlier during a list() operation.
      setLastSynchronizationResourceVersion(metadata.getResourceVersion());
    }

    /**
     * Invoked when the Kubernetes client connection closes.
     *
     * @param exception any {@link KubernetesClientException} that
     * caused this closing to happen; may be {@code null}
     */
    @Override
    public final void onClose(final KubernetesClientException exception) {
      // Note that exception can be null.
    }
    
  }
  
}
