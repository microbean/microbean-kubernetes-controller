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

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;

import io.fabric8.kubernetes.client.dsl.Listable;
import io.fabric8.kubernetes.client.dsl.VersionWatchable;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ListMeta;

import org.microbean.development.annotation.NonBlocking;

/**
 * 
 */
public class Reflector<T extends HasMetadata> implements Closeable {

  private final Object operation;

  private volatile Object lastSynchronizationResourceVersion;

  private final ScheduledExecutorService synchronizationExecutorService;

  private ScheduledFuture<?> synchronizationTask;

  private final boolean shutdownSynchronizationExecutorServiceOnClose;

  private final Duration synchronizationInterval;

  private Closeable watch;

  private final EventCache<T> eventCache;

  @SuppressWarnings("rawtypes") // kubernetes-client's implementations of KubernetesResourceList use raw types
  public <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> Reflector(final X operation,
                                                                                                                              final EventCache<T> eventCache) {
    this(operation, eventCache, null, null);
  }
  
  @SuppressWarnings("rawtypes") // kubernetes-client's implementations of KubernetesResourceList use raw types
  public <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> Reflector(final X operation,
                                                                                                                              final EventCache<T> eventCache,
                                                                                                                              final Duration synchronizationInterval) {
    this(operation, eventCache, null, synchronizationInterval);
  }
  
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

  @NonBlocking
  public synchronized final void start() {
    if (this.watch == null) {
      @SuppressWarnings("unchecked")
      final KubernetesResourceList<? extends T> list = ((Listable<? extends KubernetesResourceList<? extends T>>)operation).list();
      assert list != null;
      
      final ListMeta metadata = list.getMetadata();
      assert metadata != null;
      
      final String resourceVersion = metadata.getResourceVersion();
      assert resourceVersion != null;
      
      final Collection<? extends T> replacementItems;
      final Collection<? extends T> items = list.getItems();
      if (items == null || items.isEmpty()) {
        replacementItems = Collections.emptySet();
      } else {
        replacementItems = Collections.unmodifiableCollection(new ArrayList<>(items));
      }
      synchronized (eventCache) {
        eventCache.replace(replacementItems, resourceVersion);
      }
      
      setLastSynchronizationResourceVersion(resourceVersion);
      
      if (synchronizationExecutorService != null) {
        
        final Duration synchronizationDuration = getSynchronizationInterval();
        final long seconds;
        if (synchronizationDuration == null) {
          seconds = 0L;
        } else {
          seconds = synchronizationDuration.get(ChronoUnit.SECONDS);
        }
        
        if (seconds > 0L) {
          final ScheduledFuture<?> job = synchronizationExecutorService.scheduleWithFixedDelay(() -> {
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
          synchronizationTask = job;
        }
        
      }

      try {
        @SuppressWarnings("unchecked")
        final Closeable temp = ((VersionWatchable<? extends Closeable, Watcher<T>>)operation).withResourceVersion(resourceVersion).watch(new WatchHandler());
        assert temp != null;
        watch = temp;
      } finally {
        this.closeSynchronizationExecutorService();
      }
    }
  }

  protected void onClose() {

  }
  

  /*
   * Inner and nested classes.
   */
  

  private final class WatchHandler implements Watcher<T> {

    private WatchHandler() {
      super();
    }
    
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
      if (eventType != null) {
        synchronized (eventCache) {
          eventCache.add(Reflector.this, eventType, resource);
        }
      }
      setLastSynchronizationResourceVersion(metadata.getResourceVersion());
    }

    @Override
    public final void onClose(final KubernetesClientException exception) {
      // Note that exception can be null.
    }
    
  }
  
}
