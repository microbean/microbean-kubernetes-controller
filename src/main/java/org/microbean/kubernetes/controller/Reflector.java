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

public class Reflector<T extends HasMetadata> implements Closeable {

  private final Object operation;

  private volatile Object lastSyncResourceVersion;

  private final ScheduledExecutorService resyncExecutorService;

  private volatile ScheduledFuture<?> resyncTask;

  private final boolean shutdownResyncExecutorServiceOnClose;

  private final Duration resyncInterval;

  private volatile Closeable watch;

  private final EventCache<T> eventQueues;
  
  @SuppressWarnings("rawtypes") // kubernetes-client's implementations of KubernetesResourceList use raw types
  public <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> Reflector(final X operation,
                                                                                                                              final EventCache<T> eventCache,
                                                                                                                              final ScheduledExecutorService resyncExecutorService,
                                                                                                                              final Duration resyncInterval) {
    super();
    Objects.requireNonNull(operation);
    this.eventQueues = Objects.requireNonNull(eventCache);
    // TODO: research: maybe: operation.withField("metadata.resourceVersion", "0")?    
    this.operation = operation.withResourceVersion("0");
    this.resyncInterval = resyncInterval;
    if (resyncExecutorService == null) {
      if (resyncInterval == null) {
        this.resyncExecutorService = null;
        this.shutdownResyncExecutorServiceOnClose = false;
      } else {
        this.resyncExecutorService = Executors.newScheduledThreadPool(1);
        this.shutdownResyncExecutorServiceOnClose = true;
      }
    } else {
      this.resyncExecutorService = resyncExecutorService;
      this.shutdownResyncExecutorServiceOnClose = false;
    }
  }

  @Override
  public synchronized final void close() throws IOException {
    try {
      final ScheduledFuture<?> resyncTask = this.resyncTask;
      if (resyncTask != null) {
        resyncTask.cancel(false);
      }
      this.closeResyncExecutorService();
      if (this.watch != null) {
        this.watch.close();
      }
    } finally {
      this.onClose();
    }
  }

  private synchronized final void closeResyncExecutorService() {
    if (this.resyncExecutorService != null && this.shutdownResyncExecutorServiceOnClose) {
      this.resyncExecutorService.shutdown();
      try {
        if (!this.resyncExecutorService.awaitTermination(60L, TimeUnit.SECONDS)) {
          this.resyncExecutorService.shutdownNow();
          if (!this.resyncExecutorService.awaitTermination(60L, TimeUnit.SECONDS)) {
            this.resyncExecutorService.shutdownNow();
          }
        }
      } catch (final InterruptedException interruptedException) {
        this.resyncExecutorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  protected boolean shouldResync() {
    return this.resyncExecutorService != null;
  }
  
  private final Duration getResyncInterval() {
    return this.resyncInterval;
  }

  private final Object getLastSyncResourceVersion() {
    return this.lastSyncResourceVersion;
  }
  
  private final void setLastSyncResourceVersion(final Object resourceVersion) {
    this.lastSyncResourceVersion = resourceVersion;
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
      synchronized (eventQueues) {
        eventQueues.replace(replacementItems, resourceVersion);
      }
      
      setLastSyncResourceVersion(resourceVersion);
      
      if (resyncExecutorService != null) {
        
        final Duration resyncDuration = getResyncInterval();
        final long seconds;
        if (resyncDuration == null) {
          seconds = 0L;
        } else {
          seconds = resyncDuration.get(ChronoUnit.SECONDS);
        }
        
        if (seconds > 0L) {
          final ScheduledFuture<?> job = resyncExecutorService.scheduleWithFixedDelay(() -> {
              try {
                if (shouldResync()) {
                  synchronized (eventQueues) {
                    eventQueues.resync();
                  }
                }
              } catch (final RuntimeException runtimeException) {
                // TODO: log or...?
                throw runtimeException;
              }
            }, 0L, seconds, TimeUnit.SECONDS);
          assert job != null;
          resyncTask = job;
        }
        
      }

      try {
        @SuppressWarnings("unchecked")
          final Closeable temp = ((VersionWatchable<? extends Closeable, Watcher<T>>)operation).withResourceVersion(resourceVersion).watch(new WatchHandler());
        assert temp != null;
        watch = temp;
      } finally {
        this.closeResyncExecutorService();
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
      final Object newResourceVersion = metadata.getResourceVersion();
      final Event<T> event;
      switch (action) {
      case ADDED:
        event = new Event<>(Reflector.this, Event.Type.ADDITION, null, resource);
        break;
      case MODIFIED:
        event = new Event<>(Reflector.this, Event.Type.MODIFICATION, null, resource);
        break;
      case DELETED:
        event = new Event<>(Reflector.this, Event.Type.DELETION, null, resource);
        break;
      case ERROR:
        event = null;
        throw new IllegalStateException();
      default:
        event = null;
        throw new IllegalStateException();
      }
      synchronized (eventQueues) {
        eventQueues.add(event);
      }
      setLastSyncResourceVersion(newResourceVersion);
    }

    @Override
    public final void onClose(final KubernetesClientException exception) {
      // Note that exception can be null.
    }
    
  }
  
}
