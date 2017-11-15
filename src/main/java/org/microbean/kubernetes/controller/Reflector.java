/* -*- mode: Java; c-basic-offset: 2; indent-tabs-mode: nil; coding: utf-8-unix -*-
 *
 * Copyright © 2017 MicroBean.
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

import java.time.Duration;

import java.time.temporal.ChronoUnit;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ListMeta;

public class Reflector<T extends HasMetadata, L extends KubernetesResourceList, D> implements Closeable, Runnable {

  private final MixedOperation<T, L, ?, ?> operation;

  private final Store<T, D> store;
  
  private volatile String lastSyncResourceVersion;

  private final ScheduledExecutorService resyncExecutorService;

  private final Duration resyncInterval;

  private Watch watch;
  
  public Reflector(final Store<T, D> store,
                   final MixedOperation<T, L, ?, ?> operation,
                   final ScheduledExecutorService resyncExecutorService,
                   final Duration resyncInterval) {
    super();
    this.store = Objects.requireNonNull(store);
    // Or maybe: operation.withField("metadata.resourceVersion", "0")?
    @SuppressWarnings("unchecked")
    final MixedOperation<T, L, ?, ?> temp = (MixedOperation<T, L, ?, ?>)operation.withResourceVersion("0");
    this.operation = temp;
    this.resyncExecutorService = resyncExecutorService;
    this.resyncInterval = Objects.requireNonNull(resyncInterval);
  }

  public final MixedOperation<T, L, ?, ?> getOperation() {
    return this.operation;
  }

  public final Duration getResyncInterval() {
    return this.resyncInterval;
  }

  public String getLastSyncResourceVersion() {
    return this.lastSyncResourceVersion;
  }
  
  public void setLastSyncResourceVersion(final String resourceVersion) {
    this.lastSyncResourceVersion = resourceVersion;
  }

  protected boolean shouldResync() {
    return this.resyncExecutorService != null;
  }

  // Models the ListAndWatch func in the Go code.
  public void run() {
    @SuppressWarnings("unchecked")
    final KubernetesResourceList<? extends T> list = this.operation.list();
    assert list != null;

    final ListMeta metadata = list.getMetadata();
    assert metadata != null;
    
    final String resourceVersion = metadata.getResourceVersion();
    assert resourceVersion != null;

    final List<? extends T> items = list.getItems();
    assert items != null;
    
    store.replace(new ArrayList<T>(items), resourceVersion);
    
    this.setLastSyncResourceVersion(resourceVersion);

    final Duration resyncDuration = this.getResyncInterval();
    assert resyncDuration != null;
    final long seconds = resyncDuration.get(ChronoUnit.SECONDS);

    if (seconds > 0L && this.resyncExecutorService != null) {
      final ScheduledFuture<?> job = this.resyncExecutorService.scheduleWithFixedDelay(() -> {
          // TODO: what if any of this errors out?
          if (shouldResync()) {
            store.resync();
          }
        }, 0L, seconds, TimeUnit.SECONDS);
      assert job != null;
    }

    this.watch = this.operation.withResourceVersion(resourceVersion).watch(new Watcher());
    assert this.watch != null;

  }

  @Override
  public void close() {
    final Watch watch = this.watch;
    if (watch != null) {
      watch.close();
    }
  }


  /*
   * Inner and nested classes.
   */
  

  private final class Watcher implements io.fabric8.kubernetes.client.Watcher<T> {

    private Watcher() {
      super();
    }
    
    @Override
    public final void eventReceived(final io.fabric8.kubernetes.client.Watcher.Action event, final T resource) {
      Objects.requireNonNull(event);
      Objects.requireNonNull(resource);
      final ObjectMeta metadata = resource.getMetadata();
      assert metadata != null;
      final String newResourceVersion = metadata.getResourceVersion();
      switch (event) {
      case ADDED:
        store.add(resource);
        break;
      case MODIFIED:
        store.update(resource);
        break;
      case DELETED:
        store.delete(resource);
        break;
      case ERROR:
        throw new IllegalStateException();
      default:
        throw new IllegalStateException();
      }
      setLastSyncResourceVersion(newResourceVersion);
    }

    @Override
    public final void onClose(final KubernetesClientException exception) {
      System.out.println("*** logging close; exception: " + exception);
    }
    
  }
  
}
