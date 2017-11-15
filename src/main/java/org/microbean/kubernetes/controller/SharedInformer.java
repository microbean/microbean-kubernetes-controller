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

import java.time.Duration;
import java.time.Instant;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Queue;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReadWriteLock;

import java.util.function.Function;
import java.util.function.Supplier;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;

import io.fabric8.kubernetes.client.dsl.MixedOperation;

import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;

public class SharedInformer<T extends HasMetadata, L extends KubernetesResourceList, D> implements Runnable, Syncable {

  private final Store<T, L> store;

  private boolean hasSynced;

  private volatile String lastSyncResourceVersion;

  private List<ResourceEventListener> resourceEventListeners;
  
  public SharedInformer(final MixedOperation<T, L, ?, ?> operation,
                        final Store<T, L> store,
                        final Supplier<? extends List<? extends T>> listFunction,
                        final Function<Watcher<T>, Watch> watchFunction) {
    super();
    this.store = Objects.requireNonNull(store);
    this.resourceEventListeners = new ArrayList<>();
  }

  public final Store<T, L> getStore() {
    return this.store;
  }

  public final Controller<T, L, D> getController() {
    return null; // TODO: implement
  }

  @Override
  public void run() {
    // new delta fifo
    
  }

  @Override
  public boolean getHasSynced() {
    return this.hasSynced;
  }

  @Override
  public String getLastSyncResourceVersion() {
    return this.lastSyncResourceVersion;
  }

  public void addResourceEventListener(final ResourceEventListener listener) {
    if (listener != null) {
      this.resourceEventListeners.add(listener);
    }
  }

  public void removeResourceEventListener(final ResourceEventListener listener) {
    if (listener != null && !this.resourceEventListeners.isEmpty()) {
      this.resourceEventListeners.remove(listener);
    }
  }

  public ResourceEventListener[] getResourceEventListeners() {
    return this.resourceEventListeners.toArray(new ResourceEventListener[this.resourceEventListeners.size()]);
  }

  private static class Notification<T> {

    private final T object;
    
    protected Notification(final T object) {
      super();
      this.object = object;
    }

    public final T getObject() {
      return this.object;
    }
    
  }

  private static class Addition<T> extends Notification<T> {

    private Addition(final T newObject) {
      super(newObject);
    }
    
  }

  private static final class Update<T> extends Notification<T> {

    private final T oldObject;
    
    private Update(final T oldObject, final T newObject) {
      super(newObject);
      this.oldObject = oldObject;
    }

    public final T getOldObject() {
      return this.oldObject;
    }
    
  }

  private static final class Deletion<T> extends Notification<T> {

    private Deletion(final T oldObject) {
      super(oldObject);
    }
    
  }

  // Blind port for now of shared_informer.go's processorListener struct
  private static final class ProcessorListener<T> implements Runnable {

    private final SynchronousQueue<Notification<T>> incomingNotifications;

    private final SynchronousQueue<Notification<T>> outgoingNotifications;

    private final ResourceEventListener listener;

    private final Queue<Notification<T>> pendingNotifications; // growing ring buffer

    private final Duration requestedResyncPeriod;

    private Duration resyncPeriod;

    private volatile Instant nextResync;

    private final ReadWriteLock resyncLock;

    private final Lock resyncReadLock;

    private final Lock resyncWriteLock;
    
    private ProcessorListener(final ResourceEventListener listener,
                              final SynchronousQueue<Notification<T>> incomingNotifications,
                              final SynchronousQueue<Notification<T>> outgoingNotifications,
                              final Duration requestedResyncPeriod,
                              final Duration resyncPeriod,
                              final Instant now,
                              final int bufferSize) {
      super();
      this.resyncLock = new ReentrantReadWriteLock();
      this.resyncReadLock = this.resyncLock.readLock();
      this.resyncWriteLock = this.resyncLock.writeLock();
      this.listener = Objects.requireNonNull(listener);
      this.pendingNotifications = new ArrayDeque<>();
      this.incomingNotifications = incomingNotifications;
      this.outgoingNotifications = outgoingNotifications;
      this.requestedResyncPeriod = Objects.requireNonNull(requestedResyncPeriod);
      this.resyncPeriod = Objects.requireNonNull(resyncPeriod);
      this.determineNextResync(now);
    }

    private final void add(final Notification<T> notification) throws InterruptedException {
      if (notification != null) {
        this.incomingNotifications.put(notification);
      }
    }

    // To be started asynchronously by SharedProcessor.
    // Emulates pop() method on processorListener in shared_informer.go.
    // 
    // That method is opaque and difficult to read.  We need to clean
    // this up once we're sure the port is correct.    
    private final void pop() {
      SynchronousQueue<Notification<T>> outgoingNotifications = null;
      Notification<T> notification = null;
      while (!Thread.currentThread().isInterrupted()) {
        if (notification != null) {
          assert outgoingNotifications != null;
          try {
            outgoingNotifications.put(notification); // blocks until consumed (by run() method below); emulates Go channel
          } catch (final InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            // TODO: the Go code will drop this notification on the
            // ground in the analogous case, so we do too.
            break;
          }
          notification = this.pendingNotifications.poll(); // non-blocking; returns null if there's nothing in there
          if (notification == null) {
            outgoingNotifications = null;
          }
        } else {
          Notification<T> notificationToAdd = null;
          try {
            notificationToAdd = this.incomingNotifications.take(); // blocks until someone calls add(); simulates Go channel
          } catch (final InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            break;
          }
          assert notificationToAdd != null;
          if (notification == null) {
            notification = notificationToAdd;
            outgoingNotifications = this.outgoingNotifications;
          } else {
            this.pendingNotifications.add(notificationToAdd); // non-blocking
          }
        }
      }
    }
    
    @Override
    public final void run() {
      while (!Thread.currentThread().isInterrupted()) {
        Notification<T> notification = null;
        try {
          notification = this.outgoingNotifications.take(); // blocks
        } catch (final InterruptedException interruptedException) {
          Thread.currentThread().interrupt();
          break;
        }
        assert notification != null;
        if (notification instanceof Addition) {
          this.listener.resourceAdded(null); // TODO FIX
        } else if (notification instanceof Update) {
          this.listener.resourceUpdated(null); // TODO FIX
        } else if (notification instanceof Deletion) {
          this.listener.resourceDeleted(null); // TODO FIX
        } else {
          throw new IllegalStateException();
        }
      }
    }

    private final void setResyncPeriod(final Duration resyncPeriod) {
      Objects.requireNonNull(resyncPeriod);
      this.resyncWriteLock.lock();
      try {
        this.resyncPeriod = resyncPeriod;
      } finally {
        this.resyncWriteLock.unlock();
      }
    }
    
    private final void determineNextResync(final Instant now) {
      Objects.requireNonNull(now);
      this.resyncWriteLock.lock();
      try {
        this.nextResync = now.plus(this.resyncPeriod);
      } finally {
        this.resyncWriteLock.unlock();
      }
    }

    private final boolean shouldResync(final Instant now) {
      Objects.requireNonNull(now);
      this.resyncReadLock.lock();
      try {
        if (this.resyncPeriod.isZero()) {
          return false;
        }
        return now.compareTo(this.nextResync) >= 0;
      } finally {
        this.resyncReadLock.unlock();
      }
    }
    
  }
  
}
