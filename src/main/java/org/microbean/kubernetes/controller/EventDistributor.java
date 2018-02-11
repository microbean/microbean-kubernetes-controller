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
import java.time.Instant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.function.Consumer;

import io.fabric8.kubernetes.api.model.HasMetadata;

import net.jcip.annotations.Immutable;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

@Immutable
@ThreadSafe
public final class EventDistributor<T extends HasMetadata> extends ResourceTrackingEventQueueConsumer<T> implements Closeable {

  @GuardedBy("readLock && writeLock")
  private final Collection<Pump<T>> distributors;

  @GuardedBy("readLock && writeLock")
  private final Collection<Pump<T>> synchronizingDistributors;

  private final Duration synchronizationInterval;

  private final Lock readLock;

  private final Lock writeLock;
  
  public EventDistributor(final Map<Object, T> knownObjects) {
    super(knownObjects);
    final ReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
    this.distributors = new ArrayList<>();    
    this.synchronizingDistributors = new ArrayList<>();
    this.synchronizationInterval = null; // TODO: implement/fix
  }

  public final void addConsumer(final Consumer<? super AbstractEvent<? extends T>> consumer) {
    if (consumer != null) {
      this.writeLock.lock();
      try {
        final Pump<T> distributor = new Pump<>(this.synchronizationInterval, consumer);
        this.distributors.add(distributor);
        this.synchronizingDistributors.add(distributor);
      } finally {
        this.writeLock.unlock();
      }
    }
  }

  public final void removeConsumer(final Consumer<? super AbstractEvent<? extends T>> consumer) {
    if (consumer != null) {
      this.writeLock.lock();
      try {
        final Iterator<? extends Pump<?>> iterator = this.distributors.iterator();
        assert iterator != null;
        while (iterator.hasNext()) {
          final Pump<?> distributor = iterator.next();
          if (distributor != null && consumer.equals(distributor.getEventConsumer())) {
            iterator.remove();
            break;
          }
        }
      } finally {
        this.writeLock.unlock();
      }
    }
  }

  @Override
  public final void close() {
    this.writeLock.lock();
    try {
      this.distributors.parallelStream()
        .forEach(distributor -> {
            distributor.close();
          });
    } finally {
      this.writeLock.unlock();
    }
  }

  public final boolean shouldSynchronize() {
    boolean returnValue = false;
    this.writeLock.lock();
    try {
      this.synchronizingDistributors.clear();
      final Instant now = Instant.now();
      this.distributors.parallelStream()
        .filter(distributor -> distributor.shouldSynchronize(now))
        .forEach(distributor -> {
            this.synchronizingDistributors.add(distributor);
            distributor.determineNextSynchronizationInterval(now);
          });
      returnValue = !this.synchronizingDistributors.isEmpty();
    } finally {
      this.writeLock.unlock();
    }
    return returnValue;
  }

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
      if (!this.synchronizingDistributors.isEmpty()) {
        this.synchronizingDistributors.parallelStream()
          .forEach(distributor -> distributor.accept(event));
      }
    } finally {
      this.readLock.unlock();
    }
  }

  private final void accept(final Event<? extends T> event) {
    this.readLock.lock();
    try {
      if (!this.distributors.isEmpty()) {
        this.distributors.parallelStream()
          .forEach(distributor -> distributor.accept(event));
      }
    } finally {
      this.readLock.unlock();
    }
  }


  /*
   * Inner and nested classes.
   */
  

  private static final class Pump<T extends HasMetadata> implements Consumer<AbstractEvent<? extends T>>, Closeable {

    private volatile Instant nextSynchronizationInstant;
    
    private volatile Duration synchronizationInterval;
    
    final BlockingQueue<AbstractEvent<? extends T>> queue;
    
    private final Executor executor;
    
    private final Consumer<? super AbstractEvent<? extends T>> eventConsumer;
    
    private Pump(final Duration synchronizationInterval, final Consumer<? super AbstractEvent<? extends T>> eventConsumer) {
      super();
      Objects.requireNonNull(eventConsumer);
      this.queue = new LinkedBlockingQueue<>();
      this.executor = Executors.newSingleThreadExecutor();
      this.setSynchronizationInterval(synchronizationInterval);
      this.eventConsumer = eventConsumer;
    }
    
    final Consumer<? super AbstractEvent<? extends T>> getEventConsumer() {
      return this.eventConsumer;
    }
    
    /**
     * Adds the supplied {@link AbstractEvent} to an internal {@link
     * BlockingQueue} and schedules a task to consume it.
     *
     * @param event the {@link AbstractEvent} to add; may be {@code
     * null} in which case no action is taken
     */
    @Override
    public final void accept(final AbstractEvent<? extends T> event) {
      if (event != null) {
        final boolean added = this.queue.add(event);
        assert added;
        this.executor.execute(() -> {
            try {
              this.fireEvent(this.queue.take());
            } catch (final InterruptedException interruptedException) {
              Thread.currentThread().interrupt();
            }
          });
      }
    }
    
    @Override
    public void close() {
      if (this.executor instanceof ExecutorService) {
        final ExecutorService executorService = (ExecutorService)this.executor;
        executorService.shutdown();
        try {
          if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
              // TODO: log
            }
          }
        } catch (final InterruptedException interruptedException) {
          executorService.shutdownNow();
          Thread.currentThread().interrupt();
        }
      }
    }
    
    private final void fireEvent(final AbstractEvent<? extends T> event) {
      this.eventConsumer.accept(event);
    }
    
    
    /*
     * Synchronization-related methods.  It seems odd that one of these
     * listeners would need to report details about synchronization, but
     * that's what the Go code does.  Maybe this functionality could be
     * relocated "higher up".
     */
    
    
    public final boolean shouldSynchronize(Instant now) {
      if (now == null) {
        now = Instant.now();
      }
      final Duration interval = this.synchronizationInterval;
      final boolean returnValue = interval != null && !interval.isZero() && now.compareTo(this.nextSynchronizationInstant) >= 0;
      return returnValue;
    }
    
    public final void determineNextSynchronizationInterval(Instant now) {
      if (now == null) {
        now = Instant.now();
      }
      this.nextSynchronizationInstant = now.plus(this.synchronizationInterval);
    }
    
    public void setSynchronizationInterval(final Duration synchronizationInterval) {
      this.synchronizationInterval = synchronizationInterval;
    }
    
    public Duration getSynchronizationInterval() {
      return this.synchronizationInterval;
    }
  }
  
}
