/* -*- mode: Java; c-basic-offset: 2; indent-tabs-mode: nil; coding: utf-8-unix -*-
 *
 * Copyright © 2018 microBean.
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
import java.util.EventObject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

  private final ReentrantReadWriteLock lock;

  private final Lock readLock;

  private final Lock writeLock;

  private final Condition eventQueuesIsNotEmpty;
  
  private final LinkedHashMap<Object, Collection<Event<? extends T>>> eventQueues;

  private final Map<Object, T> resources;
  
  private final Object operation;

  private volatile Object lastSyncResourceVersion;

  private final ScheduledExecutorService resyncExecutorService;

  private volatile Thread distributorThread;
  
  private final Duration resyncInterval;

  private final Iterable<? extends T> knownObjects;

  private boolean populated;

  private int initialPopulationCount;

  private volatile Closeable watch;


  /*
   * Constructors.
   */
  

  @SuppressWarnings("rawtypes") // kubernetes-client's implementations of KubernetesResourceList use raw types
  public <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> Reflector(final X operation,
                                                                                                                              final Duration resyncInterval) {
    this(operation, resyncInterval == null ? null : Executors.newScheduledThreadPool(1), resyncInterval, null);
  }

  @SuppressWarnings("rawtypes") // kubernetes-client's implementations of KubernetesResourceList use raw types
  public <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> Reflector(final X operation,
                                                                                                                              final ScheduledExecutorService resyncExecutorService,
                                                                                                                              final Duration resyncInterval) {
    this(operation, resyncExecutorService, resyncInterval, null);
  }
  
  @SuppressWarnings("rawtypes") // kubernetes-client's implementations of KubernetesResourceList use raw types
  public <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> Reflector(final X operation,
                                                                                                                              final ScheduledExecutorService resyncExecutorService,
                                                                                                                              final Duration resyncInterval,
                                                                                                                              final Iterable<? extends T> knownObjects) {
    super();
    this.lock = new ReentrantReadWriteLock();
    this.readLock = this.lock.readLock();
    this.writeLock = this.lock.writeLock();
    this.eventQueuesIsNotEmpty = this.writeLock.newCondition();
    this.eventQueues = new LinkedHashMap<>();
    this.resources = new HashMap<>();
    // TODO: research: maybe: operation.withField("metadata.resourceVersion", "0")?    
    this.operation = operation.withResourceVersion("0");
    this.resyncExecutorService = resyncExecutorService;
    this.resyncInterval = resyncInterval;
    this.knownObjects = knownObjects;
  }

  private final Iterable<? extends T> getKnownObjects() {
    return this.knownObjects;
  }

  protected Collection<Event<? extends T>> createEventQueue(final Object key) {
    assert this.lock.isWriteLockedByCurrentThread();
    return new ArrayList<>();
  }

  private final void enqueue(final Event<T> event) {
    if (event != null) {
      final Object key = event.getKey();
      if (key == null) {
        throw new IllegalStateException("event.getKey() == null: " + event);
      }

      this.writeLock.lock();
      try {
        this.populated = true;
        final Event.Type eventType = event.getType();
        assert eventType != null;
        if (!Event.Type.SYNCHRONIZATION.equals(eventType) || !willObjectBeDeleted(key)) {
          
          Collection<Event<? extends T>> eventQueue = this.eventQueues.get(key);
          final boolean eventQueueExisted = eventQueue != null;
          if (!eventQueueExisted) {
            eventQueue = this.createEventQueue(key);
            if (eventQueue == null) {
              throw new IllegalStateException("createEventQueue(key) == null: " + key);
            }
          }
          assert eventQueue != null;

          final T resource = event.getResource();
          T oldResource = null;

          if (eventType.equals(Event.Type.DELETION)) {
            oldResource = this.resources.remove(key);
          } else {
            oldResource = this.resources.put(key, resource);
            event.setOldResource(oldResource);
          }
          
          try {
            this.enqueue(eventQueue, event);
          } catch (final RuntimeException runtimeException) {
            // If enqueuing failed, restore our "old state" map to
            // what it was.
            this.resources.put(key, oldResource);
            throw runtimeException;
          }

          eventQueue = this.deduplicateEvents(eventQueue);
          eventQueue = this.compress(eventQueue);
          
          if (eventQueue != null && !eventQueue.isEmpty()) {
            this.eventQueues.put(key, eventQueue);
            this.eventQueuesIsNotEmpty.signal();
          } else if (eventQueueExisted) {
            // There WAS an event queue, but now it's gone as a result
            // of deduplication or compression.
            this.eventQueues.remove(key);
          }
          
        }
      } finally {
        this.writeLock.unlock();
      }
      
    }
  }

  private final Collection<Event<? extends T>> deduplicateEvents(final Collection<Event<? extends T>> events) {
    assert this.lock.isWriteLockedByCurrentThread();
    final Collection<Event<? extends T>> returnValue;
    final int size = events == null ? 0 : events.size();
    if (size < 2) {
      returnValue = events;
    } else {
      final Event<? extends T> lastEvent = get(events, size - 1);
      final Event<? extends T> nextToLastEvent = get(events, size - 2);
      final Event<? extends T> event = arbitrateDuplicates(lastEvent, nextToLastEvent);
      if (event == null) {
        returnValue = events;
      } else {
        if (events instanceof List) {
          final List<Event<? extends T>> eventList = (List<Event<? extends T>>)events;
          eventList.set(size - 2, event);
          eventList.remove(size - 1);
        } else {
          final Iterator<Event<? extends T>> iterator = events.iterator();
          assert iterator != null;
          for (int i = 0; i < size; i++) {
            assert iterator.hasNext();
            iterator.next();
            if (i + 1 == size || i + 2 == size) {
              iterator.remove();
            }
          }
          events.add(event);
        }
        returnValue = events;
      }
    }
    return returnValue;
  }

  private final Event<? extends T> arbitrateDuplicates(final Event<? extends T> a, final Event<? extends T> b) {    
    final Event<? extends T> returnValue;
    if (a != null && b != null && Event.Type.DELETION.equals(a.getType()) && Event.Type.DELETION.equals(b.getType())) {
      if (b.isFinalStateKnown()) {
        returnValue = b;
      } else {
        returnValue = a;
      }
    } else {
      returnValue = null;
    }
    return returnValue;
  }
  
  private final boolean willObjectBeDeleted(final Object key) {
    assert this.lock.isWriteLockedByCurrentThread();
    final Collection<Event<? extends T>> eventQueue = this.eventQueues.get(key);
    return eventQueue != null && !eventQueue.isEmpty() && get(eventQueue, eventQueue.size() - 1).getType().equals(Event.Type.DELETION);
  }

  protected Collection<Event<? extends T>> compress(final Collection<Event<? extends T>> events) {
    assert this.lock.isWriteLockedByCurrentThread();
    return events;
  }

  private static final <X> X get(final Collection<? extends X> items, final int index) {
    X returnValue = null;
    if (items != null && !items.isEmpty()) {
      if (items instanceof List) {
        returnValue = ((List<? extends X>)items).get(index);
      } else {
        final int size = items.size();
        assert size > 0;
        final Iterator<? extends X> iterator = items.iterator();
        if (iterator != null && iterator.hasNext()) {
          for (int i = 0; i < size - index; i++) {
            assert iterator.hasNext() : "!iterator.hasNext(); i: " + i;
            if (i + 1 == size) {
              returnValue = iterator.next();
            } else {
              iterator.next();
            }
          }
        }
      }
    }
    return returnValue;
  }

  public final boolean getHasSynced() {
    this.readLock.lock();
    try {
      return this.populated && this.initialPopulationCount == 0;
    } finally {
      this.readLock.unlock();
    }
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

  protected boolean shouldResync() {
    return this.resyncExecutorService != null;
  }

  private final Object getKey(final Collection<? extends Event<? extends T>> events) {
    Object returnValue = null;
    if (events != null && !events.isEmpty()) {
      final int size = events.size();
      if (size > 0) {
        final Event<? extends T> event;
        if (events instanceof List) {
          event = ((List<? extends Event<? extends T>>)events).get(size - 1);
          if (event != null) {
            returnValue = event.getKey();
          }
        } else {
          final Iterator<? extends Event<? extends T>> iterator = events.iterator();
          Event<? extends T> e = null;
          if (iterator != null) {
            for (int i = 0; i < size; i++) {
              e = iterator.next();
            }
          }
          event = e;
        }
        if (event != null) {
          returnValue = event.getKey();
        }
      }
    }
    return returnValue;
  }

  protected Object getKey(final T resource) {
    final Object returnValue;
    if (resource == null) {
      returnValue = null;
    } else {
      final ObjectMeta metadata = resource.getMetadata();
      if (metadata == null) {
        returnValue = null;
      } else {
        String name = metadata.getName();
        if (name == null) {
          throw new IllegalStateException("metadata.getName() == null");
        } else if (name.isEmpty()) {
          throw new IllegalStateException("metadata.getName().isEmpty()");
        }
        final String namespace = metadata.getNamespace();
        if (namespace == null || namespace.isEmpty()) {
          returnValue = name;
        } else {
          returnValue = new StringBuilder(namespace).append("/").append(name).toString();
        }
      }
    }
    return returnValue;
  }

  @NonBlocking
  public synchronized final void start() {
    this.startReflecting();
    this.startDistributing();
  }
  
  // Models the ListAndWatch func in the Go code.  But note that the
  // websocket code underlying the fabric8 Kubernetes client watch
  // behavior is non-blocking, so the setting up and running of the
  // watch does not, itself, block.
  private synchronized final void startReflecting() {
    if (this.watch == null) {
      final Thread reflectorThread = new Thread(() -> {
          
          @SuppressWarnings("unchecked")
          final KubernetesResourceList<? extends T> list = ((Listable<? extends KubernetesResourceList<? extends T>>)Reflector.this.operation).list();
          assert list != null;
          
          final ListMeta metadata = list.getMetadata();
          assert metadata != null;
          
          final String resourceVersion = metadata.getResourceVersion();
          assert resourceVersion != null;
          
          final Collection<? extends T> items = list.getItems();
          if (items == null || items.isEmpty()) {
            Reflector.this.replace(Collections.emptySet(), resourceVersion);
          } else {
            Reflector.this.replace(Collections.unmodifiableCollection(new ArrayList<T>(items)), resourceVersion);
          }
          
          Reflector.this.setLastSyncResourceVersion(resourceVersion);
          
          if (Reflector.this.resyncExecutorService != null) {
            
            final Duration resyncDuration = Reflector.this.getResyncInterval();
            final long seconds;
            if (resyncDuration == null) {
              seconds = 0L;
            } else {
              seconds = resyncDuration.get(ChronoUnit.SECONDS);
            }
            
            if (seconds > 0L) {
              final ScheduledFuture<?> job = Reflector.this.resyncExecutorService.scheduleWithFixedDelay(() -> {
                  Reflector.this.writeLock.lock();
                  try {
                    if (Reflector.this.shouldResync()) {
                      Reflector.this.resync();
                    }
                  } catch (final RuntimeException runtimeException) {
                    // TODO: log or...?
                    throw runtimeException;
                  } finally {
                    Reflector.this.writeLock.unlock();
                  }
                }, 0L, seconds, TimeUnit.SECONDS);
              assert job != null;
              // TODO: we don't need to do anything with this job, right?
            }
            
          }
          
          @SuppressWarnings("unchecked")
          final Closeable temp = ((VersionWatchable<? extends Closeable, Watcher<T>>)Reflector.this.operation).withResourceVersion(resourceVersion).watch(new WatchHandler());
          assert temp != null;
          Reflector.this.watch = temp;
        });
      reflectorThread.setDaemon(true);
      reflectorThread.start();
    }
  }

  private synchronized final void startDistributing() {
    if (this.distributorThread == null) {
      this.distributorThread = new Thread(() -> {
          PROCESSING_LOOP:
          while (!Thread.interrupted()) {
            
            this.writeLock.lock();          
            try {

              // Block while there are no event queues to work on.
              while (this.eventQueues.isEmpty()) {
                try {
                  this.eventQueuesIsNotEmpty.await();
                } catch (final InterruptedException interruptedException) {
                  Thread.currentThread().interrupt();
                  break PROCESSING_LOOP;
                }
              }
              assert !this.eventQueues.isEmpty();

              // Get and remove the first event queue.  The queue
              // might hold, for example, events that have happened to
              // a hypothetical pod default/sq3r9.
              final Collection<? extends Event<? extends T>> eventQueue = this.pop();
              if (this.initialPopulationCount > 0) {
                this.initialPopulationCount--;
              }

              // If indeed we got an event queue, process all of its
              // contained events.
              if (eventQueue != null && !eventQueue.isEmpty()) {
                try {
                  this.processEventQueue(eventQueue);
                } catch (final RuntimeException oops) {
                  // TODO: see if it's a requeue request, and if so,
                  // call the equivalent of addIfNotPresent
                  throw oops;
                }
              }
              
            } finally {
              this.writeLock.unlock();
            }
          }
        });
      this.distributorThread.setDaemon(true);
      this.distributorThread.start();
    }
  }

  // e.g. handleDeltas
  private final void processEventQueue(final Collection<? extends Event<? extends T>> eventQueue) {
    assert this.lock.isWriteLockedByCurrentThread();
    if (eventQueue != null && !eventQueue.isEmpty()) {
      // TODO: investigate parallelism
      for (final Event<? extends T> event : eventQueue) {
        if (event != null) {
          this.fireEvent(event);
        }
      }
    }
  }

  protected void fireEvent(final Event<? extends T> event) {
    System.out.println("*** firing event: " + event);
  }
  
  private Collection<? extends Event<? extends T>> pop() {
    assert this.lock.isWriteLockedByCurrentThread();
    Collection<? extends Event<? extends T>> returnValue = null;
    if (!this.eventQueues.isEmpty()) {
      final Collection<? extends Entry<?, ? extends Collection<? extends Event<? extends T>>>> entrySet = this.eventQueues.entrySet();
      assert entrySet != null;
      assert !entrySet.isEmpty();
      final Iterator<? extends Entry<?, ? extends Collection<? extends Event<? extends T>>>> iterator = entrySet.iterator();
      assert iterator != null;
      assert iterator.hasNext();
      final Entry<?, ? extends Collection<? extends Event<? extends T>>> firstEntry = iterator.next();
      assert firstEntry != null;
      iterator.remove();
      returnValue = firstEntry.getValue();
    }
    return returnValue;
  }

  private final void resync() {
    assert this.lock.isWriteLockedByCurrentThread();
    final Iterable<? extends T> knownObjects = this.getKnownObjects();
    if (knownObjects != null) {
      // TODO: I don't like this.  I need to provide *some* help here:
      // the user who constructed this supplied an Iterable and may be
      // writing to it while I'm trying to iterate over it.  But I
      // don't like synchronizing on something I don't control.
      synchronized (knownObjects) {
        for (final T knownObject : knownObjects) {
          if (knownObject != null) {
            final Object key = this.getKey(knownObject);
            final Collection<Event<? extends T>> eventQueue = this.eventQueues.get(key);
            if (eventQueue == null || eventQueue.isEmpty()) {
              this.enqueue(new Event<>(this, Event.Type.SYNCHRONIZATION, null, null /* oldResource; may get set later */, knownObject));
            }
          }
        }
      }
    }
  }
  
  private final void replace(final Collection<? extends T> resources, final Object resourceVersion) {
    Objects.requireNonNull(resources);
    
    this.writeLock.lock();
    try {

      final Set<Object> keys = new HashSet<>();
      for (final T resource : resources) {
        final Object key = this.getKey(resource);
        if (key == null) {
          throw new IllegalStateException("getKey(resource) == null: " + resource);
        }
        keys.add(key);
        this.enqueue(new Event<>(this, Event.Type.SYNCHRONIZATION, null, null /* oldResource; may get set later */, resource));
      }

      final Iterable<? extends T> knownObjects = this.getKnownObjects();
      if (knownObjects == null) {
        final Collection<? extends Entry<?, Collection<Event<? extends T>>>> entrySet = this.eventQueues.entrySet();
        if (entrySet != null && !entrySet.isEmpty()) {
          for (final Entry<?, ? extends Collection<Event<? extends T>>> entry : entrySet) {
            assert entry != null;
            final Object key = entry.getKey();
            if (!keys.contains(key)) {
              // We have an event queue indexed under a key that no
              // longer exists in Kubernetes.
              final Collection<Event<? extends T>> eventQueue = entry.getValue();
              assert eventQueue != null;
              final Event<? extends T> newestEvent = get(eventQueue, eventQueue.size() - 1);
              assert newestEvent != null;
              final T resourceToBeDeleted = newestEvent.getResource();
              assert resourceToBeDeleted != null;
              this.enqueue(new Event<>(this, Event.Type.DELETION, key, resourceToBeDeleted, resourceToBeDeleted));
              if (!this.populated) {
                this.populated = true;
                this.initialPopulationCount = eventQueue.size();
              }
            }
          }
        }
      } else {
        int queuedDeletions = 0;
        // TODO: I don't like this.  I need to provide *some* help here:
        // the user who constructed this supplied an Iterable and may be
        // writing to it while I'm trying to iterate over it.  But I
        // don't like synchronizing on something I don't control.
        synchronized (knownObjects) {
          for (final T knownObject : knownObjects) {
            if (knownObject != null) {
              final Object knownKey = this.getKey(knownObject);
              if (knownKey != null && !keys.contains(knownKey)) {
                queuedDeletions++;
                this.enqueue(new Event<>(this, Event.Type.DELETION, knownKey, knownObject, knownObject));
              }
            }
          }
        }
        if (!this.populated) {
          this.populated = true;
          this.initialPopulationCount = resources.size() + queuedDeletions;
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }
  
  protected void enqueue(final Collection<Event<? extends T>> queue, final Event<? extends T> event) {
    assert this.lock.isWriteLockedByCurrentThread();
    if (queue != null && event != null) {
      queue.add(event);
    }
  }

  @Override
  public synchronized final void close() throws IOException {
    try {
      if (this.watch != null) {
        this.watch.close();
      }
    } finally {
      if (this.distributorThread != null) {
        this.distributorThread.interrupt();
      }
      this.onClose();
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
        event = new Event<>(Reflector.this, Event.Type.ADDITION, null, null, resource);
        break;
      case MODIFIED:
        event = new Event<>(Reflector.this, Event.Type.MODIFICATION, null, null, resource);
        break;
      case DELETED:
        event = new Event<>(Reflector.this, Event.Type.DELETION, null, resource, resource);
        break;
      case ERROR:
        event = null;
        throw new IllegalStateException();
      default:
        event = null;
        throw new IllegalStateException();
      }
      Reflector.this.enqueue(event);
      Reflector.this.setLastSyncResourceVersion(newResourceVersion);
    }

    @Override
    public final void onClose(final KubernetesClientException exception) {
      System.out.println("*** logging close; exception: " + exception);
    }
    
  }


  /**
   * An {@link EventObject} representing an addition, modification or
   * deletion of a Kubernetes resource.
   *
   * @author <a href="https://about.me/lairdnelson"
   * target="_parent">Laird Nelson</a>
   *
   * @param <T> the type of Kubernetes resource described by this event
   */
  public static final class Event<T extends HasMetadata> extends EventObject {

    private static final long serialVersionUID = 1L;

    private final Object key;
    
    private final Type type;

    private final T resource;

    private volatile T oldResource;

    private Event(final Reflector<T> source, final Type type, final Object key, final T oldResource, final T resource) {
      super(source);
      this.key = key;
      this.type = key != null ? Type.DELETION : Objects.requireNonNull(type);
      if (type.equals(Type.ADDITION) && oldResource != null) {
        throw new IllegalArgumentException("type.equals(Type.ADDITION) && oldResource != null: " + oldResource);
      }
      this.oldResource = oldResource;
      this.resource = resource;
    }

    @Override
    public Reflector<T> getSource() {
      @SuppressWarnings("unchecked")
      final Reflector<T> returnValue = (Reflector<T>)super.getSource();
      return returnValue;
    }
    
    public final Type getType() {
      return this.type;
    }

    public final T getResource() {
      return this.resource;
    }

    public final T getOldResource() {
      return this.oldResource;
    }

    private final void setOldResource(final T oldResource) {
      final T preexistingOldResource = this.getOldResource();
      if (preexistingOldResource != null) {
        throw new IllegalStateException("getOldResource() != null: " + preexistingOldResource);
      }
      this.oldResource = oldResource;
    }

    private final boolean isFinalStateKnown() {
      return this.key == null;
    }

    public final Object getKey() {
      Object returnValue = this.key;
      if (returnValue == null) {
        final T resource = this.getResource();
        if (resource != null) {
          final ObjectMeta metadata = resource.getMetadata();
          if (metadata != null) {
            final String namespace = metadata.getNamespace();
            if (namespace == null || namespace.isEmpty()) {
              returnValue = metadata.getName();
            } else {
              returnValue = new StringBuilder(namespace).append("/").append(metadata.getName()).toString();
            }
          }
        }
      }
      return returnValue;
    }

    @Override
    public final int hashCode() {
      int hashCode = 37;

      final Object source = this.getSource();
      int c = source == null ? 0 : source.hashCode();
      hashCode = hashCode * 17 + c;

      final Object key = this.getKey();
      c = key == null ? 0 : key.hashCode();
      hashCode = hashCode * 17 + c;
      
      final Object type = this.getType();
      c = type == null ? 0 : type.hashCode();
      hashCode = hashCode * 17 + c;

      final Object resource = this.getResource();
      c = resource == null ? 0 : resource.hashCode();
      hashCode = hashCode * 17 + c;

      final Object oldResource = this.getOldResource();
      c = oldResource == null ? 0 : oldResource.hashCode();
      hashCode = hashCode * 17 + c;
      
      return hashCode;
    }

    @Override
    public final boolean equals(final Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof Event) {

        @SuppressWarnings("unchecked")
        final Event<?> her = (Event<?>)other;

        final Object source = this.getSource();
        if (source == null) {
          if (her.getSource() != null) {
            return false;
          }
        } else if (!source.equals(her.getSource())) {
          return false;
        }

        final Object key = this.getKey();
        if (key == null) {
          if (her.getKey() != null) {
            return false;
          }
        } else if (!key.equals(her.getKey())) {
          return false;
        }
        
        final Object type = this.getType();
        if (type == null) {
          if (her.getType() != null) {
            return false;
          }
        } else if (!type.equals(her.getType())) {
          return false;
        }

        final Object resource = this.getResource();
        if (resource == null) {
          if (her.getResource() != null) {
            return false;
          }
        } else if (!resource.equals(her.getResource())) {
          return false;
        }

        final Object oldResource = this.getOldResource();
        if (oldResource == null) {
          if (her.getOldResource() != null) {
            return false;
          }
        } else if (!oldResource.equals(her.getOldResource())) {
          return false;
        }
        
        return true;
      } else {
        return false;
      }
    }

    @Override
    public final String toString() {
      return new StringBuilder(this.getType()).append(": ").append(this.getOldResource()).append(" -> ").append(this.getResource()).toString();
    }

    public static enum Type {
      ADDITION,
      MODIFICATION,
      DELETION,
      SYNCHRONIZATION      
    }
    
  }
  
}
