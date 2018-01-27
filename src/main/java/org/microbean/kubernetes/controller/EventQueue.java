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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;

import java.util.function.Consumer;

import io.fabric8.kubernetes.api.model.HasMetadata;

public class EventQueue<T extends HasMetadata> implements Iterable<Event<T>> {

  private final Object key;

  private final LinkedList<Event<T>> events;

  protected EventQueue(final Object key) {
    super();
    this.key = Objects.requireNonNull(key);
    this.events = new LinkedList<>();
  }

  public final Object getKey() {
    return this.key;
  }

  public synchronized final boolean isEmpty() {
    return this.events.isEmpty();
  }
  
  public synchronized final int size() {
    return this.events.size();
  }

  final boolean add(final Event<T> event) {    
    Objects.requireNonNull(event);
    final Object key = this.getKey();
    if (!key.equals(event.getKey())) {
      throw new IllegalArgumentException("!this.getKey().equals(event.getKey()): " + key + ", " + event.getKey());
    }
    boolean returnValue = false;
    final Event.Type eventType = event.getType();
    assert eventType != null;
    if (!eventType.equals(Event.Type.SYNCHRONIZATION) || !this.getResultsInDeletion()) {
      synchronized (this) {
        returnValue = this.events.add(event);
        if (returnValue) {
          this.deduplicate();
          final Collection<Event<T>> readOnlyEvents = Collections.unmodifiableCollection(this.events);
          final Collection<Event<T>> newEvents = this.compress(readOnlyEvents);
          if (newEvents != readOnlyEvents) {
            this.events.clear();
            if (newEvents != null && !newEvents.isEmpty()) {
              this.events.addAll(newEvents);
            }
          }
          returnValue = !this.isEmpty();
        }
      }
    }
    return returnValue;
  }

  public synchronized final Event<T> get(final int index) {
    return this.events.get(index);
  }

  public synchronized final Event<T> getLast() {
    return this.events.getLast();
  }

  private synchronized final Event<T> remove() {
    return this.events.remove();
  }

  @Override
  public synchronized final void forEach(final Consumer<? super Event<T>> action) {
    Iterable.super.forEach(action);
  }
  
  @Override
  public synchronized final Iterator<Event<T>> iterator() {
    return Collections.unmodifiableCollection(this.events).iterator();
  }

  private final void deduplicate() {
    final int size = this.size();
    if (size > 2) {
      final Event<T> lastEvent = this.events.get(size - 1);
      final Event<T> nextToLastEvent = this.events.get(size - 2);
      final Event<T> event = arbitrateDuplicates(lastEvent, nextToLastEvent);
      if (event != null) {
        this.events.set(size - 2, event);
        this.events.remove(size - 1);
      }
    }
  }

  private static final <X extends HasMetadata> Event<X> arbitrateDuplicates(final Event<X> a, final Event<X> b) {    
    final Event<X> returnValue;
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

  synchronized final boolean getResultsInDeletion() {
    return !this.isEmpty() && this.getLast().getType().equals(Event.Type.DELETION);
  }
  
  protected Collection<Event<T>> compress(final Collection<Event<T>> events) {
    return events;
  }

  @Override
  public final int hashCode() {
    int hashCode = 17;

    Object value = this.getKey();
    int c = value == null ? 0 : value.hashCode();
    hashCode = 37 * hashCode + c;

    value = this.events;
    c = value == null ? 0 : value.hashCode();
    hashCode = 37 * hashCode + c;

    return hashCode;
  }

  @Override
  public final boolean equals(final Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof EventQueue) {
      final EventQueue<?> her = (EventQueue<?>)other;

      final Object key = this.getKey();
      if (key == null) {
        if (her.getKey() != null) {
          return false;
        }
      } else if (!key.equals(her.getKey())) {
        return false;
      }

      final Object events = this.events;
      if (events == null) {
        if (her.events != null) {
          return false;
        }
      } else if (!events.equals(her.events)) {
        return false;
      }

      return true;
    } else {
      return false;
    }
  }

  @Override
  public synchronized final String toString() {
    return new StringBuilder().append(this.getKey()).append(": ").append(this.events).toString();
  }

}
