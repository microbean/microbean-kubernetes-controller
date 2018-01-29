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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import java.util.function.Consumer;

import io.fabric8.kubernetes.api.model.HasMetadata;

import net.jcip.annotations.GuardedBy;

/**
 * An abstract {@link Consumer} of {@link EventQueue}s, particularly
 * suitable for supplying to the {@link
 * EventQueueCollection#start(Consumer)} method, whose implementations
 * can distribute {@link Event}s in some fashion.
 *
 * @author <a href="https://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 *
 * @see EventQueue
 *
 * @see EventQueueCollection#start(Consumer)
 */
public abstract class AbstractEventDistributor<T extends HasMetadata> implements Consumer<EventQueue<T>> {


  /*
   * Instance fields.
   */


  /**
   * A mutable {@link Map} of {@link HasMetadata} objects indexed by
   * their keys (often a pairing of namespace and name).
   *
   * <p>This field is never {@code null}.</p>
   *
   * <p>The value of this field is {@linkplain
   * #AbstractEventDistributor(Map) supplied at construction time} and
   * is written to by the {@link #accept(EventQueue)} method.</p>
   *
   * <p>This {@link AbstractEventDistributor} implementation
   * <strong>synchronizes on this field's value</strong> when mutating
   * its contents.</p>
   */
  @GuardedBy("itself")
  private final Map<Object, T> knownObjects;


  /*
   * Constructors.
   */


  /**
   * Creates a new {@link AbstractEventDistributor}.
   *
   * @param knownObjects a mutable {@link Map} of {@link HasMetadata}
   * objects indexed by their keys (often a pairing of namespace and
   * name); must not be {@code null}; <strong>will have its contents
   * changed</strong> by this {@link AbstractEventDistributor}'s
   * {@link #accept(EventQueue)} method; <strong>will be synchronized
   * on</strong> by this {@link AbstractEventDistributor}'s {@link
   * #accept(EventQueue)} method
   *
   * @exception NullPointerException if {@code knownObjects} is {@code
   * null}
   *
   * @see #accept(EventQueue)
   */
  protected AbstractEventDistributor(final Map<Object, T> knownObjects) {
    super();
    this.knownObjects = Objects.requireNonNull(knownObjects);
  }


  /*
   * Instance methods.
   */
  

  /**
   * Reads all {@link Event}s from the supplied {@link EventQueue} in
   * order and creates and {@linkplain #fireEvent(Event) distributes}
   * new {@link Event}s for them that incorporate appropriate
   * {@linkplain Event#getPriorResource() prior resource state}
   * derived from the {@link Map} of resources supplied at {@linkplain
   * #AbstractEventDistributor(Map) construction time}.
   *
   * @param eventQueue the {@link EventQueue} to read; may be {@code
   * null} in which case no action will be taken; <strong>will be
   * synchronized on</strong> by this method
   *
   * @exception EventQueueCollection.TransientException if a transient
   * error was encountered that might be cleared if the supplied
   * {@code eventQueue} were requeued and consumed again later
   *
   * @see #fireEvent(Event)
   */
  @Override
  public final void accept(final EventQueue<T> eventQueue) {
    if (eventQueue != null) {
      synchronized (eventQueue) {
        final Object key = eventQueue.getKey();
        if (key == null) {
          throw new IllegalStateException("eventQueue.getKey() == null; eventQueue: " + eventQueue);
        }
        for (final Event<T> event : eventQueue) {
          if (event != null) {
            assert key.equals(event.getKey());
            final Event.Type eventType = event.getType();
            assert eventType != null;
            final T newResource = event.getResource();
            if (event.getPriorResource() != null) {
              // TODO: unexpected state, but should just be ignored or
              // logged
            }
            final T priorResource;
            final Event.Type newEventType;
            synchronized (this.knownObjects) {
              priorResource = this.knownObjects.get(key);
              if (eventType.equals(Event.Type.DELETION)) {
                this.knownObjects.remove(key);
                newEventType = Event.Type.DELETION;
              } else {
                assert eventType.equals(Event.Type.ADDITION) || eventType.equals(Event.Type.MODIFICATION) || eventType.equals(Event.Type.SYNCHRONIZATION);
                this.knownObjects.put(key, event.getResource());
                if (priorResource == null) {
                  newEventType = Event.Type.ADDITION;
                } else if (eventType.equals(Event.Type.SYNCHRONIZATION)) {
                  newEventType = Event.Type.SYNCHRONIZATION;
                } else {
                  newEventType = Event.Type.MODIFICATION;
                }
              }
            }
            this.fireEvent(new Event<>(this, newEventType, priorResource, newResource));
          }
        }
      }
    }
  }

  /**
   * Distributes the supplied {@link Event} in some way.
   *
   * @param event the {@link Event} to distribute; must not be {@code null}
   *
   * @exception NullPointerException if {@code event} is {@code null}
   */
  protected abstract void fireEvent(final Event<T> event);
  
}
