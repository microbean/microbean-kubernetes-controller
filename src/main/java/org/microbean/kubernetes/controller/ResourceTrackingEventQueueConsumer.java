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

import java.util.Map;
import java.util.Objects;

import java.util.function.Consumer;

import java.util.logging.Level;
import java.util.logging.Logger;

import io.fabric8.kubernetes.api.model.HasMetadata;

import net.jcip.annotations.GuardedBy;

/**
 * A {@link Consumer} of {@link EventQueue}s that tracks the
 * Kubernetes resources they contain before allowing subclasses to
 * process their individual {@link Event}s.
 *
 * <p>Typically you would supply an implementation of this class to a
 * {@link Controller}.</p>
 *
 * @author <a href="https://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 *
 * @see #accept(Event.Type, HasMetadata, HasMetadata)
 *
 * @see Controller
 */
public abstract class ResourceTrackingEventQueueConsumer<T extends HasMetadata> implements Consumer<EventQueue<? extends T>> {


  /*
   * Instance fields.
   */

  
  /**
   * A mutable {@link Map} of {@link HasMetadata} objects indexed by
   * their keys (often a pairing of namespace and name).
   *
   * <p>This field may be {@code null} in which case no resource
   * tracking will take place.</p>
   *
   * <p>The value of this field is {@linkplain
   * #AbstractEventDistributor(Map) supplied at construction time} and
   * is <strong>synchronized on</strong> and written to, if non-{@code
   * null}, by the {@link #accept(EventQueue)} method.</p>
   *
   * <p>This class <strong>synchronizes on this field's
   * value</strong>, if it is non-{@code null}, when mutating its
   * contents.</p>
   */
  @GuardedBy("itself")
  private final Map<Object, T> knownObjects;

  /**
   * A {@link Logger} for use by this {@link
   * ResourceTrackingEventQueueConsumer} implementation.
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
   * Creates a new {@link ResourceTrackingEventQueueConsumer}.
   *
   * @param knownObjects a mutable {@link Map} of {@link HasMetadata}
   * objects indexed by their keys (often a pairing of namespace and
   * name); must not be {@code null}; <strong>will have its contents
   * changed</strong> by this {@link
   * ResourceTrackingEventQueueConsumer}'s {@link #accept(EventQueue)}
   * method; <strong>will be synchronized on</strong> by this {@link
   * ResourceTrackingEventQueueConsumer}'s {@link #accept(EventQueue)}
   * method
   *
   * @see #accept(EventQueue)
   */
  protected ResourceTrackingEventQueueConsumer(final Map<Object, T> knownObjects) {
    super();
    this.logger = this.createLogger();
    if (this.logger == null) {
      throw new IllegalStateException("createLogger() == null");
    }
    final String cn = this.getClass().getName();
    final String mn = "<init>";
    if (this.logger.isLoggable(Level.FINER)) {
      final String knownObjectsString;
      if (knownObjects == null) {
        knownObjectsString = null;
      } else {
        synchronized (knownObjects) {
          knownObjectsString = knownObjects.toString();
        }
      }      
      this.logger.entering(cn, mn, knownObjectsString);
    }
    this.knownObjects = knownObjects;
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }


  /*
   * Instance methods.
   */


  /**
   * Returns a {@link Logger} for use with this {@link
   * ResourceTrackingEventQueueConsumer}.
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
   * {@linkplain EventQueue#iterator() Loops through} all the {@link
   * Event}s in the supplied {@link EventQueue}, keeping track of the
   * {@link HasMetadata} it concerns along the way by
   * <strong>synchronizing on</strong> and writing to the {@link Map}
   * {@linkplain #ResourceTrackingEventQueueConsumer(Map) supplied at
   * construction time}.
   *
   * <p>Individual {@link Event}s are forwarded on to the {@link
   * #accept(Event.Type, HasMetadata, HasMetadata)} method.</p>
   *
   * @param eventQueue the {@link EventQueue} to process; may be
   * {@code null} in which case no action will be taken
   *
   * @see #accept(Event.Type, HasMetadata, HasMetadata)
   */
  @Override
  public final void accept(final EventQueue<? extends T> eventQueue) {
    final String cn = this.getClass().getName();
    final String mn = "accept";
    if (eventQueue == null) {
      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.entering(cn, mn, null);
      }
    } else {
      synchronized (eventQueue) {
        if (this.logger.isLoggable(Level.FINER)) {
          this.logger.entering(cn, mn, eventQueue);
        }
        final Object key = eventQueue.getKey();
        if (key == null) {
          throw new IllegalStateException("eventQueue.getKey() == null; eventQueue: " + eventQueue);
        }        
        for (final Event<? extends T> event : eventQueue) {
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
            if (this.knownObjects == null) {
              priorResource = null;
              newEventType = eventType;
            } else {
              synchronized (this.knownObjects) {
                priorResource = this.knownObjects.get(key);
                if (eventType.equals(Event.Type.DELETION)) {
                  this.knownObjects.remove(key);
                  newEventType = Event.Type.DELETION;
                } else {
                  assert eventType.equals(Event.Type.ADDITION) || eventType.equals(Event.Type.MODIFICATION) || eventType.equals(Event.Type.SYNCHRONIZATION);
                  this.knownObjects.put(key, newResource);
                  if (priorResource == null) {
                    newEventType = Event.Type.ADDITION;
                  } else if (eventType.equals(Event.Type.SYNCHRONIZATION)) {
                    newEventType = Event.Type.SYNCHRONIZATION;
                  } else {
                    newEventType = Event.Type.MODIFICATION;
                  }
                }
              }
            }
            this.accept(newEventType, priorResource, newResource);
          }
        }
      }
    }
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * Called to process the contents of a given {@link Event} from the
   * {@link EventQueue} supplied to the {@link #accept(EventQueue)}
   * method, <strong>with that {@link EventQueue}'s monitor
   * held</strong>.
   *
   * <p>Implementations of this method should be relatively fast as
   * this method dictates the speed of {@link EventQueue}
   * processing.</p>
   *
   * @param eventType the {@link Event.Type} describing the event
   * encountered in the {@link EventQueue}; must not be {@code null}
   *
   * @param priorResource the <em>prior state</em> of the Kubernetes
   * resource this logical event concerns; may be {@code null}
   *
   * @param resource the Kubernetes resource this logical event
   * concerns; must not be {@code null}
   *
   * @exception NullPointerException if {@code eventType} or {@code
   * resource} is {@code null}
   *
   * @see #accept(EventQueue)
   *
   * @see Event#Event(Object, Event.Type, HasMetadata, HasMetadata)
   */
  protected abstract void accept(final Event.Type eventType, final T priorResource, final T resource);
  
}
