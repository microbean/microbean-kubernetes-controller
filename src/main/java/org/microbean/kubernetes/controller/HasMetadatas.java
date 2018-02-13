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

import java.util.logging.Level;
import java.util.logging.Logger;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;

/**
 * A utility class for working with {@link HasMetadata} resources.
 *
 * @author <a href="https://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 *
 * @see #getKey(HasMetadata)
 *
 * @see HasMetadata
 */
public final class HasMetadatas {


  /*
   * Constructors.
   */


  /**
   * Creates a new {@link HasMetadatas}.
   */
  private HasMetadatas() {
    super();
  }


  /*
   * Static methods.
   */
  

  /**
   * Returns a <em>key</em> for the supplied {@link HasMetadata}
   * derived from its {@linkplain ObjectMeta#getName() name} and
   * {@linkplain ObjectMeta#getNamespace() namespace}.
   *
   * <p>This method may return {@code null}.</p>
   *
   * @param resource the {@link HasMetadata} for which a key should be
   * returned; may be {@code null} in which case {@code null} will be
   * returned
   *
   * @return a key for the supplied {@link HasMetadata}
   *
   * @exception IllegalStateException if the supplied {@link
   * HasMetadata}'s {@linkplain ObjectMeta metadata}'s {@link
   * ObjectMeta#getName()} method returns {@code null} or an
   * {@linkplain String#isEmpty() empty} {@link String}
   *
   * @see HasMetadata#getMetadata()
   *
   * @see ObjectMeta#getName()
   *
   * @see ObjectMeta#getNamespace()
   */
  public static final Object getKey(final HasMetadata resource) {
    final String cn = HasMetadatas.class.getName();
    final String mn = "getKey";
    final Logger logger = Logger.getLogger(cn);
    assert logger != null;
    if (logger.isLoggable(Level.FINER)) {
      logger.entering(cn, mn, resource);
    }
    
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

    if (logger.isLoggable(Level.FINER)) {
      logger.exiting(cn, mn, returnValue);
    }
    return returnValue;
  }
  
}
