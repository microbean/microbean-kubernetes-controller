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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.Pod;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;

import io.fabric8.kubernetes.client.dsl.Listable;
import io.fabric8.kubernetes.client.dsl.VersionWatchable;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import io.fabric8.kubernetes.client.Watcher;

@Deprecated
public class TestReflectorBasics {

  public TestReflectorBasics() {
    super();
  }

  @Test
  public void testBasics() throws Exception {
    assumeFalse(Boolean.getBoolean("skipClusterTests"));
    final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
    assertNotNull(executorService);
    final DefaultKubernetesClient client = new DefaultKubernetesClient();
    final Reflector<ConfigMap> reflector =
      new Reflector<ConfigMap>(client.configMaps(),
                               executorService,
                               Duration.ofSeconds(10));
    System.out.println("*** running reflector");
    reflector.start();
    System.out.println("*** sleeping");
    Thread.sleep(10L * 60L * 1000L);
    System.out.println("*** closing reflector");
    reflector.close();
    System.out.println("*** reflector closed");
    System.out.println("*** closing client");
    client.close();
    System.out.println("*** client closed");
  }

  public static final void main(final String[] args) throws Exception {
    new TestReflectorBasics().testBasics();
  }

}
