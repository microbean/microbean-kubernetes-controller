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

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestInterruptionBehavior {

  private volatile Thread taskThread;
  
  public TestInterruptionBehavior() {
    super();
  }

  @Test
  public void testInterruptionAndScheduledThreadPoolExecutorInteraction() throws Exception {
    final ScheduledThreadPoolExecutor e = new ScheduledThreadPoolExecutor(1);
    e.scheduleWithFixedDelay(() -> {
        this.taskThread = Thread.currentThread();
        assertFalse(taskThread.isInterrupted());
        while (!this.taskThread.isInterrupted()) {
          try {
            Thread.sleep(200L);
          } catch (final InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            assertTrue(this.taskThread.isInterrupted());
          }
        }
        assertTrue(this.taskThread.isInterrupted());
      }, 0L, 1L, TimeUnit.MILLISECONDS);
    Thread.sleep(500L);
    this.taskThread.interrupt();
    Thread.sleep(500L);
    e.shutdown();
    assertFalse(e.awaitTermination(2L, TimeUnit.SECONDS));
    e.shutdownNow();
    assertTrue(e.awaitTermination(2L, TimeUnit.SECONDS));
  }
  
}
