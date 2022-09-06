/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.microservices.journaling;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalUtils;
import alluxio.master.microservices.MasterProcessMicroservice;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.CommonUtils;

import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * Microservice that takes care of starting, promoting, and demoting the master's journal system.
 */
public class JournalingMicroservice implements MasterProcessMicroservice {
  private static final Logger LOG = LoggerFactory.getLogger(JournalingMicroservice.class);
  private final JournalSystem mJournalSystem;

  private JournalingMicroservice(JournalSystem journalSystem) {
    mJournalSystem = journalSystem;
  }

  /**
   * @return the journal system being used in the master process
   */
  public JournalSystem getJournalSystem() {
    return mJournalSystem;
  }

  @Override
  public void start() {
    mJournalSystem.start();
    if (Configuration.getBoolean(PropertyKey.MASTER_JOURNAL_CATCHUP_PROTECT_ENABLED)) {
      LOG.info("Waiting for journals to catch up.");
      mJournalSystem.waitForCatchup();
    }
  }

  @Override
  public void promote() {
    try (Timer.Context ignored = MetricsSystem
        .timer(MetricKey.MASTER_JOURNAL_GAIN_PRIMACY_TIMER.getName()).time()) {
      mJournalSystem.gainPrimacy();
    }
  }

  @Override
  public void demote() {
    mJournalSystem.losePrimacy();
    if (Configuration.getBoolean(PropertyKey.MASTER_JOURNAL_CATCHUP_PROTECT_ENABLED)) {
      LOG.info("Waiting for journals to catch up.");
      mJournalSystem.waitForCatchup();
    }
  }

  @Override
  public void stop() {
    mJournalSystem.stop();
  }

  /**
   * @return the journaling microservice
   */
  public static MasterProcessMicroservice create() {
    URI journalLocation = JournalUtils.getJournalLocation();
    return new JournalingMicroservice(new JournalSystem.Builder()
        .setLocation(journalLocation).build(CommonUtils.ProcessType.MASTER));
  }
}
