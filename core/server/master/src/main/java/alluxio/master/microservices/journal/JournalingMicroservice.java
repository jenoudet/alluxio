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

package alluxio.master.microservices.journal;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.grpc.BackupStatusPRequest;
import alluxio.grpc.ErrorType;
import alluxio.master.journal.JournalSystem;
import alluxio.master.meta.DefaultMetaMaster;
import alluxio.master.microservices.MasterProcessMicroservice;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.wire.BackupStatus;

import com.codahale.metrics.Timer;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Microservice that takes care of starting, promoting, and demoting the master's journal system.
 */
public class JournalingMicroservice implements MasterProcessMicroservice {
  private static final Logger LOG = LoggerFactory.getLogger(JournalingMicroservice.class);
  private final JournalSystem mJournalSystem;
  // used to take emergency backups
  private final DefaultMetaMaster mMetaMaster;

  private JournalingMicroservice(JournalSystem journalSystem, DefaultMetaMaster metaMaster) {
    mJournalSystem = journalSystem;
    mMetaMaster = metaMaster;
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
    } catch (Throwable t) {
      if (Configuration.getBoolean(PropertyKey.MASTER_JOURNAL_BACKUP_WHEN_CORRUPTED)) {
        takeEmergencyBackup();
      }
      throw new AlluxioRuntimeException(Status.INTERNAL, "Error in journal when gaining primacy",
          t, ErrorType.Internal, false);
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

  private void takeEmergencyBackup() {
    LOG.warn("Emergency backup triggered");
    try {
      BackupStatus backup = mMetaMaster.takeEmergencyBackup();
      BackupStatusPRequest statusRequest =
          BackupStatusPRequest.newBuilder().setBackupId(backup.getBackupId().toString()).build();
      final int requestIntervalMs = 2_000;
      CommonUtils.waitFor("emergency backup to complete", () -> {
        try {
          BackupStatus status = mMetaMaster.getBackupStatus(statusRequest);
          LOG.info("Auto backup state: {} | Entries processed: {}.", status.getState(),
              status.getEntryCount());
          return status.isCompleted();
        } catch (AlluxioException e) {
          return false;
        }
        // no need for timeout on shutdown, we must wait until the backup is complete
      }, WaitForOptions.defaults().setInterval(requestIntervalMs).setTimeoutMs(Integer.MAX_VALUE));
    } catch (Exception e) {
      LOG.error("Emergency backup failed", e);
    }
  }

  /**
   * @param journalSystem the journal system that this microservice will manage
   * @param metaMaster the meta master used to take an emergency backup in case of journal
   *                   corruption
   * @return the journaling microservice
   */
  public static MasterProcessMicroservice create(
      JournalSystem journalSystem,
      DefaultMetaMaster metaMaster
  ) {
    return new JournalingMicroservice(journalSystem, metaMaster);
  }
}
