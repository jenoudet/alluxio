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

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.ErrorType;
import alluxio.master.BackupManager;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.SafeModeManager;
import alluxio.master.journal.JournalSystem;
import alluxio.master.microservices.MasterProcessMicroservice;
import alluxio.resource.CloseableResource;
import alluxio.underfs.MasterUfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.URIUtils;

import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Microservice that manages the lifecycle of journaled internal processes, i.e.
 * {@link alluxio.master.CoreMaster}.
 */
public class JournaledMicroservice implements MasterProcessMicroservice {
  private static final Logger LOG = LoggerFactory.getLogger(JournaledMicroservice.class);

  private final JournalSystem mJournalSystem;
  private final MasterRegistry mRegistry;
  private final BackupManager mBackupManager;
  private final SafeModeManager mSafeModeManager;
  private final MasterUfsManager mMasterUfsManager;
  private final CoreMasterContext mMasterContext;

  JournaledMicroservice(
      JournalSystem journalSystem,
      MasterRegistry registry,
      BackupManager backupManager,
      SafeModeManager safeModeManager,
      MasterUfsManager ufsManager,
      CoreMasterContext context
  ) {
    mJournalSystem = journalSystem;
    mRegistry = registry;
    mBackupManager = backupManager;
    mSafeModeManager = safeModeManager;
    mMasterUfsManager = ufsManager;
    mMasterContext = context;
  }

  @Override
  public synchronized void start() {
    try {
      mRegistry.start(false);
    } catch (IOException e) {
      throw new AlluxioRuntimeException(Status.INTERNAL, "Failed to start masters", e,
          ErrorType.Internal, false);
    }
    // Signal state-lock-manager that masters are ready.
    mMasterContext.getStateLockManager().mastersStartedCallback();
    LOG.info("All masters started.");
  }

  @Override
  public synchronized void promote() {
    try {
      LOG.info("Stopping all masters.");
      mRegistry.stop();
      LOG.info("All masters stopped.");
    } catch (IOException e) {
      throw new AlluxioRuntimeException(Status.INTERNAL, "Failed to stop masters", e,
          ErrorType.Internal, false);
    }

    if (Configuration.isSet(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP)) {
      AlluxioURI backup = new AlluxioURI(Configuration.getString(
              PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP));
      if (mJournalSystem.isEmpty()) {
        initFromBackup(backup);
      } else {
        LOG.info("The journal system is not freshly formatted, skipping restoring backup from {}",
            backup);
      }
    }
    mSafeModeManager.notifyPrimaryMasterStarted();
    try {
      mRegistry.start(true);
    } catch (UnavailableException e) {
      LOG.warn("Failed to start masters as primary, likely due to the process having already been"
          + " demoted");
      return;
    } catch (IOException e) {
      throw new AlluxioRuntimeException(Status.INTERNAL, "Failed to start masters", e,
          ErrorType.Internal, false);
    }
    mMasterContext.getStateLockManager().mastersStartedCallback();
    LOG.info("All masters started as primary.");
  }

  @Override
  public synchronized void demote() {
    try {
      mRegistry.stop();
    } catch (IOException e) {
      throw new AlluxioRuntimeException(Status.INTERNAL, "Failed to stop masters", e,
          ErrorType.Internal, false);
    }
    start(); // restart masters as standby
  }

  @Override
  public synchronized void stop() {
    LOG.info("Closing all masters.");
    try {
      mRegistry.close();
    } catch (IOException e) {
      throw new AlluxioRuntimeException(Status.INTERNAL, "Failed to close masters", e,
          ErrorType.Internal, false);
    }
    LOG.info("Closed all masters.");
  }

  private void initFromBackup(AlluxioURI backup) {
    CloseableResource<UnderFileSystem> ufsResource;
    if (URIUtils.isLocalFilesystem(backup.toString())) {
      UnderFileSystem ufs = UnderFileSystem.Factory.create("/",
          UnderFileSystemConfiguration.defaults(Configuration.global()));
      ufsResource = new CloseableResource<UnderFileSystem>(ufs) {
        @Override
        public void closeResource() { }
      };
    } else {
      ufsResource = mMasterUfsManager.getRoot().acquireUfsResource();
    }
    try (CloseableResource<UnderFileSystem> closeUfs = ufsResource;
         InputStream ufsIn = closeUfs.get().open(backup.getPath())) {
      LOG.info("Initializing metadata from backup {}", backup);
      mBackupManager.initFromBackup(ufsIn);
    } catch (IOException e) {
      LOG.error("Failed to initialize using the backup found at {}", backup);
    }
  }

  /**
   * @param journalSystem the journal system
   * @param registry core master registry
   * @param backupManager the backup manager
   * @param safeModeManager the safe mode manager
   * @param ufsManager the ufs manager
   * @param context the master context
   * @return a microservice that manages {@link alluxio.master.CoreMaster}s for the
   * {@link alluxio.master.AlluxioMasterProcess}
   */
  public static MasterProcessMicroservice create(
      JournalSystem journalSystem,
      MasterRegistry registry,
      BackupManager backupManager,
      SafeModeManager safeModeManager,
      MasterUfsManager ufsManager,
      CoreMasterContext context
  ) {
    return new JournaledMicroservice(journalSystem, registry, backupManager, safeModeManager,
        ufsManager, context);
  }
}
