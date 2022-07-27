package alluxio.master;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import alluxio.concurrent.jsr.CompletableFuture;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.JournalUtils;
import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

public class FaultTolerantAlluxioMasterProcessTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  ControllablePrimarySelector mPrimarySelector;
  JournalType mJournalType = JournalType.EMBEDDED;
  JournalSystem mJournalSystem;
  FaultTolerantAlluxioMasterProcess mMasterProcess;

  CompletableFuture<Void> mFuture;

  @Before
  public void before() throws IOException {
    Configuration.set(PropertyKey.MASTER_JOURNAL_TYPE, mJournalType);
    Configuration.set(PropertyKey.MASTER_JOURNAL_FOLDER, mFolder.newFolder("journal"));
    Configuration.set(PropertyKey.MASTER_METASTORE_DIR, mFolder.newFolder("metastore"));
    Configuration.set(PropertyKey.MASTER_BACKUP_DIRECTORY, mFolder.newFolder("backups"));

    URI journalLocation = JournalUtils.getJournalLocation();
    mJournalSystem = new JournalSystem.Builder()
        .setLocation(journalLocation)
        .build(CommonUtils.ProcessType.MASTER);
    mPrimarySelector = new ControllablePrimarySelector();
    mPrimarySelector.setState(PrimarySelector.State.STANDBY);
    mMasterProcess = Mockito.spy(new FaultTolerantAlluxioMasterProcess(mJournalSystem,
      mPrimarySelector));
  }

  @Test
  public void losePrimacyEnteringGainPrimacy() throws Exception {
    // make master lose primacy immediately after gaining it, only on the first gainPrimacy
    // invocation
    AtomicBoolean isFirst = new AtomicBoolean(true);
    Mockito.doAnswer(mock -> {
      mPrimarySelector.setState(PrimarySelector.State.STANDBY);
      return mock.callRealMethod();
    }).when(mMasterProcess).gainPrimacy();

    mFuture = CompletableFuture.runAsync(() -> {
      try {
        mMasterProcess.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    mPrimarySelector.setState(PrimarySelector.State.PRIMARY);
    mPrimarySelector.waitForState(PrimarySelector.State.STANDBY);
    verify(mMasterProcess, timeout(1_000).times(1)).losePrimacy();
    // once at the beginning, once during lose primacy
    verify(mMasterProcess, timeout(1_000).times(2)).startMasters(false);

    Mockito.doAnswer(mock -> {
      Boolean b = (Boolean) mock.callRealMethod();
      Assert.assertTrue("must gain primacy the second time", b);
      Configuration.set(PropertyKey.MASTER_JOURNAL_EXIT_ON_DEMOTION, true);
      mPrimarySelector.setState(PrimarySelector.State.STANDBY);
      return true;
    }).when(mMasterProcess).gainPrimacy();
    mPrimarySelector.setState(PrimarySelector.State.PRIMARY);
    mFuture.get();
  }
}
