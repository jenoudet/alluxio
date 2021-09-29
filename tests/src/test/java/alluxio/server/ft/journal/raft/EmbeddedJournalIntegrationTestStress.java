package alluxio.server.ft.journal.raft;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.NetAddress;
import alluxio.grpc.QuorumServerState;
import alluxio.master.journal.JournalType;
import alluxio.multi.process.MasterNetAddress;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import net.bytebuddy.utility.RandomString;
import org.junit.Assert;
import org.junit.Test;

public class EmbeddedJournalIntegrationTestStress extends EmbeddedJournalIntegrationTestBase {

  public static final int NUM_MASTERS = 5;
  public static final int NUM_WORKERS = 0;

  private long endTime;
  private int numExtraMasters = 15;
  private final AtomicBoolean fail = new AtomicBoolean(false);
  private final Set<AlluxioURI> dirNames = new HashSet<>();
  private final Set<AlluxioURI> fileNames = new HashSet<>();

  @Test
  public void chaosMonkey() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_CHAOS)
        .setClusterName("EmbeddedJournalTransferLeadership_transferLeadership")
        .setNumMasters(NUM_MASTERS)
        .setNumWorkers(NUM_WORKERS)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED.toString())
        .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "750ms")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1500ms")
        .build();
    mCluster.start();

    for (int i = 0; i < 1000; i++) {
      FileSystem fileSystemClient = mCluster.getFileSystemClient();
      AlluxioURI name = new AlluxioURI("/" + RandomString.make());
      System.out.printf("creating %s\n", name);
      if (i % 2 == 0) {
        fileSystemClient.createFile(name);
        fileNames.add(name);
      } else {
        fileSystemClient.createDirectory(name);
        dirNames.add(name);
      }
    }

    endTime = System.currentTimeMillis() + 3 * 60 * 1000;
    Thread clusterT = new Thread(() -> {
      try {
        clusterOps();
      } catch (Exception e) {
        e.printStackTrace();
        System.out.printf("time left: %ss", (endTime - System.currentTimeMillis()) / 1000);
        fail.set(true);
      }
    });
    Thread fsT = new Thread(() -> {
      try {
        fsOps();
      } catch (Exception e) {
        e.printStackTrace();
        System.out.printf("time left: %ss", (endTime - System.currentTimeMillis()) / 1000);
        fail.set(true);
      }
    });
    fsT.start();
    clusterT.start();
    fsT.join();
    clusterT.join();

    Assert.assertFalse(fail.get());

    FileSystem fsClient = mCluster.getFileSystemClient();
    for (AlluxioURI uri : dirNames) {
      Assert.assertTrue(fsClient.exists(uri));
    }
    for (AlluxioURI uri : fileNames) {
      Assert.assertTrue(fsClient.exists(uri));
    }

    mCluster.notifySuccess();
  }

  enum ClusterOperations {
    REMOVE_MASTER,
    START_MASTER,
    RESTART_MASTER,
    TRANSFER_LEADER,
  }

  enum FileSystemOperations {
    CREATE_DIR,
    CREATE_FILE,
    DELETE_DIR,
    DELETE_FILE,
//    MOVE_DIR,
//    MOVE_FILE
  }

  public static <T extends Enum<?>> T randomEnum(Class<T> clazz){
    int x = ThreadLocalRandom.current().nextInt(clazz.getEnumConstants().length);
    return clazz.getEnumConstants()[x];
  }

  private void clusterOps() throws Exception {
    while (System.currentTimeMillis() < endTime && !fail.get()) {
      ClusterOperations clusterOp = randomEnum(ClusterOperations.class);
      System.out.printf("running clusterOp: %s\n", clusterOp);
      int clusterSize = mCluster.getMasterAddresses().size();
      switch (clusterOp) {
        case REMOVE_MASTER:
          if (clusterSize == 2) {
            System.out.println("\tskip");
            continue;
          }
          int toRemove = ThreadLocalRandom.current().nextInt(clusterSize);
          MasterNetAddress netAddress = mCluster.getMasterAddresses().get(toRemove);
          mCluster.stopAndRemoveMaster(toRemove);
          mCluster.getJournalMasterClientForMaster().removeQuorumServer(
              masterEBJAddr2NetAddr(netAddress));
          waitForQuorumPropertySize(info -> true, clusterSize - 1);
          break;
        case START_MASTER:
          if (numExtraMasters == 0) {
            continue;
          }
          mCluster.startNewMasters(1, false);
          waitForQuorumPropertySize(info -> true, clusterSize + 1);
          numExtraMasters--;
          break;
        case RESTART_MASTER:
          if (clusterSize == 2) {
            System.out.println("\tskip");
            continue;
          }
          int toRestart = ThreadLocalRandom.current().nextInt(clusterSize);
          mCluster.stopMaster(toRestart);
          waitForQuorumPropertySize(info -> info.getServerState() == QuorumServerState.UNAVAILABLE,
              1);
          mCluster.startMaster(toRestart);
          waitForQuorumPropertySize(info -> info.getServerState() == QuorumServerState.AVAILABLE,
              clusterSize);
          break;
        case TRANSFER_LEADER:
          int newLeaderIdx = ThreadLocalRandom.current().nextInt(clusterSize);
          MasterNetAddress masterNetAddress = mCluster.getMasterAddresses().get(newLeaderIdx);
          NetAddress address = masterEBJAddr2NetAddr(masterNetAddress);
          mCluster.getJournalMasterClientForMaster().transferLeadership(address);
          waitForQuorumPropertySize(info -> info.getServerAddress().equals(address)
              && info.getIsLeader(), 1);
          break;
        default:
          System.out.println("not a thing");
          break;
      }
    }
  }

  private void fsOps() throws InterruptedException, IOException, AlluxioException {
    while (System.currentTimeMillis() < endTime && !fail.get()) {
      FileSystemOperations fsOp = randomEnum(FileSystemOperations.class);
      System.out.printf("running fsOp: %s\n", fsOp);
      switch (fsOp) {
        case CREATE_DIR:
          AlluxioURI dir = new AlluxioURI("/" + RandomString.make());
            mCluster.getFileSystemClient().createDirectory(dir);
          dirNames.add(dir);
          break;
        case CREATE_FILE:
          AlluxioURI file = new AlluxioURI("/" + RandomString.make());
            mCluster.getFileSystemClient().createFile(file);
          fileNames.add(file);
          break;
        case DELETE_DIR:
        case DELETE_FILE:
          List<AlluxioURI> alluxioURIS = new ArrayList<>(dirNames);
          alluxioURIS.addAll(fileNames);
          if (alluxioURIS.isEmpty()) {
            continue;
          }
          Collections.shuffle(alluxioURIS);
          AlluxioURI alluxioURI = alluxioURIS.get(0);
          mCluster.getFileSystemClient().delete(alluxioURI);
          if (dirNames.contains(alluxioURI)) {
            dirNames.remove(alluxioURI);
          } else {
            fileNames.remove(alluxioURI);
          }
          break;
        default:
          break;
      }
      Thread.sleep(5_000);
    }
  }
}
