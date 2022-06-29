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

package alluxio;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.ThreadParams;

import java.util.Random;

/**
 * Captures commonly reused thread state such as ID and file name and depth to poll.
 */
@State(Scope.Thread)
public class BaseThreadState {
  private static final long RAND_SEED = 12345;

  protected int mMyId;
  protected int mNxtDepth;
  protected long []mNxtFileId;
  protected int mNumFiles;

  @Setup(Level.Iteration)
  public void init(BaseFileStructure structure, ThreadParams params) {
    mNumFiles = structure.mFileCount;
    mNxtFileId = new long[structure.mDepth + 1];
    mMyId = params.getThreadIndex();
    Random rand = new Random(RAND_SEED + mMyId);
    for (int i = 0; i <= structure.mDepth; i++) {
      mNxtFileId[i] = rand.nextInt(mNumFiles);
    }
    mNxtDepth = rand.nextInt(structure.mDepth + 1);
  }

  public int nextDepth(BaseFileStructure structure) {
    return structure.mDepthGenerator.nextValue().intValue();
  }

  public long nxtFileId(BaseFileStructure structure) {
    return structure.mFileGenerator.nextValue().longValue();
  }
}
