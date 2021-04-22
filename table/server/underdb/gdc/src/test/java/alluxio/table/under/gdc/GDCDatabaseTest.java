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

package alluxio.table.under.gdc;

import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class GDCDatabaseTest {

  private static final String DB_NAME = "sds_test2";
  private static final Map<String, String> CONF = new HashMap<>();

  @Rule
  public ExpectedException mExpection = ExpectedException.none();

  private UdbContext mUdbContext;
  private UdbConfiguration mUdbConf;

  @Before
  public void before() {
    mUdbContext =
            new UdbContext(null, null, "gdc", "thrift://not_running:9083", DB_NAME, DB_NAME);
    mUdbConf = new UdbConfiguration(CONF);
  }

  @Test
  public void testGetDbInfo() throws IOException {
    UdbContext udbContext =
            new UdbContext(null, null, "gdc", "", DB_NAME, DB_NAME);
    UdbConfiguration udbConfig = new UdbConfiguration(ImmutableMap.of());
    GDCDatabase db = GDCDatabase.create(udbContext, udbConfig);

    db.getDatabaseInfo();
  }

  @Test
  public void create() {
    assertEquals(DB_NAME, GDCDatabase.create(mUdbContext, mUdbConf).getName());
  }

  @Test
  public void createEmptyName() {
    mExpection.expect(IllegalArgumentException.class);
    UdbContext udbContext =
            new UdbContext(null, null, "gdc", "thrift://not_running:9083", "", DB_NAME);
    assertEquals(DB_NAME,
            GDCDatabase.create(udbContext, new UdbConfiguration(ImmutableMap.of())).getName());
  }

  @Test
  public void createNullName() {
    mExpection.expect(IllegalArgumentException.class);
    UdbContext udbContext =
            new UdbContext(null, null, "gdc", "thrift://not_running:9083", null, DB_NAME);
    assertEquals(DB_NAME,
            GDCDatabase.create(udbContext, new UdbConfiguration(ImmutableMap.of())).getName());
  }
}
