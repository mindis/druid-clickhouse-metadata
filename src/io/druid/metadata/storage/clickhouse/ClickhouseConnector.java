/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.metadata.storage.clickhouse;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import ru.yandex.clickhouse.ClickHouseDriver;

import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.BooleanMapper;

import java.sql.SQLException;
import java.util.List;

public class ClickhouseConnector extends SQLMetadataConnector
{
  private static final Logger log = new Logger(ClickhouseConnector.class);
  private static final String PAYLOAD_TYPE = "LONGBLOB";
  private static final String SERIAL_TYPE = "BIGINT(20) AUTO_INCREMENT";
  private static final String QUOTE_STRING = "`";

  public String fileName = "";
  public String format = "";
  
  private final DBI dbi;

  @Inject
  public ClickhouseConnector(Supplier<MetadataStorageConnectorConfig> config, Supplier<MetadataStorageTablesConfig> dbTables)
  {
    super(config, dbTables);

    final BasicDataSource datasource = getDatasource();
    // ClickHouseDriver driver is classloader isolated as part of the extension
    // so we need to help JDBC find the driver
    datasource.setDriverClassLoader(getClass().getClassLoader());
    datasource.setDriverClassName("ru.yandex.clickhouse.ClickHouseDriver");


    this.dbi = new DBI(datasource);

    log.info("Configured Clickhouse as metadata storage");
  }

  @Override
  protected String getPayloadType()
  {
    return PAYLOAD_TYPE;
  }

  @Override
  protected String getSerialType()
  {
    return SERIAL_TYPE;
  }

  @Override
  public String getQuoteString()
  {
    return QUOTE_STRING;
  }

  public String getFileName() {
	return fileName;
  }

  public void setFileName(String fileName) {
	this.fileName = fileName;
  }

  public String getFormat() {
	return format;
  }

  public void setFormat(String format) {
	this.format = format;
  }


  @Override
  protected int getStreamingFetchSize()
  {
    return Integer.MIN_VALUE;
  }

  

  
  @Override
  public boolean tableExists(Handle handle, String tableName)
  {

	String filename = getFileName();
	String format = getFormat();

    List list  = handle.createQuery("EXISTS TABLE :tableName [INTO OUTFILE :filename] [FORMAT :format]")
    				.bind("filename", filename)
                  .bind("tableName", tableName)
                  .bind("format", format)
                  .list();
    if(Integer.parseInt(list.get(0).toString()) == 1)
    	 return true;
    else
    	return false;
    
  }

 

  @Override
  public Void insertOrUpdate(
      final String tableName,
      final String keyColumn,
      final String valueColumn,
      final String key,
      final byte[] value
  ) throws Exception
  {
    return getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            handle.createStatement(
                StringUtils.safeFormat(
                    "INSERT INTO %1$s (%2$s, %3$s) VALUES (:key, :value) ",
                    tableName,
                    keyColumn,
                    valueColumn
                )
            )
                  .bind("key", key)
                  .bind("value", value)
                  .execute();
            return null;
          }
        }
    );
  }

  @Override
  public DBI getDBI()
  {
    return dbi;
  }
}