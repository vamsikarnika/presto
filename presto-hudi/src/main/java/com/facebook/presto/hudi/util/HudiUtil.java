/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.hudi.util;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.org.apache.avro.Schema;
import org.apache.hudi.storage.StorageConfiguration;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static com.facebook.presto.hudi.HudiErrorCode.HUDI_FILESYSTEM_ERROR;
import static com.facebook.presto.hudi.HudiErrorCode.HUDI_INVALID_METADATA;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConfWithCopy;

public final class HudiUtil
{
    private static final Logger log = Logger.get(HudiUtil.class);
    private static final Cache<Schema, Map<String, Schema.Field>> SCHEMA_FIELD_CACHE = CacheBuilder.newBuilder()
            .maximumSize(100)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .build();

    private HudiUtil() {}

    private static Map<String, Schema.Field> buildFieldLookup(Schema schema)
    {
        return schema.getFields().stream()
                .collect(Collectors.toMap(
                        f -> f.name().toLowerCase(Locale.ROOT),
                        f -> f));
    }

    /**
     * Retrieves a field from the given Avro schema by column name.
     * <p>
     * The lookup proceeds in two steps:
     * <ul>
     *   <li>First, attempts an exact match on the column name.</li>
     *   <li>If not found, falls back to a case-insensitive match using a cached lookup table</li>
     * </ul>
     * <p>
     *
     * @param columnName Column name to search for.
     * @param schema Avro {@link Schema} in which to search.
     * @return The matching {@link Schema.Field}, if found.
     * @throws PrestoException if no field matches the given column name.
     */
    public static Schema.Field getFieldFromSchema(String columnName, Schema schema)
    {
        Schema.Field field = schema.getField(columnName);
        if (field != null) {
            return field;
        }

        try {
            field = SCHEMA_FIELD_CACHE
                    .get(schema, () -> buildFieldLookup(schema)).get(columnName.toLowerCase(Locale.ROOT));
            if (field != null) {
                return field;
            }
        }
        catch (ExecutionException e) {
            throw new PrestoException(HUDI_BAD_DATA,
                    "Failed to build field lookup for schema", e);
        }

        throw new PrestoException(HUDI_BAD_DATA,
                "Failed to get column " + columnName + " from table schema");
    }

    public static Schema getLatestTableSchema(HoodieTableMetaClient metaClient, String tableName)
    {
        try {
            HoodieTimer timer = HoodieTimer.start();
            Schema schema = new TableSchemaResolver(metaClient).getTableAvroSchema();
            log.info("Fetched table schema for table %s in %s ms", tableName, timer.endTimer());
            return schema;
        }
        catch (Exception e) {
            // failed to read schema
            throw new PrestoException(HUDI_FILESYSTEM_ERROR, e);
        }
    }

    public static HoodieTableMetaClient buildTableMetaClient(
            ExtendedFileSystem fileSystem,
            String tableName,
            String basePath)
    {
        try {
            StorageConfiguration<Configuration> conf = getStorageConfWithCopy(fileSystem.getConf());
            return HoodieTableMetaClient.builder().setConf(conf).setBasePath(basePath).build();
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(HUDI_BAD_DATA,
                    "Location of table %s does not contain Hudi table metadata: %s".formatted(tableName, basePath));
        }
        catch (Throwable e) {
            throw new PrestoException(HUDI_INVALID_METADATA,
                    "Unable to load Hudi meta client for table %s (%s)".formatted(tableName, basePath));
        }
    }

    public static ExtendedFileSystem getFileSystem(ConnectorSession session, HdfsEnvironment hdfsEnvironment, String schemaName, String tableName, String basePath)
    {
        HdfsContext hdfsContext = new HdfsContext(
                session,
                schemaName,
                tableName,
                basePath,
                false);
        try {
            return hdfsEnvironment.getFileSystem(hdfsContext, new Path(basePath));
        }
        catch (IOException e) {
            throw new PrestoException(HUDI_FILESYSTEM_ERROR, "Could not open file system for " + tableName, e);
        }
    }
}
