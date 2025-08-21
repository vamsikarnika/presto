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
package com.facebook.presto.hudi.partition;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hudi.HudiColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import org.apache.hadoop.fs.Path;

import java.util.List;

import static com.facebook.presto.HudiUtil.buildPartitionKeys;
import static com.facebook.presto.HudiUtil.partitionMatchesPredicates;
import static com.google.common.base.MoreObjects.toStringHelper;

public class HiveHudiPartitionInfo
        implements HudiPartitionInfo
{
    public static final String NON_PARTITION = "";
    private final List<HudiColumnHandle> partitionColumnHandles;
    private final TupleDomain<HudiColumnHandle> constraintSummary;
    private final String hivePartitionName;
    private final String relativePartitionPath;
    private final List<HivePartitionKey> hivePartitionKeys;
    private final SchemaTableName schemaTableName;
    private final List<Type> partitionColumnTypes;

    public HiveHudiPartitionInfo(
            SchemaTableName schemaTableName,
            Path tableLocation,
            String hivePartitionName,
            Partition partition,
            List<HudiColumnHandle> partitionColumnHandles,
            List<Type> partitionColumnTypes,
            TupleDomain<HudiColumnHandle> constraintSummary)
    {
        this.schemaTableName = schemaTableName;
        this.partitionColumnHandles = partitionColumnHandles;
        this.constraintSummary = constraintSummary;
        this.hivePartitionName = hivePartitionName;
        this.relativePartitionPath = getRelativePartitionPath(
                tableLocation,
                new Path(partition.getStorage().getLocation()));
        this.hivePartitionKeys = buildPartitionKeys(
                partitionColumnHandles, partition.getValues());
        this.partitionColumnTypes = partitionColumnTypes;
    }

    @Override
    public String getRelativePartitionPath()
    {
        return relativePartitionPath;
    }

    @Override
    public List<HivePartitionKey> getHivePartitionKeys()
    {
        return hivePartitionKeys;
    }

    @Override
    public boolean doesMatchPredicates()
    {
        if (hivePartitionName.equals(NON_PARTITION)) {
            return true;
        }
        return partitionMatchesPredicates(schemaTableName, hivePartitionName, partitionColumnHandles, partitionColumnTypes, constraintSummary);
    }

    public String getHivePartitionName()
    {
        return hivePartitionName;
    }

    private static String getRelativePartitionPath(Path baseLocation, Path fullPartitionLocation)
    {
        String basePath = baseLocation.getName();
        String fullPartitionPath = fullPartitionLocation.getName();

        if (!fullPartitionPath.startsWith(basePath)) {
            throw new IllegalArgumentException("Partition [" + fullPartitionPath + "] does not belong to base-location " + basePath);
        }

        String baseLocationParent = baseLocation.getParent().toUri().getPath();
        String baseLocationName = baseLocation.getName();
        int partitionStartIndex = fullPartitionPath.indexOf(
                baseLocationName,
                baseLocationParent == null ? 0 : baseLocationParent.length());
        // Partition-Path could be empty for non-partitioned tables
        boolean isNonPartitionedTable = partitionStartIndex + baseLocationName.length() == fullPartitionPath.length();
        return isNonPartitionedTable ? NON_PARTITION : fullPartitionPath.substring(partitionStartIndex + baseLocationName.length() + 1);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hivePartitionName", hivePartitionName)
                .add("hivePartitionKeys", hivePartitionKeys)
                .toString();
    }
}