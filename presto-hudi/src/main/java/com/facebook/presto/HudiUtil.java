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
package com.facebook.presto;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HivePartition;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.hive.PartitionNameWithVersion;
import com.facebook.presto.hudi.HudiColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveUtil.parsePartitionValue;
import static com.facebook.presto.hive.metastore.MetastoreUtil.checkCondition;
import static com.facebook.presto.hive.metastore.MetastoreUtil.extractPartitionValues;

public class HudiUtil
{

    public static boolean partitionMatchesPredicates(
            SchemaTableName tableName,
            String hivePartitionName,
            List<HudiColumnHandle> partitionColumnHandles,
            List<Type> partitionColumnTypes,
            TupleDomain<HudiColumnHandle> constraintSummary)
    {
        HivePartition partition = parsePartition(
                tableName, new PartitionNameWithVersion(hivePartitionName, Optional.empty()), partitionColumnHandles, partitionColumnTypes, null);

        return partitionMatches(partitionColumnHandles, constraintSummary, partition);
    }

    public static boolean partitionMatches(List<HudiColumnHandle> partitionColumns, TupleDomain<HudiColumnHandle> constraintSummary, HivePartition partition)
    {
        if (constraintSummary.isNone()) {
            return false;
        }
        Map<HudiColumnHandle, Domain> domains = constraintSummary.getDomains().orElseGet(ImmutableMap::of);
        for (HudiColumnHandle column : partitionColumns) {
            NullableValue value = partition.getKeys().get(column);
            Domain allowedDomain = domains.get(column);
            if (allowedDomain != null && !allowedDomain.includesNullableValue(value.getValue())) {
                return false;
            }
        }
        return true;
    }

    public static List<HivePartitionKey> buildPartitionKeys(List<HudiColumnHandle> keys, List<String> values)
    {
        checkCondition(keys.size() == values.size(), HIVE_INVALID_METADATA,
                "Expected %s partition key values, but got %s. Keys: %s, Values: %s.",
                keys.size(), values.size(), keys, values);
        ImmutableList.Builder<HivePartitionKey> partitionKeys = ImmutableList.builder();
        for (int i = 0; i < keys.size(); i++) {
            String name = keys.get(i).getName();
            String value = values.get(i);
            partitionKeys.add(new HivePartitionKey(name, Optional.of(value)));
        }
        return partitionKeys.build();
    }

    public static HivePartition parsePartition(
            SchemaTableName tableName,
            PartitionNameWithVersion partitionNameWithVersion,
            List<HudiColumnHandle> partitionColumns,
            List<Type> partitionColumnTypes,
            DateTimeZone timeZone)
    {
        List<String> partitionColumnNames = partitionColumns.stream()
                .map(HudiColumnHandle::getName)
                .collect(Collectors.toList());
        List<String> partitionValues = extractPartitionValues(partitionNameWithVersion.getPartitionName(), Optional.of(partitionColumnNames));
        ImmutableMap.Builder<ColumnHandle, NullableValue> builder = ImmutableMap.builder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            HudiColumnHandle column = partitionColumns.get(i);
            NullableValue parsedValue = parsePartitionValue(partitionNameWithVersion.getPartitionName(), partitionValues.get(i), partitionColumnTypes.get(i), timeZone);
            builder.put(column, parsedValue);
        }
        Map<ColumnHandle, NullableValue> values = builder.build();
        return new HivePartition(tableName, partitionNameWithVersion, values);
    }
}
