/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License
 */
package co.cask.cdap.cli.command.metadata;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.proto.id.EntityId;

/**
 * Helper for CLI to convert string representation of {@link EntityId} used in metadata CLI command
 * to/from {@link MetadataEntity}
 */
public class MetadataCommandHelper {
  private static final String METADATA_ENTITY_KV_SEPARATOR = "=";
  private static final String METADATA_ENTITY_PARTS_SEPARATOR = ",";
  private static final String METADATA_ENTITY_TYPE = "type";

  /**
   * Returns a CLI friendly representation of MetadataEntity for a dataset ds1 in ns1 the EntityId representation will
   * be dataset:ns1.ds1 and it's equivalent MetadataEntity representation will be
   * MetadataEntity{details={namespace=ns1, dataset=ds1}, type='dataset'} and the CLI friendly representation will be
   * namespace=ns1,dataset=ds1,type=dataset. Note: It is not necessary to give a type, if a type is not provided the
   * last key-value pair's key in the hierarchy will be considered as the type.
   */
  public static String toString(MetadataEntity metadataEntity) {
    StringBuilder builder = new StringBuilder();
    for (MetadataEntity.KeyValue keyValue : metadataEntity) {
      builder.append(keyValue.getKey());
      builder.append(METADATA_ENTITY_KV_SEPARATOR);
      builder.append(keyValue.getValue());
      builder.append(METADATA_ENTITY_PARTS_SEPARATOR);
    }
    builder.append(METADATA_ENTITY_TYPE);
    builder.append(METADATA_ENTITY_KV_SEPARATOR);
    builder.append(metadataEntity.getType());
    return builder.toString();
  }

  /**
   * Converts a CLI friendly string representation of MetadataEntity to MetadataEntity. For more details see
   * documentation for {@link #toString(MetadataEntity)}
   *
   * @param entityDetails the cli friendly string representation
   * @return {@link MetadataEntity}
   */
  public static MetadataEntity getMetadataEntity(String entityDetails) {
    MetadataEntity metadataEntity;
    try {
      // For backward compatibility we support entityId.toString representation from CLI for metadata for example
      // dataset representation look likes dataset:namespaceName.datasetName. Try to parse it as CDAP entity if it
      // fails then take the representation to be MetadataEntity which was introduced in CDAP 5.0
      metadataEntity = EntityId.fromString(entityDetails).toMetadataEntity();
    } catch (IllegalArgumentException e) {
      metadataEntity = fromString(entityDetails);
    }
    return metadataEntity;
  }

  private static MetadataEntity fromString(String input) {
    String[] keyValues = input.split(METADATA_ENTITY_PARTS_SEPARATOR);
    int lastKeyValueIndex = keyValues.length - 1;
    MetadataEntity.Builder builder = MetadataEntity.builder();

    String customType = null;
    if (keyValues[lastKeyValueIndex].split(METADATA_ENTITY_KV_SEPARATOR)[0].equalsIgnoreCase(METADATA_ENTITY_TYPE)) {
      // if a type is specified then store it to call appendAsType later
      customType = keyValues[keyValues.length - 1].split(METADATA_ENTITY_KV_SEPARATOR)[1];
      lastKeyValueIndex -= 1;
    }

    for (int i = 0; i <= lastKeyValueIndex; i++) {
      String[] part = keyValues[i].split(METADATA_ENTITY_KV_SEPARATOR);
      if (part[0].equals(customType)) {
        builder.appendAsType(part[0], part[1]);
      } else {
        builder.append(part[0], part[1]);
      }
    }
    return builder.build();
  }
}
