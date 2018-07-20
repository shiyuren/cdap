/*
 * Copyright © 2015 Cask Data, Inc.
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
 * the License.
 */

package co.cask.cdap.data2.metadata.dataset;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespacedEntityId;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents the complete metadata of a {@link MetadataEntity} including its properties and tags.
 */
public class Metadata {
  private final MetadataEntity metadataEntity;
  private final Map<String, String> properties;
  private final Set<String> tags;


  public Metadata(MetadataEntity metadataEntity) {
    this(metadataEntity, Collections.emptyMap(), Collections.emptySet());
  }

  /**
   * Returns an empty {@link Metadata}
   */
  public Metadata(NamespacedEntityId namespacedEntityId) {
    this(namespacedEntityId.toMetadataEntity());
  }

  public Metadata(NamespacedEntityId namespacedEntityId, Map<String, String> properties, Set<String> tags) {
    this(namespacedEntityId.toMetadataEntity(), properties, tags);
  }

  public Metadata(MetadataEntity metadataEntity, Map<String, String> properties, Set<String> tags) {
    if (metadataEntity == null || properties == null || tags == null) {
      throw new IllegalArgumentException("Valid and non-null metadata entity, properties and tags must be provided.");
    }
    this.metadataEntity = metadataEntity;
    this.properties = new HashMap<>(properties);
    this.tags = new HashSet<>(tags);
  }

  public MetadataEntity getMetadataEntity() {
    return metadataEntity;
  }

  /**
   * @return {@link NamespacedEntityId} to which the {@link Metadata} belongs if it is a known cdap entity type for
   * example datasets, applications etc. Custom resources likes fields etc cannot be converted into cdap
   * {@link NamespacedEntityId} and calling this for {@link Metadata} associated with such resources will fail with a
   * {@link IllegalArgumentException}.
   * @throws IllegalArgumentException if the {@link Metadata} belong to a custom cdap resource and not a known cdap
   * entity.
   */
  public NamespacedEntityId getEntityId() {
    return EntityId.fromMetadataEntity(metadataEntity);
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public Set<String> getTags() {
    return tags;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Metadata that = (Metadata) o;

    return Objects.equals(metadataEntity, that.metadataEntity) &&
      Objects.equals(properties, that.properties) &&
      Objects.equals(tags, that.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metadataEntity, properties, tags);
  }

  @Override
  public String toString() {
    return "Metadata{" +
      "metadataEntity=" + metadataEntity +
      ", properties=" + properties +
      ", tags=" + tags +
      '}';
  }
}
