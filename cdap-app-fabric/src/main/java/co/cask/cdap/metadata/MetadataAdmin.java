/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.InvalidMetadataException;
import co.cask.cdap.common.metadata.MetadataRecordV2;
import co.cask.cdap.data2.metadata.dataset.SearchRequest;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.metadata.MetadataSearchResponseV2;

import java.util.Map;
import java.util.Set;

/**
 * Interface that the {@link MetadataHttpHandler} uses to interact with Metadata.
 * All the create, update and remove operations through this interface are restricted to {@link MetadataScope#USER},
 * so that clients of the RESTful API cannot create, update or remove {@link MetadataScope#SYSTEM} metadata.
 * The operations to retrieve metadata properties and tags allow passing in a scope, so clients of the RESTful API
 * can retrieve both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM} metadata.
 */
public interface MetadataAdmin {

  /**
   * Adds the specified {@link Map} to the metadata of the specified {@link MetadataEntity metadataEntity}.
   * Existing keys are updated with new values, newer keys are appended to the metadata. This API only supports adding
   * properties in {@link MetadataScope#USER}.
   *
   * @throws InvalidMetadataException if some of the properties violate metadata validation rules
   */
  void addProperties(MetadataEntity metadataEntity, Map<String, String> properties) throws InvalidMetadataException;

  /**
   * Adds the specified tags to specified {@link MetadataEntity}. This API only supports adding tags in
   * {@link MetadataScope#USER}.
   *
   * @throws InvalidMetadataException if some of the properties violate metadata validation rules
   */
  void addTags(MetadataEntity metadataEntity, Set<String> tags) throws InvalidMetadataException;

  /**
   * Returns a set of {@link MetadataRecordV2} representing all metadata (including properties and tags) for the
   * specified {@link MetadataEntity} in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}.
   */
  Set<MetadataRecordV2> getMetadata(MetadataEntity metadataEntity);

  /**
   * Returns a set of {@link MetadataRecordV2} representing all metadata (including properties and tags) for the
   * specified {@link MetadataEntity} in the specified {@link MetadataScope}.
   */
  // TODO: Should this return a single metadata record instead or is a set of one record ok?
  Set<MetadataRecordV2> getMetadata(MetadataScope scope, MetadataEntity metadataEntity);

  /**
   * @return a {@link Map} representing the metadata of the specified {@link MetadataEntity} in both
   * {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}
   */
  // TODO: This should perhaps return a Map<MetadataScope, Map<String, String>>
  Map<String, String> getProperties(MetadataEntity metadataEntity);

  /**
   * @return a {@link Map} representing the metadata of the specified {@link MetadataEntity} in the specified
   * {@link MetadataScope}
   */
  Map<String, String> getProperties(MetadataScope scope, MetadataEntity metadataEntity);

  /**
   * @return all the tags for the specified {@link MetadataEntity} in both {@link MetadataScope#USER} and
   * {@link MetadataScope#SYSTEM}
   */
  // TODO: This should perhaps return a Map<MetadataScope, Set<String>>
  Set<String> getTags(MetadataEntity metadataEntity);

  /**
   * @return all the tags for the specified {@link MetadataEntity} in the specified {@link MetadataScope}
   */
  Set<String> getTags(MetadataScope scope, MetadataEntity metadataEntity);

  /**
   * Removes all the metadata (including properties and tags) for the specified {@link MetadataEntity}. This
   * API only supports removing metadata in {@link MetadataScope#USER}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove metadata for
   */
  void removeMetadata(MetadataEntity metadataEntity);

  /**
   * Removes all properties from the metadata of the specified {@link MetadataEntity}. This API only supports
   * removing properties in {@link MetadataScope#USER}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove properties for
   */
  void removeProperties(MetadataEntity metadataEntity);

  /**
   * Removes the specified keys from the metadata properties of the specified {@link MetadataEntity}. This API only
   * supports removing properties in {@link MetadataScope#USER}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove the specified properties for
   * @param keys the metadata property keys to remove
   */
  void removeProperties(MetadataEntity metadataEntity, Set<String> keys);

  /**
   * Removes all tags from the specified {@link MetadataEntity}. This API only supports removing tags in
   * {@link MetadataScope#USER}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove tags for
   */
  void removeTags(MetadataEntity metadataEntity);

  /**
   * Removes the specified tags from the specified {@link MetadataEntity}. This API only supports removing tags in
   * {@link MetadataScope#USER}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove the specified tags for
   * @param tags the tags to remove
   */
  void removeTags(MetadataEntity metadataEntity, Set<String> tags);

  /**
   * Executes a search for CDAP entities in the specified namespace with the specified search query and
   * an optional set of {@link EntityTypeSimpleName entity types} in the specified {@link MetadataScope}.
   *
   * @param request the search request
   * @return the {@link MetadataSearchResponseV2} containing search results for the specified search query and filters
   */
  MetadataSearchResponseV2 search(SearchRequest request) throws Exception;
}
