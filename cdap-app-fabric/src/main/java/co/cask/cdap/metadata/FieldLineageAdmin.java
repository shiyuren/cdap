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
 * the License.
 */

package co.cask.cdap.metadata;

import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.api.lineage.field.Operation;
import co.cask.cdap.api.lineage.field.ReadOperation;
import co.cask.cdap.api.lineage.field.TransformOperation;
import co.cask.cdap.api.lineage.field.WriteOperation;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.metadata.lineage.field.DefaultFieldLineageReader;
import co.cask.cdap.data2.metadata.lineage.field.EndPointField;
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageReader;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;

import co.cask.cdap.proto.metadata.lineage.DatasetField;
import co.cask.cdap.proto.metadata.lineage.FieldLineageDetails;
import co.cask.cdap.proto.metadata.lineage.FieldLineageSummary;
import co.cask.cdap.proto.metadata.lineage.FieldOperationInfo;
import co.cask.cdap.proto.metadata.lineage.FieldOperationInput;
import co.cask.cdap.proto.metadata.lineage.FieldOperationOutput;
import co.cask.cdap.proto.metadata.lineage.ProgramFieldOperationInfo;
import co.cask.cdap.proto.metadata.lineage.ProgramInfo;
import co.cask.cdap.proto.metadata.lineage.ProgramRunOperations;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Service to compute field lineage based on operations stored in {@link DefaultFieldLineageReader}.
 */
public class FieldLineageAdmin {

  private final FieldLineageReader fieldLineageReader;

  @Inject
  @VisibleForTesting
  public FieldLineageAdmin(FieldLineageReader fieldLineageReader) {
    this.fieldLineageReader = fieldLineageReader;
  }

  /**
   * Get the set of fields written to the EndPoint by field lineage {@link WriteOperation},
   * over the given time range, optionally filtered to include only fields that have the given prefix.
   *
   * @param endPoint the EndPoint for which the fields need to be returned
   * @param start start time (inclusive) in milliseconds
   * @param end end time (exclusive) in milliseconds
   * @param prefix prefix for the field name, if {@code null} then all fields are returned
   * @return set of fields written to a given EndPoint
   */
  public Set<String> getFields(EndPoint endPoint, long start, long end, @Nullable String prefix) {
    Set<String> fields = fieldLineageReader.getFields(endPoint, start, end);
    if (prefix == null) {
      return fields;
    }

    Set<String> prefixFields = new HashSet<>();
    for (String field : fields) {
      if (field.startsWith(prefix)) {
        prefixFields.add(field);
      }
    }
    return prefixFields;
  }

  /**
   * Get the summary for the specified EndPointField over a given time range depending on the direction specified.
   * Summary in the "incoming" direction consists of set of EndPointFields which participated in the computation
   * of the given EndPointField; summary in the "outgoing" direction consists of set of EndPointFields
   * which were computed from the specified EndPointField. When direction is specified as 'both', incoming as well
   * as outgoing summaries are returned.
   *
   * @param direction the direction in which summary need to be computed
   * @param endPointField the EndPointField for which summary to be returned
   * @param start start time (inclusive) in milliseconds
   * @param end end time (exclusive) in milliseconds
   * @return the FieldLineageSummary
   */
  FieldLineageSummary getSummary(Constants.FieldLineage.Direction direction, EndPointField endPointField, long start,
                                 long end) {
    Set<DatasetField> incoming = null;
    Set<DatasetField> outgoing = null;
    if (direction == Constants.FieldLineage.Direction.INCOMING || direction == Constants.FieldLineage.Direction.BOTH) {
      Set<EndPointField> incomingSummary = fieldLineageReader.getIncomingSummary(endPointField, start, end);
      incoming = convertSummaryToDatasetField(incomingSummary);
    }
    if (direction == Constants.FieldLineage.Direction.OUTGOING || direction == Constants.FieldLineage.Direction.BOTH) {
      Set<EndPointField> outgoingSummary = fieldLineageReader.getOutgoingSummary(endPointField, start, end);
      outgoing = convertSummaryToDatasetField(outgoingSummary);
    }
    return new FieldLineageSummary(incoming, outgoing);
  }

  private Set<DatasetField> convertSummaryToDatasetField(Set<EndPointField> summary) {
    Map<EndPoint, Set<String>> endPointFields = new HashMap<>();
    for (EndPointField endPointField : summary) {
      EndPoint endPoint = endPointField.getEndPoint();
      Set<String> fields = endPointFields.computeIfAbsent(endPoint, k -> new HashSet<>());
      fields.add(endPointField.getField());
    }

    Set<DatasetField> result = new HashSet<>();
    for (Map.Entry<EndPoint, Set<String>> entry : endPointFields.entrySet()) {
      DatasetId datasetId = new DatasetId(entry.getKey().getNamespace(), entry.getKey().getName());
      result.add(new DatasetField(datasetId, entry.getValue()));
    }

    return result;
  }

  /**
   * Get the operation details for the specified EndPointField over a given time range depending on the
   * direction specified. Operation details in the "incoming" direction consists of consists of the datasets
   * and their fields ({@link DatasetField}) that this field originates from, as well as the programs and
   * operations that generated this field from those origins. In outgoing direction, it consists of the datasets
   * and their fields ({@link DatasetField}) that were computed from this field, along with the programs and
   * operations that performed the computation. When direction is specified as 'both', incoming as well
   * as outgoing operations are returned.
   *
   * @param direction the direction in which operations need to be computed
   * @param endPointField the EndPointField for which operations to be returned
   * @param start start time (inclusive) in milliseconds
   * @param end end time (exclusive) in milliseconds
   * @return the FieldLineageDetails instance
   */
  FieldLineageDetails getOperationDetails(Constants.FieldLineage.Direction direction, EndPointField endPointField,
                                          long start, long end) {
    List<ProgramFieldOperationInfo> incoming = null;
    List<ProgramFieldOperationInfo> outgoing = null;
    if (direction == Constants.FieldLineage.Direction.INCOMING || direction == Constants.FieldLineage.Direction.BOTH) {
      Set<ProgramRunOperations> incomingOperations = fieldLineageReader.getIncomingOperations(endPointField, start,
                                                                                              end);
      incoming = processOperations(incomingOperations);
    }
    if (direction == Constants.FieldLineage.Direction.OUTGOING || direction == Constants.FieldLineage.Direction.BOTH) {
      Set<ProgramRunOperations> outgoingOperations = fieldLineageReader.getOutgoingOperations(endPointField, start,
                                                                                              end);
      outgoing = processOperations(outgoingOperations);
    }
    return new FieldLineageDetails(incoming, outgoing);
  }

  private List<ProgramFieldOperationInfo> processOperations(Set<ProgramRunOperations> programRunOperations) {
    List<ProgramFieldOperationInfo> result = new ArrayList<>();
    for (ProgramRunOperations entry : programRunOperations) {
      List<ProgramInfo> programInfo = computeProgramInfo(entry.getProgramRunIds());
      List<FieldOperationInfo> fieldOperationInfo = computeFieldOperationInfo(entry.getOperations());
      result.add(new ProgramFieldOperationInfo(programInfo, fieldOperationInfo));
    }
    return result;
  }

  /**
   * Computes the list of {@link ProgramInfo} from given set of ProgramRunIds.
   * For each program, there is only one item in the returned list, representing the
   * latest run of that program. Returned list is also sorted by the last executed time
   * in descending order.
   *
   * @param programRunIds set of program run ids from which program info to be computed
   * @return list of ProgramInfo
   */
  private List<ProgramInfo> computeProgramInfo(Set<ProgramRunId> programRunIds) {
    Map<ProgramId, Long> programIdToLastExecutedTime = new HashMap<>();
    for (ProgramRunId programRunId : programRunIds) {
      long programRunExecutedTime = RunIds.getTime(programRunId.getRun(), TimeUnit.SECONDS);
      Long lastExecutedTime = programIdToLastExecutedTime.get(programRunId.getParent());
      if (lastExecutedTime == null || programRunExecutedTime > lastExecutedTime) {
        programIdToLastExecutedTime.put(programRunId.getParent(), programRunExecutedTime);
      }
    }

    Stream<Map.Entry<ProgramId, Long>> sortedByLastExecutedTime =
      programIdToLastExecutedTime.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue()));

    List<ProgramInfo> programInfos;

    programInfos = sortedByLastExecutedTime.map(programIdLongEntry
      -> new ProgramInfo(programIdLongEntry.getKey(), programIdLongEntry.getValue())).collect(Collectors.toList());

    return programInfos;
  }

  /**
   * Computes list of {@link FieldOperationInfo} from the given operations.
   * Returned list contains the operations sorted in topological order i.e. each operation
   * in the list is guaranteed to occur before any other operation that reads its outputs.
   *
   * @param operations set of operation to convert to FieldOperationInfo instances
   * @return list of FieldOperationInfo sorted topologically
   */
  private List<FieldOperationInfo> computeFieldOperationInfo(Set<Operation> operations) {
    List<Operation> orderedOperations = FieldLineageInfo.getTopologicallySortedOperations(operations);
    List<FieldOperationInfo> fieldOperationInfos = new ArrayList<>();
    for (Operation operation : orderedOperations) {
      fieldOperationInfos.add(convertToFieldOperationInfo(operation));
    }

    return fieldOperationInfos;
  }

  private FieldOperationInfo convertToFieldOperationInfo(Operation operation) {
    FieldOperationInput inputs = null;
    FieldOperationOutput outputs = null;
    switch (operation.getType()) {
      case READ:
        ReadOperation read = (ReadOperation) operation;
        inputs = FieldOperationInput.of(read.getSource());
        outputs = FieldOperationOutput.of(read.getOutputs());
        break;
      case TRANSFORM:
        TransformOperation transform = (TransformOperation) operation;
        inputs = FieldOperationInput.of(transform.getInputs());
        outputs = FieldOperationOutput.of(transform.getOutputs());
        break;
      case WRITE:
        WriteOperation write = (WriteOperation) operation;
        inputs = FieldOperationInput.of(write.getInputs());
        outputs = FieldOperationOutput.of(write.getDestination());
        break;
    }
    return new FieldOperationInfo(operation.getName(), operation.getDescription(), inputs, outputs);
  }
}
