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

package co.cask.cdap.datapipeline;

import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.api.lineage.field.InputField;
import co.cask.cdap.api.lineage.field.Operation;
import co.cask.cdap.api.lineage.field.ReadOperation;
import co.cask.cdap.api.lineage.field.TransformOperation;
import co.cask.cdap.api.lineage.field.WriteOperation;
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import co.cask.cdap.etl.api.lineage.field.PipelineOperation;
import co.cask.cdap.etl.api.lineage.field.PipelineReadOperation;
import co.cask.cdap.etl.api.lineage.field.PipelineTransformOperation;
import co.cask.cdap.etl.api.lineage.field.PipelineWriteOperation;
import co.cask.cdap.etl.proto.Connection;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class LineageOperationProcessorTest {

  @Test
  public void testSimplePipeline() {
    // n1-->n2-->n3
    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));
    connections.add(new Connection("n2", "n3"));

    Map<String, List<PipelineOperation>> stageOperations = new HashMap<>();
    List<PipelineOperation> pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineReadOperation("read", "reading data", EndPoint.of("default", "file"), "offset",
                                                     "body"));
    stageOperations.put("n1", pipelineOperations);
    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineTransformOperation("parse", "parsing data", Collections.singletonList("body"),
                                                          Arrays.asList("name", "address", "zip")));
    stageOperations.put("n2", pipelineOperations);
    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineWriteOperation("write", "writing data", EndPoint.of("default", "file2"),
                                                      "name", "address", "zip"));
    stageOperations.put("n3", pipelineOperations);

    Map<String, StageInputOutput> stageInputOutputs = new HashMap<>();
    stageInputOutputs.put("n1", new StageInputOutput(Collections.emptySet(),
                                                     new HashSet<>(Arrays.asList("offset", "body"))));
    stageInputOutputs.put("n2", new StageInputOutput(new HashSet<>(Arrays.asList("offset", "body")),
                                                     new HashSet<>(Arrays.asList("name", "address", "zip"))));
    stageInputOutputs.put("n3", new StageInputOutput(new HashSet<>(Arrays.asList("name", "address", "zip")),
                                                     Collections.emptySet()));

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageInputOutputs,
                                                                          stageOperations, Collections.emptySet());
    Set<Operation> processedOperations = processor.process();
    Set<Operation> expected = new HashSet<>();
    expected.add(new ReadOperation("n1.read", "reading data",
                                   EndPoint.of("default", "file"), "offset", "body"));
    expected.add(new TransformOperation("n2.parse", "parsing data",
                                        Collections.singletonList(InputField.of("n1.read", "body")), "name", "address",
                                        "zip"));
    expected.add(new WriteOperation("n3.write", "writing data",
                                    EndPoint.of("default", "file2"), InputField.of("n2.parse", "name"),
                                    InputField.of("n2.parse", "address"), InputField.of("n2.parse", "zip")));

    Assert.assertEquals(new FieldLineageInfo(expected), new FieldLineageInfo(processedOperations));
  }

  @Test
  public void testAnotherSimplePipeline() {

    // n1-->n2-->n3-->n4
    // n1 => read: file -> (offset, body)
    // n2 => parse: (body) -> (first_name, last_name) | n2
    // n3 => concat: (first_name, last_name) -> (name) | n
    // n4 => write: (offset, name) -> another_file

    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));
    connections.add(new Connection("n2", "n3"));
    connections.add(new Connection("n3", "n4"));

    Map<String, List<PipelineOperation>> stageOperations = new HashMap<>();
    List<PipelineOperation> pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineReadOperation("read", "some read", EndPoint.of("ns", "file1"), "offset",
                                                     "body"));
    stageOperations.put("n1", pipelineOperations);

    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineTransformOperation("parse", "parsing body", Collections.singletonList("body"),
                                                          "first_name", "last_name"));
    stageOperations.put("n2", pipelineOperations);

    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineTransformOperation("concat", "concatinating the fields",
                                                          Arrays.asList("first_name", "last_name"), "name"));
    stageOperations.put("n3", pipelineOperations);

    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineWriteOperation("write_op", "writing data to file",
                                                      EndPoint.of("myns", "another_file"),
                                                      Arrays.asList("offset", "name")));
    stageOperations.put("n4", pipelineOperations);

    Map<String, StageInputOutput> stageInputOutputs = new HashMap<>();
    stageInputOutputs.put("n1", new StageInputOutput(Collections.emptySet(),
                                                     new HashSet<>(Arrays.asList("offset", "body"))));
    stageInputOutputs.put("n2", new StageInputOutput(new HashSet<>(Arrays.asList("offset", "body")),
                                                     new HashSet<>(Arrays.asList("offset", "first_name",
                                                                                 "last_name"))));
    stageInputOutputs.put("n3", new StageInputOutput(new HashSet<>(Arrays.asList("first_name", "last_name", "offset")),
                                                     new HashSet<>(Arrays.asList("offset", "name"))));
    stageInputOutputs.put("n4", new StageInputOutput(new HashSet<>(Arrays.asList("offset", "name")),
                                                     Collections.emptySet()));

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageInputOutputs,
                                                                          stageOperations, Collections.emptySet());
    Set<Operation> processedOperations = processor.process();

    ReadOperation read = new ReadOperation("n1.read", "some read", EndPoint.of("ns", "file1"), "offset", "body");

    TransformOperation parse = new TransformOperation("n2.parse", "parsing body",
                                                      Collections.singletonList(InputField.of("n1.read", "body")),
                                                      "first_name", "last_name");

    TransformOperation concat = new TransformOperation("n3.concat", "concatinating the fields",
                                                       Arrays.asList(InputField.of("n2.parse", "first_name"),
                                                                     InputField.of("n2.parse", "last_name")),
                                                       "name");

    WriteOperation write = new WriteOperation("n4.write_op", "writing data to file",
                                              EndPoint.of("myns", "another_file"),
                                              Arrays.asList(InputField.of("n1.read", "offset"),
                                                            InputField.of("n3.concat", "name")));

    List<Operation> expectedOperations = new ArrayList<>();
    expectedOperations.add(parse);
    expectedOperations.add(concat);
    expectedOperations.add(read);
    expectedOperations.add(write);

    Assert.assertEquals(new FieldLineageInfo(expectedOperations), new FieldLineageInfo(processedOperations));
  }

  @Test
  public void testSourceWithMultipleDestinations() {
    //              |----->n3
    // n1--->n2-----|
    //              |----->n4

    // n1 => read: file -> (offset, body)
    // n2 => parse: body -> (id, name, address, zip)
    // n3 => write1: (parse.id, parse.name) -> info
    // n4 => write2: (parse.address, parse.zip) -> location

    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));
    connections.add(new Connection("n2", "n3"));
    connections.add(new Connection("n3", "n4"));

    EndPoint source = EndPoint.of("ns", "file");
    EndPoint info = EndPoint.of("ns", "info");
    EndPoint location = EndPoint.of("ns", "location");

    Map<String, List<PipelineOperation>> stageOperations = new HashMap<>();
    List<PipelineOperation> pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineReadOperation("read", "reading from file", source, "offset", "body"));
    stageOperations.put("n1", pipelineOperations);

    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineTransformOperation("parse", "parsing body", Collections.singletonList("body"),
                                                          "id", "name", "address", "zip"));
    stageOperations.put("n2", pipelineOperations);

    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineWriteOperation("infoWrite", "writing info", info, "id", "name"));
    stageOperations.put("n3", pipelineOperations);

    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineWriteOperation("locationWrite", "writing location", location, "address", "zip"));
    stageOperations.put("n4", pipelineOperations);

    Map<String, StageInputOutput> stageInputOutputs = new HashMap<>();
    stageInputOutputs.put("n1", new StageInputOutput(Collections.emptySet(),
                                                     new HashSet<>(Arrays.asList("offset", "body"))));
    stageInputOutputs.put("n2", new StageInputOutput(new HashSet<>(Arrays.asList("offset", "body")),
                                                     new HashSet<>(Arrays.asList("id", "name", "address", "zip"))));
    stageInputOutputs.put("n3", new StageInputOutput(new HashSet<>(Arrays.asList("id", "name", "address", "zip")),
                                                     Collections.emptySet()));
    stageInputOutputs.put("n4", new StageInputOutput(new HashSet<>(Arrays.asList("id", "name", "address", "zip")),
                                                     Collections.emptySet()));

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageInputOutputs,
                                                                          stageOperations, Collections.emptySet());
    Set<Operation> processedOperations = processor.process();

    Set<Operation> expectedOperations = new HashSet<>();

    ReadOperation read = new ReadOperation("n1.read", "reading from file", source, "offset", "body");

    expectedOperations.add(read);

    TransformOperation parse = new TransformOperation("n2.parse", "parsing body",
                                                      Collections.singletonList(InputField.of("n1.read", "body")),
                                                      "id", "name", "address", "zip");

    expectedOperations.add(parse);

    WriteOperation infoWrite = new WriteOperation("n3.infoWrite", "writing info", info, InputField.of("n2.parse", "id"),
                                                  InputField.of("n2.parse", "name"));

    expectedOperations.add(infoWrite);

    WriteOperation locationWrite = new WriteOperation("n4.locationWrite", "writing location", location,
                                                      InputField.of("n2.parse", "address"),
                                                      InputField.of("n2.parse", "zip"));

    expectedOperations.add(locationWrite);
    Assert.assertEquals(new FieldLineageInfo(expectedOperations), new FieldLineageInfo(processedOperations));
  }

  @Test
  public void testDirectMerge() {

    // n1--------->n3
    //       |
    // n2--------->n4

    // n1 => pRead: personFile -> (offset, body)
    // n2 => hRead: hrFile -> (offset, body)
    // n1.n2.merge => n1.n2.merge: (pRead.offset, pRead.body, hRead.offset, hRead.body) -> (offset, body)
    // n3 => write1: (n1.n2.merge.offset, n1.n2.merge.body) -> testStore
    // n4 => write1: (n1.n2.merge.offset, n1.n2.merge.body) -> prodStore

    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n3"));
    connections.add(new Connection("n1", "n4"));
    connections.add(new Connection("n2", "n3"));
    connections.add(new Connection("n2", "n4"));

    EndPoint pEndPoint = EndPoint.of("ns", "personFile");
    EndPoint hEndPoint = EndPoint.of("ns", "hrFile");
    EndPoint testEndPoint = EndPoint.of("ns", "testStore");
    EndPoint prodEndPoint = EndPoint.of("ns", "prodStore");

    Map<String, List<PipelineOperation>> stageOperations = new HashMap<>();
    List<PipelineOperation> pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineReadOperation("pRead", "Reading from person file", pEndPoint, "offset", "body"));
    stageOperations.put("n1", pipelineOperations);

    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineReadOperation("hRead", "Reading from hr file", hEndPoint, "offset", "body"));
    stageOperations.put("n2", pipelineOperations);

    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineWriteOperation("write1", "Writing to test store", testEndPoint, "offset",
                                                      "body"));
    stageOperations.put("n3", pipelineOperations);

    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineWriteOperation("write2", "Writing to prod store", prodEndPoint, "offset",
                                                      "body"));
    stageOperations.put("n4", pipelineOperations);

    Map<String, StageInputOutput> stageInputsOutputs = new HashMap<>();
    stageInputsOutputs.put("n1", new StageInputOutput(Collections.emptySet(),
                                                      new HashSet<>(Arrays.asList("offset", "body"))));
    stageInputsOutputs.put("n2", new StageInputOutput(Collections.emptySet(),
                                                      new HashSet<>(Arrays.asList("offset", "body"))));
    stageInputsOutputs.put("n3", new StageInputOutput(new HashSet<>(Arrays.asList("offset", "body")),
                                                      Collections.emptySet()));
    stageInputsOutputs.put("n4", new StageInputOutput(new HashSet<>(Arrays.asList("offset", "body")),
                                                      Collections.emptySet()));

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageInputsOutputs,
                                                                          stageOperations, Collections.emptySet());
    Set<Operation> processedOperations = processor.process();

    Set<Operation> expectedOperations = new HashSet<>();
    ReadOperation pRead = new ReadOperation("n1.pRead", "Reading from person file", pEndPoint, "offset", "body");
    expectedOperations.add(pRead);

    ReadOperation hRead = new ReadOperation("n2.hRead", "Reading from hr file", hEndPoint, "offset", "body");
    expectedOperations.add(hRead);

    // implicit merge should be added by app
    TransformOperation merge = new TransformOperation("n1.n2.merge", "Merging stages: n1,n2",
                                                      Arrays.asList(InputField.of("n1.pRead", "offset"),
                                                                    InputField.of("n1.pRead", "body"),
                                                                    InputField.of("n2.hRead", "offset"),
                                                                    InputField.of("n2.hRead", "body")),
                                                      "offset", "body");
    expectedOperations.add(merge);

    WriteOperation write1 = new WriteOperation("n3.write1", "Writing to test store", testEndPoint,
                                               Arrays.asList(InputField.of("n1.n2.merge", "offset"),
                                                             InputField.of("n1.n2.merge", "body")));
    expectedOperations.add(write1);

    WriteOperation write2 = new WriteOperation("n4.write2", "Writing to prod store", prodEndPoint,
                                               Arrays.asList(InputField.of("n1.n2.merge", "offset"),
                                                             InputField.of("n1.n2.merge", "body")));
    expectedOperations.add(write2);

    Assert.assertEquals(new FieldLineageInfo(expectedOperations), new FieldLineageInfo(processedOperations));
  }

  @Test
  public void testComplexMerge() {
    //
    //  n1----n2---
    //            |         |-------n6
    //            |----n5---|
    //  n3----n4---         |---n7----n8
    //
    //
    //  n1: read: file1 -> offset,body
    //  n2: parse: body -> name, address, zip
    //  n3: read: file2 -> offset,body
    //  n4: parse: body -> name, address, zip
    //  n5: normalize: address -> address
    //  n5: rename: address -> state_address
    //  n6: write: offset, name, address -> file3
    //  n7: rename: offset -> file_offset
    //  n8: write: file_offset, name, address, zip -> file4

    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));
    connections.add(new Connection("n2", "n5"));
    connections.add(new Connection("n3", "n4"));
    connections.add(new Connection("n4", "n5"));
    connections.add(new Connection("n5", "n6"));
    connections.add(new Connection("n5", "n7"));
    connections.add(new Connection("n7", "n8"));

    EndPoint n1EndPoint = EndPoint.of("ns", "file1");
    EndPoint n3EndPoint = EndPoint.of("ns", "file2");
    EndPoint n6EndPoint = EndPoint.of("ns", "file3");
    EndPoint n8EndPoint = EndPoint.of("ns", "file4");

    Map<String, List<PipelineOperation>> stageOperations = new HashMap<>();
    List<PipelineOperation> pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineReadOperation("read", "reading file 1", n1EndPoint, "offset", "body"));
    stageOperations.put("n1", pipelineOperations);

    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineTransformOperation("parse", "parsing file 1", Collections.singletonList("body"),
                                                          "name", "address", "zip"));
    stageOperations.put("n2", pipelineOperations);

    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineReadOperation("read", "reading file 2", n3EndPoint, "offset", "body"));
    stageOperations.put("n3", pipelineOperations);

    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineTransformOperation("parse", "parsing file 2", Collections.singletonList("body"),
                                                          "name", "address", "zip"));
    stageOperations.put("n4", pipelineOperations);

    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineTransformOperation("normalize", "normalizing address",
                                                          Collections.singletonList("address"), "address"));
    pipelineOperations.add(new PipelineTransformOperation("rename", "renaming address to state_address",
                                                          Collections.singletonList("address"), "state_address"));
    stageOperations.put("n5", pipelineOperations);

    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineWriteOperation("write", "writing file 3", n6EndPoint, "offset", "name",
                                                      "address"));
    stageOperations.put("n6", pipelineOperations);

    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineTransformOperation("rename", "renaming offset to file_offset",
                                                          Collections.singletonList("offset"), "file_offset"));
    stageOperations.put("n7", pipelineOperations);

    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineWriteOperation("write", "writing file 4", n8EndPoint, "file_offset", "name",
                                                      "address", "zip"));
    stageOperations.put("n8", pipelineOperations);

    Map<String, StageInputOutput> stageInputOutputs = new HashMap<>();
    stageInputOutputs.put("n1", new StageInputOutput(Collections.emptySet(),
                                                     new HashSet<>(Arrays.asList("offset", "body"))));
    stageInputOutputs.put("n2", new StageInputOutput(new HashSet<>(Arrays.asList("offset", "body")),
                                                     new HashSet<>(Arrays.asList("offset", "body", "name", "address",
                                                                                 "zip"))));
    stageInputOutputs.put("n3", new StageInputOutput(Collections.emptySet(),
                                                     new HashSet<>(Arrays.asList("offset", "body"))));
    stageInputOutputs.put("n4", new StageInputOutput(new HashSet<>(Arrays.asList("offset", "body")),
                                                     new HashSet<>(Arrays.asList("offset", "body", "name", "address",
                                                                                 "zip"))));
    stageInputOutputs.put("n5", new StageInputOutput(new HashSet<>(Arrays.asList("offset", "body", "name", "address",
                                                                                 "zip")),
                                                     new HashSet<>(Arrays.asList("offset", "body", "name", "address",
                                                                                 "zip", "state_address"))));
    stageInputOutputs.put("n6", new StageInputOutput(new HashSet<>(Arrays.asList("offset", "body", "name", "address",
                                                                                 "zip", "state_address")),
                                                     new HashSet<>(Arrays.asList("offset", "body"))));
    stageInputOutputs.put("n7", new StageInputOutput(new HashSet<>(Arrays.asList("offset", "body", "name", "address",
                                                                                 "zip", "state_address")),
                                                     new HashSet<>(Arrays.asList("file_offset", "body", "name",
                                                                                 "address", "zip", "state_address"))));
    stageInputOutputs.put("n8", new StageInputOutput(new HashSet<>(Arrays.asList("file_offset", "body", "name",
                                                                                 "address", "zip", "state_address")),
                                                     new HashSet<>(Arrays.asList("offset", "body"))));


    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageInputOutputs,
                                                                          stageOperations, Collections.emptySet());
    Set<Operation> processedOperations = processor.process();
    Set<Operation> expectedOperations = new HashSet<>();

    ReadOperation read = new ReadOperation("n1.read", "reading file 1", n1EndPoint, "offset", "body");
    expectedOperations.add(read);

    TransformOperation parse = new TransformOperation("n2.parse", "parsing file 1",
                                                      Collections.singletonList(InputField.of("n1.read", "body")),
                                                      "name", "address", "zip");
    expectedOperations.add(parse);

    read = new ReadOperation("n3.read", "reading file 2", n3EndPoint, "offset", "body");
    expectedOperations.add(read);

    parse = new TransformOperation("n4.parse", "parsing file 2",
                                   Collections.singletonList(InputField.of("n3.read", "body")), "name", "address",
                                   "zip");
    expectedOperations.add(parse);

    List<InputField> inputsToMerge = new ArrayList<>();
    inputsToMerge.add(InputField.of("n1.read", "offset"));
    inputsToMerge.add(InputField.of("n1.read", "body"));
    inputsToMerge.add(InputField.of("n2.parse", "name"));
    inputsToMerge.add(InputField.of("n2.parse", "address"));
    inputsToMerge.add(InputField.of("n2.parse", "zip"));
    inputsToMerge.add(InputField.of("n3.read", "offset"));
    inputsToMerge.add(InputField.of("n3.read", "body"));
    inputsToMerge.add(InputField.of("n4.parse", "name"));
    inputsToMerge.add(InputField.of("n4.parse", "address"));
    inputsToMerge.add(InputField.of("n4.parse", "zip"));

    TransformOperation merge = new TransformOperation("n2.n4.merge", "Merging stages: n2,n4", inputsToMerge, "offset",
                                                      "body", "name", "address", "zip");
    expectedOperations.add(merge);

    TransformOperation normalize = new TransformOperation("n5.normalize", "normalizing address",
                                                          Collections.singletonList(InputField.of("n2.n4.merge",
                                                                                                  "address")),
                                                          "address");
    expectedOperations.add(normalize);

    TransformOperation rename = new TransformOperation("n5.rename", "renaming address to state_address",
                                                       Collections.singletonList(InputField.of("n5.normalize",
                                                                                               "address")),
                                                       "state_address");
    expectedOperations.add(rename);

    WriteOperation write = new WriteOperation("n6.write", "writing file 3", n6EndPoint,
                                              InputField.of("n2.n4.merge", "offset"),
                                              InputField.of("n2.n4.merge", "name"),
                                              InputField.of("n5.normalize", "address"));
    expectedOperations.add(write);

    rename = new TransformOperation("n7.rename", "renaming offset to file_offset",
                                    Collections.singletonList(InputField.of("n2.n4.merge", "offset")), "file_offset");
    expectedOperations.add(rename);

    write = new WriteOperation("n8.write", "writing file 4", n8EndPoint, InputField.of("n7.rename", "file_offset"),
                               InputField.of("n2.n4.merge", "name"), InputField.of("n5.normalize", "address"),
                               InputField.of("n2.n4.merge", "zip"));
    expectedOperations.add(write);

    Assert.assertEquals(expectedOperations, processedOperations);
  }

  @Test
  public void testInvalidInputs() {
    // n1-->n2-->n3
    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));
    connections.add(new Connection("n2", "n3"));

    Map<String, List<PipelineOperation>> stageOperations = new HashMap<>();
    List<PipelineOperation> pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineReadOperation("read", "reading data", EndPoint.of("default", "file"), "offset",
                                                     "body"));
    stageOperations.put("n1", pipelineOperations);
    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineTransformOperation("parse", "parsing data", Collections.singletonList("body"),
                                                          Arrays.asList("name", "address", "zip")));
    stageOperations.put("n2", pipelineOperations);
    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineWriteOperation("write", "writing data", EndPoint.of("default", "file2"),
                                                      "name", "address", "zip"));
    stageOperations.put("n3", pipelineOperations);

    Map<String, StageInputOutput> stageInputOutputs = new HashMap<>();
    stageInputOutputs.put("n1", new StageInputOutput(Collections.emptySet(),
                                                     new HashSet<>(Arrays.asList("offset", "body"))));
    stageInputOutputs.put("n2", new StageInputOutput(new HashSet<>(Arrays.asList("offset", "body")),
                                                     new HashSet<>(Arrays.asList("name", "address", "zip"))));
    stageInputOutputs.put("n3", new StageInputOutput(new HashSet<>(Arrays.asList("address", "zip")),
                                                     Collections.emptySet()));

    LineageOperationsProcessor processor;

    try {
      processor = new LineageOperationsProcessor(connections, stageInputOutputs, stageOperations,
                                                 Collections.emptySet());
      Assert.fail();
    } catch (InvalidLineageException e) {
      // expected
      Map<String, InvalidFieldOperations> invalidFieldOperations = e.getInvalidFieldOperations();
      Assert.assertEquals(1, invalidFieldOperations.size());
      InvalidFieldOperations n3Invalids = invalidFieldOperations.get("n3");
      Assert.assertEquals(1, n3Invalids.getInvalidInputs().size());
      Map<String, List<String>> invalidInputs = new HashMap<>();
      invalidInputs.put("name", Collections.singletonList("write"));
      Assert.assertEquals(invalidInputs, n3Invalids.getInvalidInputs());
      Assert.assertEquals(0, n3Invalids.getInvalidOutputs().size());
    }

    // name is provided by output of the operation previous to write
    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineTransformOperation("name_lookup", "generating name",
                                                          Collections.singletonList("address"), "name"));
    pipelineOperations.add(new PipelineWriteOperation("write", "writing data", EndPoint.of("default", "file2"),
                                                      "name", "address", "zip"));
    stageOperations.put("n3", pipelineOperations);

    // this should succeed since name is available now
    processor = new LineageOperationsProcessor(connections, stageInputOutputs, stageOperations,
                                               Collections.emptySet());
  }

  @Test
  public void testUnusedOutputs() {
    // n1-->n2-->n3
    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));
    connections.add(new Connection("n2", "n3"));

    Map<String, List<PipelineOperation>> stageOperations = new HashMap<>();
    List<PipelineOperation> pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineReadOperation("read", "reading data", EndPoint.of("default", "file"), "offset",
                                                     "body"));
    stageOperations.put("n1", pipelineOperations);
    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineTransformOperation("parse", "parsing data", Collections.singletonList("body"),
                                                          Arrays.asList("name", "address", "zip")));
    stageOperations.put("n2", pipelineOperations);
    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineWriteOperation("write", "writing data", EndPoint.of("default", "file2"),
                                                      "name", "address", "zip"));
    stageOperations.put("n3", pipelineOperations);

    Map<String, StageInputOutput> stageInputOutputs = new HashMap<>();
    stageInputOutputs.put("n1", new StageInputOutput(Collections.emptySet(), Collections.singleton("body")));
    stageInputOutputs.put("n2", new StageInputOutput(Collections.singleton("body"),
                                                     new HashSet<>(Arrays.asList("name", "address"))));
    stageInputOutputs.put("n3", new StageInputOutput(new HashSet<>(Arrays.asList("name", "address")),
                                                     Collections.emptySet()));

    LineageOperationsProcessor processor;

    try {
      processor = new LineageOperationsProcessor(connections, stageInputOutputs, stageOperations,
                                                 Collections.emptySet());
      Assert.fail();
    } catch (InvalidLineageException e) {
      // Following is the exception message

      // "Outputs of following operations are neither used by subsequent operations in that " +
      //  "stage nor are part of the output schema of that stage: " +
      //  "<stage:n1, [operation:read, field:offset]>, <stage:n2, [operation:parse, field:zip]>. " +
      //  "Inputs of following operations are neither part of the input schema of a stage nor are generated by any " +
      //  "previous operations recorded by that stage: <stage:n3, [operation:write, field:zip]>. ";

      Map<String, InvalidFieldOperations> expected = new HashMap<>();
      InvalidFieldOperations invalids = new InvalidFieldOperations(Collections.emptyMap(),
                                                                   ImmutableMap.of("offset",
                                                                                   Collections.singletonList("read")));

      expected.put("n1", invalids);

      invalids = new InvalidFieldOperations(Collections.emptyMap(),
                                            ImmutableMap.of("zip", Collections.singletonList("parse")));

      expected.put("n2", invalids);

      invalids = new InvalidFieldOperations(ImmutableMap.of("zip", Collections.singletonList("write")),
                                            Collections.emptyMap());

      expected.put("n3", invalids);
      Map<String, InvalidFieldOperations> invalidFieldOperations = e.getInvalidFieldOperations();
      Assert.assertEquals(expected, invalidFieldOperations);
    }
  }

  @Test
  public void testRedundantOutputs() {
    // n1-->n2-->n3
    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));
    connections.add(new Connection("n2", "n3"));

    Map<String, List<PipelineOperation>> stageOperations = new HashMap<>();
    List<PipelineOperation> pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineReadOperation("read", "reading data", EndPoint.of("default", "file"), "offset",
                                                     "body"));
    stageOperations.put("n1", pipelineOperations);
    pipelineOperations = new ArrayList<>();

    // Output of the following operation is redundant since it will be overwritten by the parse operation
    pipelineOperations.add(new PipelineTransformOperation("redundant_parse", "parsing data",
                                                          Collections.singletonList("body"), "name"));

    pipelineOperations.add(new PipelineTransformOperation("parse", "parsing data", Collections.singletonList("body"),
                                                          Arrays.asList("name", "address", "zip")));
    stageOperations.put("n2", pipelineOperations);
    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineWriteOperation("write", "writing data", EndPoint.of("default", "file2"),
                                                      "name", "address", "zip"));
    stageOperations.put("n3", pipelineOperations);

    Map<String, StageInputOutput> stageInputOutputs = new HashMap<>();
    stageInputOutputs.put("n1", new StageInputOutput(Collections.emptySet(),
                                                     new HashSet<>(Arrays.asList("offset", "body"))));
    stageInputOutputs.put("n2", new StageInputOutput(new HashSet<>(Arrays.asList("offset", "body")),
                                                     new HashSet<>(Arrays.asList("name", "address", "zip"))));
    stageInputOutputs.put("n3", new StageInputOutput(new HashSet<>(Arrays.asList("name", "address", "zip")),
                                                     Collections.emptySet()));

    try {
      LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageInputOutputs,
                                                                            stageOperations, Collections.emptySet());
      Assert.fail();
    } catch (InvalidLineageException e) {
      // Exception message:
      // "Outputs of following operations are neither used by subsequent operations in " +
      //  "that stage nor are part of the output schema of that stage: " +
      //  "<stage:n2, [operation:redundant_parse, field:name]>. ";

      Map<String, InvalidFieldOperations> expected = new HashMap<>();
      InvalidFieldOperations invalids =
        new InvalidFieldOperations(Collections.emptyMap(),
                                   ImmutableMap.of("name", Collections.singletonList("redundant_parse")));

      expected.put("n2", invalids);

      Map<String, InvalidFieldOperations> invalidFieldOperations = e.getInvalidFieldOperations();
      Assert.assertEquals(expected, invalidFieldOperations);
    }
  }

  @Test
  public void testRedundantOutputUsedAsInput() {
    // n1-->n2-->n3
    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));
    connections.add(new Connection("n2", "n3"));

    Map<String, List<PipelineOperation>> stageOperations = new HashMap<>();
    List<PipelineOperation> pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineReadOperation("read", "reading data", EndPoint.of("default", "file"), "offset",
                                                     "body"));
    stageOperations.put("n1", pipelineOperations);
    pipelineOperations = new ArrayList<>();

    // Output of the following operation is redundant since it will be overwritten by the parse operation
    pipelineOperations.add(new PipelineTransformOperation("redundant_parse1", "parsing data",
                                                          Collections.singletonList("body"), "name"));

    pipelineOperations.add(new PipelineTransformOperation("redundant_parse2", "parsing data",
                                                          Collections.singletonList("body"), "name"));

    // Following operation is non redundant as its output is consumed by parse operation
    pipelineOperations.add(new PipelineTransformOperation("non_redundant_parse", "parsing data",
                                                          Collections.singletonList("body"), "name"));

    pipelineOperations.add(new PipelineTransformOperation("parse", "parsing data", Arrays.asList("body", "name"),
                                                          Arrays.asList("name", "address", "zip")));
    stageOperations.put("n2", pipelineOperations);
    pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new PipelineWriteOperation("write", "writing data", EndPoint.of("default", "file2"),
                                                      "name", "address", "zip"));
    stageOperations.put("n3", pipelineOperations);

    Map<String, StageInputOutput> stageInputOutputs = new HashMap<>();
    stageInputOutputs.put("n1", new StageInputOutput(Collections.emptySet(),
                                                     new HashSet<>(Arrays.asList("offset", "body"))));
    stageInputOutputs.put("n2", new StageInputOutput(new HashSet<>(Arrays.asList("offset", "body")),
                                                     new HashSet<>(Arrays.asList("name", "address", "zip"))));
    stageInputOutputs.put("n3", new StageInputOutput(new HashSet<>(Arrays.asList("name", "address", "zip")),
                                                     Collections.emptySet()));

    try {
      LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageInputOutputs,
                                                                            stageOperations, Collections.emptySet());
      Assert.fail();
    } catch (InvalidLineageException e) {
      // Exception message:
      // "Outputs of following operations are neither used by subsequent operations " +
      //  "in that stage nor are part of the output schema of that stage: " +
      //  "<stage:n2, [operation:redundant_parse1, field:name], [operation:redundant_parse2, field:name]>. ";

      Map<String, InvalidFieldOperations> expected = new HashMap<>();
      InvalidFieldOperations invalids =
        new InvalidFieldOperations(Collections.emptyMap(),
                                   ImmutableMap.of("name", Arrays.asList("redundant_parse1", "redundant_parse2")));

      expected.put("n2", invalids);

      Map<String, InvalidFieldOperations> invalidFieldOperations = e.getInvalidFieldOperations();
      Assert.assertEquals(expected, invalidFieldOperations);
    }
  }
}
