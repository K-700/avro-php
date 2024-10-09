<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

require_once('test_helper.php');

/**
 * Class IODatumReaderTest
 */
class IODatumReaderTest extends \PHPUnit\Framework\TestCase
{

  public function testSchemaMatching()
  {
    $writers_schema = <<<JSON
      { "type": "map",
        "values": "bytes" }
JSON;
    $readers_schema = $writers_schema;
    $this->assertTrue(AvroIODatumReader::schemas_match(
                        AvroSchema::parse($writers_schema),
                        AvroSchema::parse($readers_schema)));
  }

    /**
     * Infinity loop inside \AvroIOBinaryDecoder::skip_long function on php version < 8.0.0 in forked repo
     */
    public function testReadAndWriteDataEquals()
    {
        $record = [
            "id" => 174,
            "login" => "testLogin",
            "status" => "active",
        ];
        $expectedRecord = [
            "id" => 174,
            "login" => "testLogin",
        ];

        $writers_schema = <<<JSON
      { 
        "type": "record",
        "name": "user",
        "fields": [
          {
            "name": "id", 
            "type": "int" 
          },
          {
            "name": "login", 
            "type": "string" 
          },
          {
            "name": "deletedById", 
            "type": "int",
            "default": 0
          }
        ]
     }
JSON;

        $readers_schema = <<<JSON
      { 
        "type": "record",
        "name": "user",
        "fields": [
          {
            "name": "id", 
            "type": "int" 
          },
          {
            "name": "login", 
            "type": "string" 
          }
        ]
     }
JSON;

        $reader = new AvroIODatumReader();
        $written = new AvroStringIO();
        $encoder = new AvroIOBinaryEncoder($written);
        $writer = new AvroIODatumWriter();
        $writer->write_data(AvroSchema::parse($writers_schema), $record, $encoder);
        $decoder = new AvroIOBinaryDecoder(new AvroStringIO((string)$written));
        $this->assertEquals(
            $expectedRecord,
            $reader->read_data(
                AvroSchema::parse($writers_schema),
                AvroSchema::parse($readers_schema),
                $decoder
            ));
    }
}
