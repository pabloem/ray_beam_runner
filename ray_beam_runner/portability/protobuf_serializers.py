#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

""" By default, protobuf messages are not serializable by cloudpickle.

This module registers custom serializers for the beam portability api
messages. It should be imported in each module that sends around
protobuf messages using cloudpickle (e.g. a ray remote call).
"""


import ray

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2

def proto_serializer(message):
    return message.SerializeToString()

def beam_fn_api_deserializer(msg_name):
    def deserializer(serialized):
        cls = getattr(beam_fn_api_pb2, msg_name)
        return cls.FromString(serialized)
    return deserializer

def beam_runner_api_deserializer(msg_name):
    def deserializer(serialized):
        cls = getattr(beam_runner_api_pb2, msg_name)
        return cls.FromString(serialized)
    return deserializer


for msg_name in beam_fn_api_pb2.DESCRIPTOR.message_types_by_name.keys():
    ray.util.register_serializer(
        getattr(beam_fn_api_pb2, msg_name),
        serializer=proto_serializer,
        deserializer=beam_fn_api_deserializer(msg_name),
    )

for msg_name in beam_runner_api_pb2.DESCRIPTOR.message_types_by_name.keys():
    ray.util.register_serializer(
        getattr(beam_runner_api_pb2, msg_name),
        serializer=proto_serializer,
        deserializer=beam_runner_api_deserializer(msg_name),
        )
