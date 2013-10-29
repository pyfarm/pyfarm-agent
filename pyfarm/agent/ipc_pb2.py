# Generated by the protocol buffer compiler.  DO NOT EDIT!

from google.protobuf import descriptor
from google.protobuf import message
from google.protobuf import reflection
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)



DESCRIPTOR = descriptor.FileDescriptor(
  name='ipc.proto',
  package='',
  serialized_pb='\n\tipc.proto\"\x96\x01\n\x0b\x44ynamicType\x12%\n\x04type\x18\x01 \x02(\x0e\x32\x17.DynamicType.PythonType\x12\x0b\n\x03int\x18\x02 \x01(\x05\x12\x0e\n\x06string\x18\x03 \x01(\t\x12\x0f\n\x07\x62oolean\x18\x04 \x01(\x08\"2\n\nPythonType\x12\n\n\x06STRING\x10\x00\x12\x0b\n\x07INTEGER\x10\x01\x12\x0b\n\x07\x42OOLEAN\x10\x02\"|\n\tException\x12\x11\n\tclassname\x18\x01 \x02(\t\x12\x0f\n\x07message\x18\x04 \x02(\t\x12\x1a\n\x04\x61rgs\x18\x02 \x03(\x0b\x32\x0c.DynamicType\x12\r\n\x05\x65rrno\x18\x03 \x01(\x05\x12\r\n\x05\x65rror\x18\x05 \x01(\t\x12\x11\n\ttraceback\x18\x06 \x01(\t\"F\n\nIPCMessage\x12\x19\n\x05\x65rror\x18\x01 \x01(\x0b\x32\n.Exception\x12\x0c\n\x04\x63ode\x18\x02 \x01(\x05\x12\x0f\n\x07message\x18\x03 \x01(\t')



_DYNAMICTYPE_PYTHONTYPE = descriptor.EnumDescriptor(
  name='PythonType',
  full_name='DynamicType.PythonType',
  filename=None,
  file=DESCRIPTOR,
  values=[
    descriptor.EnumValueDescriptor(
      name='STRING', index=0, number=0,
      options=None,
      type=None),
    descriptor.EnumValueDescriptor(
      name='INTEGER', index=1, number=1,
      options=None,
      type=None),
    descriptor.EnumValueDescriptor(
      name='BOOLEAN', index=2, number=2,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=114,
  serialized_end=164,
)


_DYNAMICTYPE = descriptor.Descriptor(
  name='DynamicType',
  full_name='DynamicType',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='type', full_name='DynamicType.type', index=0,
      number=1, type=14, cpp_type=8, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='int', full_name='DynamicType.int', index=1,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='string', full_name='DynamicType.string', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='boolean', full_name='DynamicType.boolean', index=3,
      number=4, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _DYNAMICTYPE_PYTHONTYPE,
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=14,
  serialized_end=164,
)


_EXCEPTION = descriptor.Descriptor(
  name='Exception',
  full_name='Exception',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='classname', full_name='Exception.classname', index=0,
      number=1, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='message', full_name='Exception.message', index=1,
      number=4, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='args', full_name='Exception.args', index=2,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='errno', full_name='Exception.errno', index=3,
      number=3, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='error', full_name='Exception.error', index=4,
      number=5, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='traceback', full_name='Exception.traceback', index=5,
      number=6, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=166,
  serialized_end=290,
)


_IPCMESSAGE = descriptor.Descriptor(
  name='IPCMessage',
  full_name='IPCMessage',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='error', full_name='IPCMessage.error', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='code', full_name='IPCMessage.code', index=1,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='message', full_name='IPCMessage.message', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=292,
  serialized_end=362,
)

_DYNAMICTYPE.fields_by_name['type'].enum_type = _DYNAMICTYPE_PYTHONTYPE
_DYNAMICTYPE_PYTHONTYPE.containing_type = _DYNAMICTYPE;
_EXCEPTION.fields_by_name['args'].message_type = _DYNAMICTYPE
_IPCMESSAGE.fields_by_name['error'].message_type = _EXCEPTION
DESCRIPTOR.message_types_by_name['DynamicType'] = _DYNAMICTYPE
DESCRIPTOR.message_types_by_name['Exception'] = _EXCEPTION
DESCRIPTOR.message_types_by_name['IPCMessage'] = _IPCMESSAGE

class DynamicType(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _DYNAMICTYPE
  
  # @@protoc_insertion_point(class_scope:DynamicType)

class Exception(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _EXCEPTION
  
  # @@protoc_insertion_point(class_scope:Exception)

class IPCMessage(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _IPCMESSAGE
  
  # @@protoc_insertion_point(class_scope:IPCMessage)

# @@protoc_insertion_point(module_scope)