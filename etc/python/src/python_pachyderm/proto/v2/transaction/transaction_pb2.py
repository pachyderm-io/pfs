# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: python_pachyderm/proto/v2/transaction/transaction.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from python_pachyderm.proto.v2.gogoproto import gogo_pb2 as python__pachyderm_dot_proto_dot_v2_dot_gogoproto_dot_gogo__pb2
from python_pachyderm.proto.v2.pfs import pfs_pb2 as python__pachyderm_dot_proto_dot_v2_dot_pfs_dot_pfs__pb2
from python_pachyderm.proto.v2.pps import pps_pb2 as python__pachyderm_dot_proto_dot_v2_dot_pps_dot_pps__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='python_pachyderm/proto/v2/transaction/transaction.proto',
  package='transaction',
  syntax='proto3',
  serialized_options=b'Z1github.com/pachyderm/pachyderm/v2/src/transaction',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n7python_pachyderm/proto/v2/transaction/transaction.proto\x12\x0btransaction\x1a\x1bgoogle/protobuf/empty.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a.python_pachyderm/proto/v2/gogoproto/gogo.proto\x1a\'python_pachyderm/proto/v2/pfs/pfs.proto\x1a\'python_pachyderm/proto/v2/pps/pps.proto\"\x12\n\x10\x44\x65leteAllRequest\"\xc8\x04\n\x12TransactionRequest\x12+\n\x0b\x63reate_repo\x18\x01 \x01(\x0b\x32\x16.pfs.CreateRepoRequest\x12+\n\x0b\x64\x65lete_repo\x18\x02 \x01(\x0b\x32\x16.pfs.DeleteRepoRequest\x12-\n\x0cstart_commit\x18\x03 \x01(\x0b\x32\x17.pfs.StartCommitRequest\x12/\n\rfinish_commit\x18\x04 \x01(\x0b\x32\x18.pfs.FinishCommitRequest\x12/\n\rsquash_commit\x18\x05 \x01(\x0b\x32\x18.pfs.SquashCommitRequest\x12/\n\rcreate_branch\x18\x06 \x01(\x0b\x32\x18.pfs.CreateBranchRequest\x12/\n\rdelete_branch\x18\x07 \x01(\x0b\x32\x18.pfs.DeleteBranchRequest\x12\x45\n\x19update_pipeline_job_state\x18\x08 \x01(\x0b\x32\".pps.UpdatePipelineJobStateRequest\x12\x33\n\x0f\x63reate_pipeline\x18\t \x01(\x0b\x32\x1a.pps.CreatePipelineRequest\x12\x36\n\x11stop_pipeline_job\x18\n \x01(\x0b\x32\x1b.pps.StopPipelineJobRequest\x12\x31\n\ndelete_all\x18\x0b \x01(\x0b\x32\x1d.transaction.DeleteAllRequest\"\x84\x01\n\x13TransactionResponse\x12\x1b\n\x06\x63ommit\x18\x01 \x01(\x0b\x32\x0b.pfs.Commit\x12P\n\x18\x63reate_pipeline_response\x18\x02 \x01(\x0b\x32..transaction.CreatePipelineTransactionResponse\"^\n!CreatePipelineTransactionResponse\x12\x12\n\nfileset_id\x18\x01 \x01(\t\x12%\n\x10prev_spec_commit\x18\x02 \x01(\x0b\x32\x0b.pfs.Commit\"!\n\x0bTransaction\x12\x12\n\x02id\x18\x01 \x01(\tB\x06\xe2\xde\x1f\x02ID\"\xd5\x01\n\x0fTransactionInfo\x12-\n\x0btransaction\x18\x01 \x01(\x0b\x32\x18.transaction.Transaction\x12\x31\n\x08requests\x18\x02 \x03(\x0b\x32\x1f.transaction.TransactionRequest\x12\x33\n\tresponses\x18\x03 \x03(\x0b\x32 .transaction.TransactionResponse\x12+\n\x07started\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\"J\n\x10TransactionInfos\x12\x36\n\x10transaction_info\x18\x01 \x03(\x0b\x32\x1c.transaction.TransactionInfo\"L\n\x17\x42\x61tchTransactionRequest\x12\x31\n\x08requests\x18\x01 \x03(\x0b\x32\x1f.transaction.TransactionRequest\"\x19\n\x17StartTransactionRequest\"J\n\x19InspectTransactionRequest\x12-\n\x0btransaction\x18\x01 \x01(\x0b\x32\x18.transaction.Transaction\"I\n\x18\x44\x65leteTransactionRequest\x12-\n\x0btransaction\x18\x01 \x01(\x0b\x32\x18.transaction.Transaction\"\x18\n\x16ListTransactionRequest\"I\n\x18\x46inishTransactionRequest\x12-\n\x0btransaction\x18\x01 \x01(\x0b\x32\x18.transaction.Transaction2\xe4\x04\n\x03\x41PI\x12X\n\x10\x42\x61tchTransaction\x12$.transaction.BatchTransactionRequest\x1a\x1c.transaction.TransactionInfo\"\x00\x12T\n\x10StartTransaction\x12$.transaction.StartTransactionRequest\x1a\x18.transaction.Transaction\"\x00\x12\\\n\x12InspectTransaction\x12&.transaction.InspectTransactionRequest\x1a\x1c.transaction.TransactionInfo\"\x00\x12T\n\x11\x44\x65leteTransaction\x12%.transaction.DeleteTransactionRequest\x1a\x16.google.protobuf.Empty\"\x00\x12W\n\x0fListTransaction\x12#.transaction.ListTransactionRequest\x1a\x1d.transaction.TransactionInfos\"\x00\x12Z\n\x11\x46inishTransaction\x12%.transaction.FinishTransactionRequest\x1a\x1c.transaction.TransactionInfo\"\x00\x12\x44\n\tDeleteAll\x12\x1d.transaction.DeleteAllRequest\x1a\x16.google.protobuf.Empty\"\x00\x42\x33Z1github.com/pachyderm/pachyderm/v2/src/transactionb\x06proto3'
  ,
  dependencies=[google_dot_protobuf_dot_empty__pb2.DESCRIPTOR,google_dot_protobuf_dot_timestamp__pb2.DESCRIPTOR,python__pachyderm_dot_proto_dot_v2_dot_gogoproto_dot_gogo__pb2.DESCRIPTOR,python__pachyderm_dot_proto_dot_v2_dot_pfs_dot_pfs__pb2.DESCRIPTOR,python__pachyderm_dot_proto_dot_v2_dot_pps_dot_pps__pb2.DESCRIPTOR,])




_DELETEALLREQUEST = _descriptor.Descriptor(
  name='DeleteAllRequest',
  full_name='transaction.DeleteAllRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=264,
  serialized_end=282,
)


_TRANSACTIONREQUEST = _descriptor.Descriptor(
  name='TransactionRequest',
  full_name='transaction.TransactionRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='create_repo', full_name='transaction.TransactionRequest.create_repo', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='delete_repo', full_name='transaction.TransactionRequest.delete_repo', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='start_commit', full_name='transaction.TransactionRequest.start_commit', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='finish_commit', full_name='transaction.TransactionRequest.finish_commit', index=3,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='squash_commit', full_name='transaction.TransactionRequest.squash_commit', index=4,
      number=5, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='create_branch', full_name='transaction.TransactionRequest.create_branch', index=5,
      number=6, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='delete_branch', full_name='transaction.TransactionRequest.delete_branch', index=6,
      number=7, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='update_pipeline_job_state', full_name='transaction.TransactionRequest.update_pipeline_job_state', index=7,
      number=8, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='create_pipeline', full_name='transaction.TransactionRequest.create_pipeline', index=8,
      number=9, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='stop_pipeline_job', full_name='transaction.TransactionRequest.stop_pipeline_job', index=9,
      number=10, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='delete_all', full_name='transaction.TransactionRequest.delete_all', index=10,
      number=11, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=285,
  serialized_end=869,
)


_TRANSACTIONRESPONSE = _descriptor.Descriptor(
  name='TransactionResponse',
  full_name='transaction.TransactionResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='commit', full_name='transaction.TransactionResponse.commit', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='create_pipeline_response', full_name='transaction.TransactionResponse.create_pipeline_response', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=872,
  serialized_end=1004,
)


_CREATEPIPELINETRANSACTIONRESPONSE = _descriptor.Descriptor(
  name='CreatePipelineTransactionResponse',
  full_name='transaction.CreatePipelineTransactionResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='fileset_id', full_name='transaction.CreatePipelineTransactionResponse.fileset_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='prev_spec_commit', full_name='transaction.CreatePipelineTransactionResponse.prev_spec_commit', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1006,
  serialized_end=1100,
)


_TRANSACTION = _descriptor.Descriptor(
  name='Transaction',
  full_name='transaction.Transaction',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='transaction.Transaction.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=b'\342\336\037\002ID', file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1102,
  serialized_end=1135,
)


_TRANSACTIONINFO = _descriptor.Descriptor(
  name='TransactionInfo',
  full_name='transaction.TransactionInfo',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='transaction', full_name='transaction.TransactionInfo.transaction', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='requests', full_name='transaction.TransactionInfo.requests', index=1,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='responses', full_name='transaction.TransactionInfo.responses', index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='started', full_name='transaction.TransactionInfo.started', index=3,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1138,
  serialized_end=1351,
)


_TRANSACTIONINFOS = _descriptor.Descriptor(
  name='TransactionInfos',
  full_name='transaction.TransactionInfos',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='transaction_info', full_name='transaction.TransactionInfos.transaction_info', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1353,
  serialized_end=1427,
)


_BATCHTRANSACTIONREQUEST = _descriptor.Descriptor(
  name='BatchTransactionRequest',
  full_name='transaction.BatchTransactionRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='requests', full_name='transaction.BatchTransactionRequest.requests', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1429,
  serialized_end=1505,
)


_STARTTRANSACTIONREQUEST = _descriptor.Descriptor(
  name='StartTransactionRequest',
  full_name='transaction.StartTransactionRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1507,
  serialized_end=1532,
)


_INSPECTTRANSACTIONREQUEST = _descriptor.Descriptor(
  name='InspectTransactionRequest',
  full_name='transaction.InspectTransactionRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='transaction', full_name='transaction.InspectTransactionRequest.transaction', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1534,
  serialized_end=1608,
)


_DELETETRANSACTIONREQUEST = _descriptor.Descriptor(
  name='DeleteTransactionRequest',
  full_name='transaction.DeleteTransactionRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='transaction', full_name='transaction.DeleteTransactionRequest.transaction', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1610,
  serialized_end=1683,
)


_LISTTRANSACTIONREQUEST = _descriptor.Descriptor(
  name='ListTransactionRequest',
  full_name='transaction.ListTransactionRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1685,
  serialized_end=1709,
)


_FINISHTRANSACTIONREQUEST = _descriptor.Descriptor(
  name='FinishTransactionRequest',
  full_name='transaction.FinishTransactionRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='transaction', full_name='transaction.FinishTransactionRequest.transaction', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1711,
  serialized_end=1784,
)

_TRANSACTIONREQUEST.fields_by_name['create_repo'].message_type = python__pachyderm_dot_proto_dot_v2_dot_pfs_dot_pfs__pb2._CREATEREPOREQUEST
_TRANSACTIONREQUEST.fields_by_name['delete_repo'].message_type = python__pachyderm_dot_proto_dot_v2_dot_pfs_dot_pfs__pb2._DELETEREPOREQUEST
_TRANSACTIONREQUEST.fields_by_name['start_commit'].message_type = python__pachyderm_dot_proto_dot_v2_dot_pfs_dot_pfs__pb2._STARTCOMMITREQUEST
_TRANSACTIONREQUEST.fields_by_name['finish_commit'].message_type = python__pachyderm_dot_proto_dot_v2_dot_pfs_dot_pfs__pb2._FINISHCOMMITREQUEST
_TRANSACTIONREQUEST.fields_by_name['squash_commit'].message_type = python__pachyderm_dot_proto_dot_v2_dot_pfs_dot_pfs__pb2._SQUASHCOMMITREQUEST
_TRANSACTIONREQUEST.fields_by_name['create_branch'].message_type = python__pachyderm_dot_proto_dot_v2_dot_pfs_dot_pfs__pb2._CREATEBRANCHREQUEST
_TRANSACTIONREQUEST.fields_by_name['delete_branch'].message_type = python__pachyderm_dot_proto_dot_v2_dot_pfs_dot_pfs__pb2._DELETEBRANCHREQUEST
_TRANSACTIONREQUEST.fields_by_name['update_pipeline_job_state'].message_type = python__pachyderm_dot_proto_dot_v2_dot_pps_dot_pps__pb2._UPDATEPIPELINEJOBSTATEREQUEST
_TRANSACTIONREQUEST.fields_by_name['create_pipeline'].message_type = python__pachyderm_dot_proto_dot_v2_dot_pps_dot_pps__pb2._CREATEPIPELINEREQUEST
_TRANSACTIONREQUEST.fields_by_name['stop_pipeline_job'].message_type = python__pachyderm_dot_proto_dot_v2_dot_pps_dot_pps__pb2._STOPPIPELINEJOBREQUEST
_TRANSACTIONREQUEST.fields_by_name['delete_all'].message_type = _DELETEALLREQUEST
_TRANSACTIONRESPONSE.fields_by_name['commit'].message_type = python__pachyderm_dot_proto_dot_v2_dot_pfs_dot_pfs__pb2._COMMIT
_TRANSACTIONRESPONSE.fields_by_name['create_pipeline_response'].message_type = _CREATEPIPELINETRANSACTIONRESPONSE
_CREATEPIPELINETRANSACTIONRESPONSE.fields_by_name['prev_spec_commit'].message_type = python__pachyderm_dot_proto_dot_v2_dot_pfs_dot_pfs__pb2._COMMIT
_TRANSACTIONINFO.fields_by_name['transaction'].message_type = _TRANSACTION
_TRANSACTIONINFO.fields_by_name['requests'].message_type = _TRANSACTIONREQUEST
_TRANSACTIONINFO.fields_by_name['responses'].message_type = _TRANSACTIONRESPONSE
_TRANSACTIONINFO.fields_by_name['started'].message_type = google_dot_protobuf_dot_timestamp__pb2._TIMESTAMP
_TRANSACTIONINFOS.fields_by_name['transaction_info'].message_type = _TRANSACTIONINFO
_BATCHTRANSACTIONREQUEST.fields_by_name['requests'].message_type = _TRANSACTIONREQUEST
_INSPECTTRANSACTIONREQUEST.fields_by_name['transaction'].message_type = _TRANSACTION
_DELETETRANSACTIONREQUEST.fields_by_name['transaction'].message_type = _TRANSACTION
_FINISHTRANSACTIONREQUEST.fields_by_name['transaction'].message_type = _TRANSACTION
DESCRIPTOR.message_types_by_name['DeleteAllRequest'] = _DELETEALLREQUEST
DESCRIPTOR.message_types_by_name['TransactionRequest'] = _TRANSACTIONREQUEST
DESCRIPTOR.message_types_by_name['TransactionResponse'] = _TRANSACTIONRESPONSE
DESCRIPTOR.message_types_by_name['CreatePipelineTransactionResponse'] = _CREATEPIPELINETRANSACTIONRESPONSE
DESCRIPTOR.message_types_by_name['Transaction'] = _TRANSACTION
DESCRIPTOR.message_types_by_name['TransactionInfo'] = _TRANSACTIONINFO
DESCRIPTOR.message_types_by_name['TransactionInfos'] = _TRANSACTIONINFOS
DESCRIPTOR.message_types_by_name['BatchTransactionRequest'] = _BATCHTRANSACTIONREQUEST
DESCRIPTOR.message_types_by_name['StartTransactionRequest'] = _STARTTRANSACTIONREQUEST
DESCRIPTOR.message_types_by_name['InspectTransactionRequest'] = _INSPECTTRANSACTIONREQUEST
DESCRIPTOR.message_types_by_name['DeleteTransactionRequest'] = _DELETETRANSACTIONREQUEST
DESCRIPTOR.message_types_by_name['ListTransactionRequest'] = _LISTTRANSACTIONREQUEST
DESCRIPTOR.message_types_by_name['FinishTransactionRequest'] = _FINISHTRANSACTIONREQUEST
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

DeleteAllRequest = _reflection.GeneratedProtocolMessageType('DeleteAllRequest', (_message.Message,), {
  'DESCRIPTOR' : _DELETEALLREQUEST,
  '__module__' : 'python_pachyderm.proto.v2.transaction.transaction_pb2'
  # @@protoc_insertion_point(class_scope:transaction.DeleteAllRequest)
  })
_sym_db.RegisterMessage(DeleteAllRequest)

TransactionRequest = _reflection.GeneratedProtocolMessageType('TransactionRequest', (_message.Message,), {
  'DESCRIPTOR' : _TRANSACTIONREQUEST,
  '__module__' : 'python_pachyderm.proto.v2.transaction.transaction_pb2'
  # @@protoc_insertion_point(class_scope:transaction.TransactionRequest)
  })
_sym_db.RegisterMessage(TransactionRequest)

TransactionResponse = _reflection.GeneratedProtocolMessageType('TransactionResponse', (_message.Message,), {
  'DESCRIPTOR' : _TRANSACTIONRESPONSE,
  '__module__' : 'python_pachyderm.proto.v2.transaction.transaction_pb2'
  # @@protoc_insertion_point(class_scope:transaction.TransactionResponse)
  })
_sym_db.RegisterMessage(TransactionResponse)

CreatePipelineTransactionResponse = _reflection.GeneratedProtocolMessageType('CreatePipelineTransactionResponse', (_message.Message,), {
  'DESCRIPTOR' : _CREATEPIPELINETRANSACTIONRESPONSE,
  '__module__' : 'python_pachyderm.proto.v2.transaction.transaction_pb2'
  # @@protoc_insertion_point(class_scope:transaction.CreatePipelineTransactionResponse)
  })
_sym_db.RegisterMessage(CreatePipelineTransactionResponse)

Transaction = _reflection.GeneratedProtocolMessageType('Transaction', (_message.Message,), {
  'DESCRIPTOR' : _TRANSACTION,
  '__module__' : 'python_pachyderm.proto.v2.transaction.transaction_pb2'
  # @@protoc_insertion_point(class_scope:transaction.Transaction)
  })
_sym_db.RegisterMessage(Transaction)

TransactionInfo = _reflection.GeneratedProtocolMessageType('TransactionInfo', (_message.Message,), {
  'DESCRIPTOR' : _TRANSACTIONINFO,
  '__module__' : 'python_pachyderm.proto.v2.transaction.transaction_pb2'
  # @@protoc_insertion_point(class_scope:transaction.TransactionInfo)
  })
_sym_db.RegisterMessage(TransactionInfo)

TransactionInfos = _reflection.GeneratedProtocolMessageType('TransactionInfos', (_message.Message,), {
  'DESCRIPTOR' : _TRANSACTIONINFOS,
  '__module__' : 'python_pachyderm.proto.v2.transaction.transaction_pb2'
  # @@protoc_insertion_point(class_scope:transaction.TransactionInfos)
  })
_sym_db.RegisterMessage(TransactionInfos)

BatchTransactionRequest = _reflection.GeneratedProtocolMessageType('BatchTransactionRequest', (_message.Message,), {
  'DESCRIPTOR' : _BATCHTRANSACTIONREQUEST,
  '__module__' : 'python_pachyderm.proto.v2.transaction.transaction_pb2'
  # @@protoc_insertion_point(class_scope:transaction.BatchTransactionRequest)
  })
_sym_db.RegisterMessage(BatchTransactionRequest)

StartTransactionRequest = _reflection.GeneratedProtocolMessageType('StartTransactionRequest', (_message.Message,), {
  'DESCRIPTOR' : _STARTTRANSACTIONREQUEST,
  '__module__' : 'python_pachyderm.proto.v2.transaction.transaction_pb2'
  # @@protoc_insertion_point(class_scope:transaction.StartTransactionRequest)
  })
_sym_db.RegisterMessage(StartTransactionRequest)

InspectTransactionRequest = _reflection.GeneratedProtocolMessageType('InspectTransactionRequest', (_message.Message,), {
  'DESCRIPTOR' : _INSPECTTRANSACTIONREQUEST,
  '__module__' : 'python_pachyderm.proto.v2.transaction.transaction_pb2'
  # @@protoc_insertion_point(class_scope:transaction.InspectTransactionRequest)
  })
_sym_db.RegisterMessage(InspectTransactionRequest)

DeleteTransactionRequest = _reflection.GeneratedProtocolMessageType('DeleteTransactionRequest', (_message.Message,), {
  'DESCRIPTOR' : _DELETETRANSACTIONREQUEST,
  '__module__' : 'python_pachyderm.proto.v2.transaction.transaction_pb2'
  # @@protoc_insertion_point(class_scope:transaction.DeleteTransactionRequest)
  })
_sym_db.RegisterMessage(DeleteTransactionRequest)

ListTransactionRequest = _reflection.GeneratedProtocolMessageType('ListTransactionRequest', (_message.Message,), {
  'DESCRIPTOR' : _LISTTRANSACTIONREQUEST,
  '__module__' : 'python_pachyderm.proto.v2.transaction.transaction_pb2'
  # @@protoc_insertion_point(class_scope:transaction.ListTransactionRequest)
  })
_sym_db.RegisterMessage(ListTransactionRequest)

FinishTransactionRequest = _reflection.GeneratedProtocolMessageType('FinishTransactionRequest', (_message.Message,), {
  'DESCRIPTOR' : _FINISHTRANSACTIONREQUEST,
  '__module__' : 'python_pachyderm.proto.v2.transaction.transaction_pb2'
  # @@protoc_insertion_point(class_scope:transaction.FinishTransactionRequest)
  })
_sym_db.RegisterMessage(FinishTransactionRequest)


DESCRIPTOR._options = None
_TRANSACTION.fields_by_name['id']._options = None

_API = _descriptor.ServiceDescriptor(
  name='API',
  full_name='transaction.API',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=1787,
  serialized_end=2399,
  methods=[
  _descriptor.MethodDescriptor(
    name='BatchTransaction',
    full_name='transaction.API.BatchTransaction',
    index=0,
    containing_service=None,
    input_type=_BATCHTRANSACTIONREQUEST,
    output_type=_TRANSACTIONINFO,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='StartTransaction',
    full_name='transaction.API.StartTransaction',
    index=1,
    containing_service=None,
    input_type=_STARTTRANSACTIONREQUEST,
    output_type=_TRANSACTION,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='InspectTransaction',
    full_name='transaction.API.InspectTransaction',
    index=2,
    containing_service=None,
    input_type=_INSPECTTRANSACTIONREQUEST,
    output_type=_TRANSACTIONINFO,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='DeleteTransaction',
    full_name='transaction.API.DeleteTransaction',
    index=3,
    containing_service=None,
    input_type=_DELETETRANSACTIONREQUEST,
    output_type=google_dot_protobuf_dot_empty__pb2._EMPTY,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='ListTransaction',
    full_name='transaction.API.ListTransaction',
    index=4,
    containing_service=None,
    input_type=_LISTTRANSACTIONREQUEST,
    output_type=_TRANSACTIONINFOS,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='FinishTransaction',
    full_name='transaction.API.FinishTransaction',
    index=5,
    containing_service=None,
    input_type=_FINISHTRANSACTIONREQUEST,
    output_type=_TRANSACTIONINFO,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='DeleteAll',
    full_name='transaction.API.DeleteAll',
    index=6,
    containing_service=None,
    input_type=_DELETEALLREQUEST,
    output_type=google_dot_protobuf_dot_empty__pb2._EMPTY,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_API)

DESCRIPTOR.services_by_name['API'] = _API

# @@protoc_insertion_point(module_scope)
