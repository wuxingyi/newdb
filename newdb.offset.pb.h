// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: newdb.offset.proto

#ifndef PROTOBUF_newdb_2eoffset_2eproto__INCLUDED
#define PROTOBUF_newdb_2eoffset_2eproto__INCLUDED

#include <string>

#include <google/protobuf/stubs/common.h>

#if GOOGLE_PROTOBUF_VERSION < 2005000
#error This file was generated by a newer version of protoc which is
#error incompatible with your Protocol Buffer headers.  Please update
#error your headers.
#endif
#if 2005000 < GOOGLE_PROTOBUF_MIN_PROTOC_VERSION
#error This file was generated by an older version of protoc which is
#error incompatible with your Protocol Buffer headers.  Please
#error regenerate this file with a newer version of protoc.
#endif

#include <google/protobuf/generated_message_util.h>
#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/unknown_field_set.h>
// @@protoc_insertion_point(includes)

namespace newdb {

// Internal implementation detail -- do not call these.
void  protobuf_AddDesc_newdb_2eoffset_2eproto();
void protobuf_AssignDesc_newdb_2eoffset_2eproto();
void protobuf_ShutdownFile_newdb_2eoffset_2eproto();

class dboffset;

// ===================================================================

class dboffset : public ::google::protobuf::Message {
 public:
  dboffset();
  virtual ~dboffset();

  dboffset(const dboffset& from);

  inline dboffset& operator=(const dboffset& from) {
    CopyFrom(from);
    return *this;
  }

  inline const ::google::protobuf::UnknownFieldSet& unknown_fields() const {
    return _unknown_fields_;
  }

  inline ::google::protobuf::UnknownFieldSet* mutable_unknown_fields() {
    return &_unknown_fields_;
  }

  static const ::google::protobuf::Descriptor* descriptor();
  static const dboffset& default_instance();

  void Swap(dboffset* other);

  // implements Message ----------------------------------------------

  dboffset* New() const;
  void CopyFrom(const ::google::protobuf::Message& from);
  void MergeFrom(const ::google::protobuf::Message& from);
  void CopyFrom(const dboffset& from);
  void MergeFrom(const dboffset& from);
  void Clear();
  bool IsInitialized() const;

  int ByteSize() const;
  bool MergePartialFromCodedStream(
      ::google::protobuf::io::CodedInputStream* input);
  void SerializeWithCachedSizes(
      ::google::protobuf::io::CodedOutputStream* output) const;
  ::google::protobuf::uint8* SerializeWithCachedSizesToArray(::google::protobuf::uint8* output) const;
  int GetCachedSize() const { return _cached_size_; }
  private:
  void SharedCtor();
  void SharedDtor();
  void SetCachedSize(int size) const;
  public:

  ::google::protobuf::Metadata GetMetadata() const;

  // nested types ----------------------------------------------------

  // accessors -------------------------------------------------------

  // required int64 length = 1;
  inline bool has_length() const;
  inline void clear_length();
  static const int kLengthFieldNumber = 1;
  inline ::google::protobuf::int64 length() const;
  inline void set_length(::google::protobuf::int64 value);

  // required int64 offset = 2;
  inline bool has_offset() const;
  inline void clear_offset();
  static const int kOffsetFieldNumber = 2;
  inline ::google::protobuf::int64 offset() const;
  inline void set_offset(::google::protobuf::int64 value);

  // @@protoc_insertion_point(class_scope:newdb.dboffset)
 private:
  inline void set_has_length();
  inline void clear_has_length();
  inline void set_has_offset();
  inline void clear_has_offset();

  ::google::protobuf::UnknownFieldSet _unknown_fields_;

  ::google::protobuf::int64 length_;
  ::google::protobuf::int64 offset_;

  mutable int _cached_size_;
  ::google::protobuf::uint32 _has_bits_[(2 + 31) / 32];

  friend void  protobuf_AddDesc_newdb_2eoffset_2eproto();
  friend void protobuf_AssignDesc_newdb_2eoffset_2eproto();
  friend void protobuf_ShutdownFile_newdb_2eoffset_2eproto();

  void InitAsDefaultInstance();
  static dboffset* default_instance_;
};
// ===================================================================


// ===================================================================

// dboffset

// required int64 length = 1;
inline bool dboffset::has_length() const {
  return (_has_bits_[0] & 0x00000001u) != 0;
}
inline void dboffset::set_has_length() {
  _has_bits_[0] |= 0x00000001u;
}
inline void dboffset::clear_has_length() {
  _has_bits_[0] &= ~0x00000001u;
}
inline void dboffset::clear_length() {
  length_ = GOOGLE_LONGLONG(0);
  clear_has_length();
}
inline ::google::protobuf::int64 dboffset::length() const {
  return length_;
}
inline void dboffset::set_length(::google::protobuf::int64 value) {
  set_has_length();
  length_ = value;
}

// required int64 offset = 2;
inline bool dboffset::has_offset() const {
  return (_has_bits_[0] & 0x00000002u) != 0;
}
inline void dboffset::set_has_offset() {
  _has_bits_[0] |= 0x00000002u;
}
inline void dboffset::clear_has_offset() {
  _has_bits_[0] &= ~0x00000002u;
}
inline void dboffset::clear_offset() {
  offset_ = GOOGLE_LONGLONG(0);
  clear_has_offset();
}
inline ::google::protobuf::int64 dboffset::offset() const {
  return offset_;
}
inline void dboffset::set_offset(::google::protobuf::int64 value) {
  set_has_offset();
  offset_ = value;
}


// @@protoc_insertion_point(namespace_scope)

}  // namespace newdb

#ifndef SWIG
namespace google {
namespace protobuf {


}  // namespace google
}  // namespace protobuf
#endif  // SWIG

// @@protoc_insertion_point(global_scope)

#endif  // PROTOBUF_newdb_2eoffset_2eproto__INCLUDED
