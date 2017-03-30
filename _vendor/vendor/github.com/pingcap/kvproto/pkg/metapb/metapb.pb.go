// Code generated by protoc-gen-gogo.
// source: metapb.proto
// DO NOT EDIT!

/*
	Package metapb is a generated protocol buffer package.

	It is generated from these files:
		metapb.proto

	It has these top-level messages:
		Cluster
		StoreLabel
		Store
		RegionEpoch
		Region
		Peer
*/
package metapb

import (
	"fmt"
	"io"
	"math"

	proto "github.com/golang/protobuf/proto"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type StoreState int32

const (
	StoreState_Up        StoreState = 0
	StoreState_Offline   StoreState = 1
	StoreState_Tombstone StoreState = 2
)

var StoreState_name = map[int32]string{
	0: "Up",
	1: "Offline",
	2: "Tombstone",
}
var StoreState_value = map[string]int32{
	"Up":        0,
	"Offline":   1,
	"Tombstone": 2,
}

func (x StoreState) Enum() *StoreState {
	p := new(StoreState)
	*p = x
	return p
}
func (x StoreState) String() string {
	return proto.EnumName(StoreState_name, int32(x))
}
func (x *StoreState) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(StoreState_value, data, "StoreState")
	if err != nil {
		return err
	}
	*x = StoreState(value)
	return nil
}
func (StoreState) EnumDescriptor() ([]byte, []int) { return fileDescriptorMetapb, []int{0} }

type Cluster struct {
	Id uint64 `protobuf:"varint,1,opt,name=id" json:"id"`
	// max peer count for a region.
	// pd will do the auto-balance if region peer count mismatches.
	MaxPeerCount     uint32 `protobuf:"varint,2,opt,name=max_peer_count,json=maxPeerCount" json:"max_peer_count"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *Cluster) Reset()                    { *m = Cluster{} }
func (m *Cluster) String() string            { return proto.CompactTextString(m) }
func (*Cluster) ProtoMessage()               {}
func (*Cluster) Descriptor() ([]byte, []int) { return fileDescriptorMetapb, []int{0} }

func (m *Cluster) GetId() uint64 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *Cluster) GetMaxPeerCount() uint32 {
	if m != nil {
		return m.MaxPeerCount
	}
	return 0
}

// Case insensitive key/value for replica constraints.
type StoreLabel struct {
	Key              string `protobuf:"bytes,1,opt,name=key" json:"key"`
	Value            string `protobuf:"bytes,2,opt,name=value" json:"value"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *StoreLabel) Reset()                    { *m = StoreLabel{} }
func (m *StoreLabel) String() string            { return proto.CompactTextString(m) }
func (*StoreLabel) ProtoMessage()               {}
func (*StoreLabel) Descriptor() ([]byte, []int) { return fileDescriptorMetapb, []int{1} }

func (m *StoreLabel) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *StoreLabel) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

type Store struct {
	Id               uint64        `protobuf:"varint,1,opt,name=id" json:"id"`
	Address          string        `protobuf:"bytes,2,opt,name=address" json:"address"`
	State            StoreState    `protobuf:"varint,3,opt,name=state,enum=metapb.StoreState" json:"state"`
	Labels           []*StoreLabel `protobuf:"bytes,4,rep,name=labels" json:"labels,omitempty"`
	XXX_unrecognized []byte        `json:"-"`
}

func (m *Store) Reset()                    { *m = Store{} }
func (m *Store) String() string            { return proto.CompactTextString(m) }
func (*Store) ProtoMessage()               {}
func (*Store) Descriptor() ([]byte, []int) { return fileDescriptorMetapb, []int{2} }

func (m *Store) GetId() uint64 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *Store) GetAddress() string {
	if m != nil {
		return m.Address
	}
	return ""
}

func (m *Store) GetState() StoreState {
	if m != nil {
		return m.State
	}
	return StoreState_Up
}

func (m *Store) GetLabels() []*StoreLabel {
	if m != nil {
		return m.Labels
	}
	return nil
}

type RegionEpoch struct {
	// Conf change version, auto increment when add or remove peer
	ConfVer uint64 `protobuf:"varint,1,opt,name=conf_ver,json=confVer" json:"conf_ver"`
	// Region version, auto increment when split or merge
	Version          uint64 `protobuf:"varint,2,opt,name=version" json:"version"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *RegionEpoch) Reset()                    { *m = RegionEpoch{} }
func (m *RegionEpoch) String() string            { return proto.CompactTextString(m) }
func (*RegionEpoch) ProtoMessage()               {}
func (*RegionEpoch) Descriptor() ([]byte, []int) { return fileDescriptorMetapb, []int{3} }

func (m *RegionEpoch) GetConfVer() uint64 {
	if m != nil {
		return m.ConfVer
	}
	return 0
}

func (m *RegionEpoch) GetVersion() uint64 {
	if m != nil {
		return m.Version
	}
	return 0
}

type Region struct {
	Id uint64 `protobuf:"varint,1,opt,name=id" json:"id"`
	// Region key range [start_key, end_key).
	StartKey         []byte       `protobuf:"bytes,2,opt,name=start_key,json=startKey" json:"start_key,omitempty"`
	EndKey           []byte       `protobuf:"bytes,3,opt,name=end_key,json=endKey" json:"end_key,omitempty"`
	RegionEpoch      *RegionEpoch `protobuf:"bytes,4,opt,name=region_epoch,json=regionEpoch" json:"region_epoch,omitempty"`
	Peers            []*Peer      `protobuf:"bytes,5,rep,name=peers" json:"peers,omitempty"`
	Term             *uint64      `protobuf:"varint,6,opt,name=term" json:"term,omitempty"`
	XXX_unrecognized []byte       `json:"-"`
}

func (m *Region) Reset()                    { *m = Region{} }
func (m *Region) String() string            { return proto.CompactTextString(m) }
func (*Region) ProtoMessage()               {}
func (*Region) Descriptor() ([]byte, []int) { return fileDescriptorMetapb, []int{4} }

func (m *Region) GetId() uint64 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *Region) GetStartKey() []byte {
	if m != nil {
		return m.StartKey
	}
	return nil
}

func (m *Region) GetEndKey() []byte {
	if m != nil {
		return m.EndKey
	}
	return nil
}

func (m *Region) GetRegionEpoch() *RegionEpoch {
	if m != nil {
		return m.RegionEpoch
	}
	return nil
}

func (m *Region) GetPeers() []*Peer {
	if m != nil {
		return m.Peers
	}
	return nil
}

func (m *Region) GetTerm() uint64 {
	if m != nil && m.Term != nil {
		return *m.Term
	}
	return 0
}

type Peer struct {
	Id               uint64 `protobuf:"varint,1,opt,name=id" json:"id"`
	StoreId          uint64 `protobuf:"varint,2,opt,name=store_id,json=storeId" json:"store_id"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *Peer) Reset()                    { *m = Peer{} }
func (m *Peer) String() string            { return proto.CompactTextString(m) }
func (*Peer) ProtoMessage()               {}
func (*Peer) Descriptor() ([]byte, []int) { return fileDescriptorMetapb, []int{5} }

func (m *Peer) GetId() uint64 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *Peer) GetStoreId() uint64 {
	if m != nil {
		return m.StoreId
	}
	return 0
}

func init() {
	proto.RegisterType((*Cluster)(nil), "metapb.Cluster")
	proto.RegisterType((*StoreLabel)(nil), "metapb.StoreLabel")
	proto.RegisterType((*Store)(nil), "metapb.Store")
	proto.RegisterType((*RegionEpoch)(nil), "metapb.RegionEpoch")
	proto.RegisterType((*Region)(nil), "metapb.Region")
	proto.RegisterType((*Peer)(nil), "metapb.Peer")
	proto.RegisterEnum("metapb.StoreState", StoreState_name, StoreState_value)
}
func (m *Cluster) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Cluster) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	dAtA[i] = 0x8
	i++
	i = encodeVarintMetapb(dAtA, i, uint64(m.Id))
	dAtA[i] = 0x10
	i++
	i = encodeVarintMetapb(dAtA, i, uint64(m.MaxPeerCount))
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *StoreLabel) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *StoreLabel) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	dAtA[i] = 0xa
	i++
	i = encodeVarintMetapb(dAtA, i, uint64(len(m.Key)))
	i += copy(dAtA[i:], m.Key)
	dAtA[i] = 0x12
	i++
	i = encodeVarintMetapb(dAtA, i, uint64(len(m.Value)))
	i += copy(dAtA[i:], m.Value)
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *Store) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Store) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	dAtA[i] = 0x8
	i++
	i = encodeVarintMetapb(dAtA, i, uint64(m.Id))
	dAtA[i] = 0x12
	i++
	i = encodeVarintMetapb(dAtA, i, uint64(len(m.Address)))
	i += copy(dAtA[i:], m.Address)
	dAtA[i] = 0x18
	i++
	i = encodeVarintMetapb(dAtA, i, uint64(m.State))
	if len(m.Labels) > 0 {
		for _, msg := range m.Labels {
			dAtA[i] = 0x22
			i++
			i = encodeVarintMetapb(dAtA, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(dAtA[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *RegionEpoch) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *RegionEpoch) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	dAtA[i] = 0x8
	i++
	i = encodeVarintMetapb(dAtA, i, uint64(m.ConfVer))
	dAtA[i] = 0x10
	i++
	i = encodeVarintMetapb(dAtA, i, uint64(m.Version))
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *Region) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Region) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	dAtA[i] = 0x8
	i++
	i = encodeVarintMetapb(dAtA, i, uint64(m.Id))
	if m.StartKey != nil {
		dAtA[i] = 0x12
		i++
		i = encodeVarintMetapb(dAtA, i, uint64(len(m.StartKey)))
		i += copy(dAtA[i:], m.StartKey)
	}
	if m.EndKey != nil {
		dAtA[i] = 0x1a
		i++
		i = encodeVarintMetapb(dAtA, i, uint64(len(m.EndKey)))
		i += copy(dAtA[i:], m.EndKey)
	}
	if m.RegionEpoch != nil {
		dAtA[i] = 0x22
		i++
		i = encodeVarintMetapb(dAtA, i, uint64(m.RegionEpoch.Size()))
		n1, err := m.RegionEpoch.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n1
	}
	if len(m.Peers) > 0 {
		for _, msg := range m.Peers {
			dAtA[i] = 0x2a
			i++
			i = encodeVarintMetapb(dAtA, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(dAtA[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	if m.Term != nil {
		dAtA[i] = 0x30
		i++
		i = encodeVarintMetapb(dAtA, i, uint64(*m.Term))
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *Peer) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Peer) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	dAtA[i] = 0x8
	i++
	i = encodeVarintMetapb(dAtA, i, uint64(m.Id))
	dAtA[i] = 0x10
	i++
	i = encodeVarintMetapb(dAtA, i, uint64(m.StoreId))
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func encodeFixed64Metapb(dAtA []byte, offset int, v uint64) int {
	dAtA[offset] = uint8(v)
	dAtA[offset+1] = uint8(v >> 8)
	dAtA[offset+2] = uint8(v >> 16)
	dAtA[offset+3] = uint8(v >> 24)
	dAtA[offset+4] = uint8(v >> 32)
	dAtA[offset+5] = uint8(v >> 40)
	dAtA[offset+6] = uint8(v >> 48)
	dAtA[offset+7] = uint8(v >> 56)
	return offset + 8
}
func encodeFixed32Metapb(dAtA []byte, offset int, v uint32) int {
	dAtA[offset] = uint8(v)
	dAtA[offset+1] = uint8(v >> 8)
	dAtA[offset+2] = uint8(v >> 16)
	dAtA[offset+3] = uint8(v >> 24)
	return offset + 4
}
func encodeVarintMetapb(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *Cluster) Size() (n int) {
	var l int
	_ = l
	n += 1 + sovMetapb(uint64(m.Id))
	n += 1 + sovMetapb(uint64(m.MaxPeerCount))
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *StoreLabel) Size() (n int) {
	var l int
	_ = l
	l = len(m.Key)
	n += 1 + l + sovMetapb(uint64(l))
	l = len(m.Value)
	n += 1 + l + sovMetapb(uint64(l))
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *Store) Size() (n int) {
	var l int
	_ = l
	n += 1 + sovMetapb(uint64(m.Id))
	l = len(m.Address)
	n += 1 + l + sovMetapb(uint64(l))
	n += 1 + sovMetapb(uint64(m.State))
	if len(m.Labels) > 0 {
		for _, e := range m.Labels {
			l = e.Size()
			n += 1 + l + sovMetapb(uint64(l))
		}
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *RegionEpoch) Size() (n int) {
	var l int
	_ = l
	n += 1 + sovMetapb(uint64(m.ConfVer))
	n += 1 + sovMetapb(uint64(m.Version))
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *Region) Size() (n int) {
	var l int
	_ = l
	n += 1 + sovMetapb(uint64(m.Id))
	if m.StartKey != nil {
		l = len(m.StartKey)
		n += 1 + l + sovMetapb(uint64(l))
	}
	if m.EndKey != nil {
		l = len(m.EndKey)
		n += 1 + l + sovMetapb(uint64(l))
	}
	if m.RegionEpoch != nil {
		l = m.RegionEpoch.Size()
		n += 1 + l + sovMetapb(uint64(l))
	}
	if len(m.Peers) > 0 {
		for _, e := range m.Peers {
			l = e.Size()
			n += 1 + l + sovMetapb(uint64(l))
		}
	}
	if m.Term != nil {
		n += 1 + sovMetapb(uint64(*m.Term))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *Peer) Size() (n int) {
	var l int
	_ = l
	n += 1 + sovMetapb(uint64(m.Id))
	n += 1 + sovMetapb(uint64(m.StoreId))
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func sovMetapb(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozMetapb(x uint64) (n int) {
	return sovMetapb(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Cluster) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetapb
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Cluster: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Cluster: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Id", wireType)
			}
			m.Id = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Id |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MaxPeerCount", wireType)
			}
			m.MaxPeerCount = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.MaxPeerCount |= (uint32(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMetapb(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMetapb
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *StoreLabel) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetapb
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: StoreLabel: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: StoreLabel: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Key", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthMetapb
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Key = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthMetapb
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Value = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipMetapb(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMetapb
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Store) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetapb
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Store: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Store: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Id", wireType)
			}
			m.Id = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Id |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Address", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthMetapb
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Address = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field State", wireType)
			}
			m.State = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.State |= (StoreState(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Labels", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthMetapb
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Labels = append(m.Labels, &StoreLabel{})
			if err := m.Labels[len(m.Labels)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipMetapb(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMetapb
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *RegionEpoch) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetapb
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: RegionEpoch: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: RegionEpoch: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field ConfVer", wireType)
			}
			m.ConfVer = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.ConfVer |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Version", wireType)
			}
			m.Version = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Version |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMetapb(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMetapb
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Region) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetapb
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Region: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Region: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Id", wireType)
			}
			m.Id = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Id |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field StartKey", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthMetapb
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.StartKey = append(m.StartKey[:0], dAtA[iNdEx:postIndex]...)
			if m.StartKey == nil {
				m.StartKey = []byte{}
			}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field EndKey", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthMetapb
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.EndKey = append(m.EndKey[:0], dAtA[iNdEx:postIndex]...)
			if m.EndKey == nil {
				m.EndKey = []byte{}
			}
			iNdEx = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field RegionEpoch", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthMetapb
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.RegionEpoch == nil {
				m.RegionEpoch = &RegionEpoch{}
			}
			if err := m.RegionEpoch.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Peers", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthMetapb
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Peers = append(m.Peers, &Peer{})
			if err := m.Peers[len(m.Peers)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 6:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Term", wireType)
			}
			var v uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Term = &v
		default:
			iNdEx = preIndex
			skippy, err := skipMetapb(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMetapb
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Peer) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetapb
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Peer: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Peer: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Id", wireType)
			}
			m.Id = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Id |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field StoreId", wireType)
			}
			m.StoreId = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.StoreId |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMetapb(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMetapb
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipMetapb(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowMetapb
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthMetapb
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowMetapb
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipMetapb(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthMetapb = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowMetapb   = fmt.Errorf("proto: integer overflow")
)

func init() { proto.RegisterFile("metapb.proto", fileDescriptorMetapb) }

var fileDescriptorMetapb = []byte{
	// 445 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x74, 0x52, 0x5d, 0x8b, 0xd3, 0x40,
	0x14, 0x6d, 0xbe, 0xb7, 0x37, 0xd9, 0xa5, 0x8c, 0x8b, 0x86, 0x15, 0xba, 0x4b, 0x9e, 0x96, 0x7d,
	0xa8, 0xe2, 0x83, 0x6f, 0x82, 0xec, 0xe2, 0x83, 0x54, 0x54, 0x52, 0xf5, 0x35, 0xa4, 0xcd, 0xb4,
	0x16, 0x93, 0x4c, 0x98, 0x99, 0x16, 0xfd, 0x27, 0xfa, 0x7f, 0x7c, 0xe8, 0xa3, 0xbf, 0x40, 0x44,
	0xff, 0x88, 0x77, 0x6e, 0x12, 0x69, 0x85, 0x3e, 0x04, 0xe6, 0x9e, 0x73, 0xef, 0xcd, 0x39, 0x67,
	0x06, 0xa2, 0x8a, 0xeb, 0xbc, 0x99, 0x4f, 0x1a, 0x29, 0xb4, 0x60, 0x7e, 0x5b, 0x5d, 0x9c, 0xaf,
	0xc4, 0x4a, 0x10, 0xf4, 0xc8, 0x9c, 0x5a, 0x36, 0x99, 0x42, 0x70, 0x57, 0x6e, 0x94, 0xe6, 0x92,
	0x9d, 0x83, 0xbd, 0x2e, 0x62, 0xeb, 0xca, 0xba, 0x76, 0x6f, 0xdd, 0xdd, 0xcf, 0xcb, 0x41, 0x8a,
	0x35, 0xbb, 0x81, 0xb3, 0x2a, 0xff, 0x9c, 0x35, 0x9c, 0xcb, 0x6c, 0x21, 0x36, 0xb5, 0x8e, 0x6d,
	0xec, 0x38, 0xed, 0x3a, 0x22, 0xe4, 0xde, 0x22, 0x75, 0x67, 0x98, 0xe4, 0x39, 0xc0, 0x4c, 0x0b,
	0xc9, 0x5f, 0xe5, 0x73, 0x5e, 0xb2, 0xfb, 0xe0, 0x7c, 0xe2, 0x5f, 0x68, 0xe1, 0xb0, 0x6b, 0x37,
	0x00, 0xbb, 0x00, 0x6f, 0x9b, 0x97, 0x1b, 0x4e, 0x8b, 0x7a, 0xa6, 0x85, 0x92, 0x6f, 0x16, 0x78,
	0xb4, 0xe2, 0x88, 0x9a, 0x31, 0x04, 0x79, 0x51, 0x48, 0xae, 0xd4, 0xc1, 0x74, 0x0f, 0xb2, 0x09,
	0x78, 0x4a, 0xe7, 0x9a, 0xc7, 0x0e, 0xb2, 0x67, 0x4f, 0xd8, 0xa4, 0x8b, 0x82, 0x76, 0xce, 0x0c,
	0xd3, 0xff, 0x8f, 0xda, 0xd0, 0x9d, 0x5f, 0x1a, 0xb1, 0x2a, 0x76, 0xaf, 0x9c, 0xeb, 0xf0, 0xbf,
	0x01, 0xf2, 0x91, 0x76, 0x1d, 0xc9, 0x6b, 0x08, 0x53, 0xbe, 0x5a, 0x8b, 0xfa, 0x45, 0x23, 0x16,
	0x1f, 0xd9, 0x25, 0x9c, 0x2c, 0x44, 0xbd, 0xcc, 0xb6, 0x5c, 0x1e, 0xc8, 0x0c, 0x0c, 0xfa, 0x01,
	0xf3, 0x44, 0xad, 0xc8, 0x29, 0x1c, 0x20, 0xad, 0xff, 0xf8, 0x0e, 0x4c, 0xbe, 0x5b, 0xe0, 0xb7,
	0x0b, 0x8f, 0x98, 0x7d, 0x08, 0x43, 0x54, 0x29, 0x75, 0x66, 0x62, 0x34, 0x2b, 0xa2, 0xf4, 0x84,
	0x80, 0x29, 0xa6, 0xf8, 0x00, 0x02, 0x5e, 0x17, 0x44, 0x39, 0x44, 0xf9, 0x58, 0x1a, 0xe2, 0x29,
	0x44, 0x92, 0xb6, 0x66, 0xdc, 0xe8, 0x44, 0x63, 0x16, 0x1a, 0xbb, 0xd7, 0x1b, 0xdb, 0xb3, 0x90,
	0x86, 0x72, 0xcf, 0x4f, 0x02, 0x9e, 0xb9, 0x64, 0x15, 0x7b, 0x94, 0x44, 0xd4, 0x0f, 0x98, 0xeb,
	0x4d, 0x5b, 0x8a, 0x31, 0x70, 0xf1, 0xa5, 0x54, 0xb1, 0x6f, 0x94, 0xa6, 0x74, 0x4e, 0x9e, 0x81,
	0x6b, 0x5a, 0x8e, 0x78, 0xc0, 0x94, 0x94, 0x89, 0x32, 0x43, 0xee, 0x20, 0x05, 0x42, 0x5f, 0x16,
	0x37, 0x8f, 0xbb, 0x37, 0x43, 0x97, 0xc3, 0x7c, 0xb0, 0xdf, 0x37, 0xa3, 0x01, 0x0b, 0x21, 0x78,
	0xb3, 0x5c, 0x96, 0xeb, 0x9a, 0x8f, 0x2c, 0x76, 0x0a, 0xc3, 0x77, 0xa2, 0x9a, 0xe3, 0x04, 0x96,
	0xf6, 0xed, 0x68, 0xf7, 0x7b, 0x6c, 0xfd, 0xc0, 0xef, 0x17, 0x7e, 0x5f, 0xff, 0x8c, 0x07, 0x7f,
	0x03, 0x00, 0x00, 0xff, 0xff, 0x7c, 0x61, 0xa1, 0x15, 0xf1, 0x02, 0x00, 0x00,
}
