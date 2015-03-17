package datastore

import (
	"encoding/binary"
	"io"
)

const encodingVersion uint32 = 1

type ipDataStoreEncoder struct {
	w io.Writer
}

func newEncoder(w io.Writer) *ipDataStoreEncoder {
	return &ipDataStoreEncoder{w: w}
}

func (enc *ipDataStoreEncoder) encode(store *IPDataStore) error {
	store.RLock()
	defer store.RUnlock()

	var buf [43]byte

	// write encoding version
	binary.LittleEndian.PutUint32(buf[0:4], uint32(encodingVersion))
	_, err := enc.w.Write(buf[0:4])
	if err != nil {
		return err
	}

	for ip, ipData := range store.m {
		// pack the ipData's individual data into a byte array
		binary.LittleEndian.PutUint32(buf[0:4], uint32(ip))
		binary.LittleEndian.PutUint32(buf[4:8], uint32(ipData.CurImpacts.FiveMin))
		binary.LittleEndian.PutUint32(buf[8:12], uint32(ipData.CurImpacts.Hour))
		binary.LittleEndian.PutUint32(buf[12:16], uint32(ipData.CurImpacts.Day))
		binary.LittleEndian.PutUint32(buf[16:20], uint32(ipData.MaxImpacts.FiveMin))
		binary.LittleEndian.PutUint32(buf[20:24], uint32(ipData.MaxImpacts.Hour))
		binary.LittleEndian.PutUint32(buf[24:28], uint32(ipData.MaxImpacts.Day))
		binary.LittleEndian.PutUint32(buf[28:32], uint32(ipData.StartTimes.FiveMin))
		binary.LittleEndian.PutUint32(buf[32:36], uint32(ipData.StartTimes.Hour))
		binary.LittleEndian.PutUint32(buf[36:40], uint32(ipData.StartTimes.Day))
		binary.LittleEndian.PutUint16(buf[40:42], uint16(ipData.Forgiven))
		buf[42] = ipData.BlackWhite

		// write the buffer
		_, err := enc.w.Write(buf[0:])
		if err != nil {
			return err
		}
	}

	return nil
}
