package datastore

import (
	"encoding/binary"
	"errors"
	"io"
)

type IPDataStoreDecoder struct {
	r io.Reader
}

func NewDecoder(r io.Reader) *IPDataStoreDecoder {
	return &IPDataStoreDecoder{r: r}
}

func (dec *IPDataStoreDecoder) DecodeEvery(fn func(IPLong, *IPData)) error {
	// read encoding version
	var buf [43]byte
	var version uint32

	_, err := io.ReadFull(dec.r, buf[0:4])
	if err != nil {
		return err
	}
	version = binary.LittleEndian.Uint32(buf[0:4])
	if version != encodingVersion {
		return errors.New("Wrong version kdb")
	}

FileLoop:
	for {
		_, err := io.ReadFull(dec.r, buf[0:])
		if err == io.EOF {
			break FileLoop
		} else if err != nil {
			return err
		}

		// unpack buf into ip and the fields of IPData
		ipData := &IPData{}
		ip := IPLong(binary.LittleEndian.Uint32(buf[0:4]))
		ipData.CurImpacts.FiveMin = ImpactAmount(binary.LittleEndian.Uint32(buf[4:8]))
		ipData.CurImpacts.Hour = ImpactAmount(binary.LittleEndian.Uint32(buf[8:12]))
		ipData.CurImpacts.Day = ImpactAmount(binary.LittleEndian.Uint32(buf[12:16]))
		ipData.MaxImpacts.FiveMin = ImpactAmount(binary.LittleEndian.Uint32(buf[16:20]))
		ipData.MaxImpacts.Hour = ImpactAmount(binary.LittleEndian.Uint32(buf[20:24]))
		ipData.MaxImpacts.Day = ImpactAmount(binary.LittleEndian.Uint32(buf[24:28]))
		ipData.StartTimes.FiveMin = binary.LittleEndian.Uint32(buf[28:32])
		ipData.StartTimes.Hour = binary.LittleEndian.Uint32(buf[32:36])
		ipData.StartTimes.Day = binary.LittleEndian.Uint32(buf[36:40])
		ipData.Forgiven = ForgivenNum(binary.LittleEndian.Uint16(buf[40:42]))
		ipData.BlackWhite = buf[42]

		fn(ip, ipData)
	}

	return nil
}

func (dec *IPDataStoreDecoder) Decode(m *IPDataMap) error {
	return dec.DecodeEvery(func(ip IPLong, ipData *IPData) {
		(*m)[ip] = ipData
	})
}
