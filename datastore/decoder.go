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

func (dec *IPDataStoreDecoder) Decode(m *IPDataMap) error {
	// read encoding version
	var buf [43]byte
	var version uint32

	_, err := dec.r.Read(buf[0:4])
	if err != nil {
		return err
	}
	version = binary.LittleEndian.Uint32(buf[0:4])
	if version != encodingVersion {
		return errors.New("Wrong version kdb")
	}

FileLoop:
	for {
		var ip IPLong
		ipData := new(IPData)

		for num := 0; num < 43; {
			n, err := dec.r.Read(buf[0:])
			if err == io.EOF {
				break FileLoop
			} else if err != nil {
				return err
			}
			num += n
		}

		// unpack buf into ip and the fields of IPData
		ip = IPLong(binary.LittleEndian.Uint32(buf[0:4]))
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
		buf[42] = ipData.BlackWhite

		(*m)[ip] = ipData
	}

	return nil
}
