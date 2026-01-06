// Export internal functions for testing
package store

// EncodeBlockRecordsAtLevel exports encodeBlockRecordsAtLevel for testing (V11 striped format)
func EncodeBlockRecordsAtLevel(records []BlockRecord, level int) []byte {
	return encodeBlockRecordsAtLevel(records, level)
}

// EncodeBlockRecordsAtLevelV10 exports encodeBlockRecordsAtLevelV10 for testing
func EncodeBlockRecordsAtLevelV10(records []BlockRecord, level int) []byte {
	return encodeBlockRecordsAtLevelV10(records, level)
}

// DecodeBlockRecordsAtLevel exports decodeBlockRecordsAtLevel for testing
func DecodeBlockRecordsAtLevel(data []byte, count int, level int, version uint8) ([]BlockRecord, error) {
	return decodeBlockRecordsAtLevel(data, count, level, version)
}

// DecodeBlockRecordsV10 exports decodeBlockRecordsV10 for testing
func DecodeBlockRecordsV10(data []byte, count int, level int) ([]BlockRecord, error) {
	return decodeBlockRecordsV10(data, count, level)
}

// DecodeBlockRecordsV11 exports decodeBlockRecordsV11 for testing
func DecodeBlockRecordsV11(data []byte, count int, level int) ([]BlockRecord, error) {
	return decodeBlockRecordsV11(data, count, level)
}

// EncodeBlockHeader exports encodeBlockHeader for testing
func EncodeBlockHeader(h BlockHeader) []byte {
	return encodeBlockHeader(h)
}

// DecodeBlockHeader exports decodeBlockHeader for testing
func DecodeBlockHeader(data []byte) (BlockHeader, error) {
	return decodeBlockHeader(data)
}

// RedistributeToChildren exports redistributeToChildren for testing
func (ps *PositionStore) RedistributeToChildren(records []BlockRecord, level int, oldPath string) (int64, error) {
	return ps.redistributeToChildren(records, level, oldPath)
}

// MergeBlockRecords exports mergeBlockRecords for testing
func MergeBlockRecords(existing, newer []BlockRecord, keySize int) []BlockRecord {
	return mergeBlockRecords(existing, newer, keySize)
}
