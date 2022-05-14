package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

var _ storage.StorageReader = new(badgerReader)

type badgerReader struct {
	db  *badger.DB
	txn *badger.Txn
	itr *engine_util.BadgerIterator
}

func NewBadgerReader(db *badger.DB) *badgerReader {
	return &badgerReader{
		db: db,
	}
}

func (r *badgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCF(r.db, cf, key)
}

func (r *badgerReader) IterCF(cf string) engine_util.DBIterator {
	r.txn = r.db.NewTransaction(false)
	r.itr = engine_util.NewCFIterator(cf, r.txn)
	return r.itr
}

func (r *badgerReader) Close() {
	r.itr.Close()
	r.txn.Discard()
}
