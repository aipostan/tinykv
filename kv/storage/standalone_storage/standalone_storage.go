package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	eng *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	Path := conf.StoreAddr
	KvPath := path.Join(Path, "kv")
	RaftPath := path.Join(Path, "Raft")
	return &StandAloneStorage{
		&engine_util.Engines{
			Kv:       engine_util.CreateDB(KvPath, false),
			KvPath:   KvPath,
			Raft:     engine_util.CreateDB(RaftPath, true),
			RaftPath: RaftPath,
		},
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.eng.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandAloneStorageReader(s.eng.Kv.NewTransaction(false)), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	var err error = nil
	for _, quest := range batch {
		switch quest.Data.(type) {
		case storage.Put:
			err = engine_util.PutCF(s.eng.Kv, quest.Cf(), quest.Key(), quest.Value())
		case storage.Delete:
			err = engine_util.DeleteCF(s.eng.Kv, quest.Cf(), quest.Key())
		}
		if err != nil {
			return err
		}
	}
	return err
}

type StandAloneStorageReader struct {
	KvTxn *badger.Txn
}

func NewStandAloneStorageReader(KvTxn *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		KvTxn: KvTxn,
	}
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) (ret []byte, err error) {
	ret, err = engine_util.GetCFFromTxn(s.KvTxn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.KvTxn)
}

func (s *StandAloneStorageReader) Close() {
	s.KvTxn.Discard()
}
