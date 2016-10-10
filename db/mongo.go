package db

import (
	"fmt"
	"net/url"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/danielfireman/deciframe-api/model"
)

const (
	TabelaMusicas = "musicas"
)

type M struct {
	IDUnicoMusica string   `bson:"id_unico_musica"`
	IDArtista     string   `bson:"id_artista"`
	ID            string   `bson:"id_musica"`
	Genero        string   `bson:"genero"`
	Artista       string   `bson:"nome_artista"`
	Nome          string   `bson:"nome_musica"`
	Acordes       []string `bson:"acordes"`
	Tom           string   `bson:"tom"`
	SeqFamosas    []string `bson:"seq_famosas,omitempty"`
	Popularidade  int      `bson:"popularidade"`
}

func (m *M) URL() string {
	return fmt.Sprintf("http://www.cifraclub.com.br/%s/%s", m.IDArtista, m.ID)
}

func IDUnicoMusica(artista, id string) string {
	return fmt.Sprintf("%s_%s", artista, id)
}
func NaoEncontrado(err error) bool {
	return mgo.ErrNotFound == err
}

type DB struct {
	session *mgo.Session
	name    string
}

func (db *DB) BuscaMusicaPorIDUnico(idUnicoMusica string) (*model.Musica, error) {
	session := db.session.Copy()
	defer session.Close()
	c := session.DB(db.name).C(TabelaMusicas)

	m := M{}
	if err := c.Find(bson.M{"id_unico_musica": idUnicoMusica}).One(&m); err != nil {
		return nil, err
	}

	return &model.Musica{
		IDArtista:  m.IDArtista,
		UniqueID:   m.IDUnicoMusica,
		Genero:     m.Genero,
		ID:         m.ID,
		Artista:    m.Artista,
		Nome:       m.Nome,
		URL:        m.URL(),
		SeqFamosas: m.SeqFamosas,
		Tom:        m.Tom,
		Acordes:    m.Acordes,
	}, nil
}

func (db *DB) BuscaMusicasPorAcordes(acordes, generos []string) ([]*model.Musica, error) {
	session := db.session.Copy()
	defer session.Close()
	c := session.DB(db.name).C(TabelaMusicas)
	if len(generos) == 0 {
		return db.executaConsulta(c.Find(bson.M{"acordes": bson.M{"$in": acordes}}).Hint("acordes"))
	}
	return db.executaConsulta(
		c.Find(bson.M{
			"acordes": bson.M{"$in": acordes},
			"genero":  bson.M{"$in": generos},
		}).Hint("acordes").Hint("genero"))
}

func (db *DB) BuscaMusicasPorSeqFamosa(seqFamosas, generos []string) ([]*model.Musica, error) {
	session := db.session.Copy()
	defer session.Close()
	c := session.DB(db.name).C(TabelaMusicas)
	if len(generos) == 0 {
		return db.executaConsulta(
			c.Find(bson.M{"seq_famosas": bson.M{"$in": seqFamosas}}).Sort("-popularidade").Hint("seq_famosas"))
	}
	return db.executaConsulta(
		c.Find(bson.M{
			"seq_famosas": bson.M{"$in": seqFamosas},
			"genero":      bson.M{"$in": generos},
		}).Sort("-popularidade").Hint("seq_famosas").Hint("genero"))
}

func (db *DB) executaConsulta(q *mgo.Query) ([]*model.Musica, error) {
	iter := q.Iter()
	defer iter.Close()

	var res []*model.Musica
	for !iter.Done() {
		m := &M{}
		if !iter.Next(m) {
			if iter.Err() != mgo.ErrNotFound {
				return nil, iter.Err()
			}
		}
		res = append(res, &model.Musica{
			IDArtista:    m.IDArtista,
			UniqueID:     m.IDUnicoMusica,
			Genero:       m.Genero,
			ID:           m.ID,
			Artista:      m.Artista,
			Nome:         m.Nome,
			URL:          m.URL(),
			SeqFamosas:   m.SeqFamosas,
			Tom:          m.Tom,
			Acordes:      m.Acordes,
			Popularidade: m.Popularidade,
		})
	}
	return res, nil
}

func (db *DB) GetColecaoMusicas() *mgo.Collection {
	return db.session.DB(db.name).C(TabelaMusicas)
}

func (db *DB) Close() {
	db.session.Close()
}

func Mongo(uri string) (*DB, error) {
	mgoURL, err := url.Parse(uri)
	if uri == "" || err != nil {
		return nil, fmt.Errorf("Ocorreu um erro no parse da mongo url:%s err:%q\n", uri, err)
	}
	s, err := mgo.Dial(mgoURL.String())
	if err != nil {
		return nil, err
	}
	s.SetMode(mgo.Eventual, true)
	return &DB{
		session: s,
		name:    mgoURL.EscapedPath()[1:], // Removendo barra inicial do path.
	}, nil
}
