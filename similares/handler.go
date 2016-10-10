package similares

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/go-redis/cache.v4"

	"github.com/danielfireman/deciframe-api/db"
	sets "github.com/deckarep/golang-set"
	"github.com/julienschmidt/httprouter"
	"github.com/newrelic/go-agent"
)

type SimilaresResponse struct {
	UniqueID     string        `json:"id_unico_musica"`
	IDArtista    string        `json:"id_artista"`
	ID           string        `json:"id_musica"`
	Artista      string        `json:"nome_artista"`
	Nome         string        `json:"nome_musica"`
	Popularidade int           `json:"popularidade"`
	Acordes      []string      `json:"acordes"`
	Genero       string        `json:"genero"`
	URL          string        `json:"url"`
	Diferenca    []interface{} `json:"diferenca"`
	Intersecao   []interface{} `json:"intersecao"`
}

// PorMenorDiferenca implementa sort.Interface for []*Musica baseado no campo Diferenca
type PorMenorDiferenca []*SimilaresResponse

func (p PorMenorDiferenca) Len() int {
	return len(p)
}
func (p PorMenorDiferenca) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func (p PorMenorDiferenca) Less(i, j int) bool {
	return len(p[i].Diferenca) < len(p[j].Diferenca)
}

const (
	NUM_ACESSOS_CONCORRENTES = 5
	TAM_PAGINA               = 100
)

type HandlerFactory struct {
	mon   newrelic.Application
	fila  chan struct{}
	db    *db.DB
	cache *cache.Codec
}

func FabricaDeTratadores(db *db.DB, cache *cache.Codec, mon newrelic.Application) *HandlerFactory {
	return &HandlerFactory{
		mon:  mon,
		db:   db,
		fila: make(chan struct{}, NUM_ACESSOS_CONCORRENTES),
		cache: cache,
	}
}

func (s *HandlerFactory) GetHandler() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		txn := s.mon.StartTransaction("similares", w, r)
		defer txn.End()

		// Controlando acesso concorrente;
		filaSeg := newrelic.StartSegment(txn, "fila")
		s.fila <- struct{}{}
		defer func() {
			<-s.fila
		}()
		filaSeg.End()

		pagina, err := paginaRequisitada(r)
		if err != nil {
			txn.WriteHeader(http.StatusBadRequest)
			return
		}

		/*response := s.buscaNoCache(r.URL.RawQuery, txn)
		if len(response) > 0 {
			b, err := marshal(response, pagina, txn)
			if err != nil {
				log.Printf("Erro processando request [%s]: '%q'", r.URL.String(), err)
				txn.WriteHeader(http.StatusInternalServerError)
				return
			}
			txn.Header().Add("Access-Control-Allow-Origin", "*")
			fmt.Fprintf(txn, b)
			return
		}*/

		var response []*SimilaresResponse
		var acordes []string
		queryValues := r.URL.Query()
		switch {
		case queryValues.Get("acordes") != "":
			acordes = strings.Split(queryValues.Get("acordes"), ",")
		case queryValues.Get("id_unico_musica") != "":
			buscaIDUnicoSeg := newrelic.StartSegment(txn, "busca_id_unico")
			m, err := s.db.BuscaMusicaPorIDUnico(queryValues.Get("id_unico_musica"))
			buscaIDUnicoSeg.End()
			if err != nil {
				if db.NaoEncontrado(err) {
					txn.WriteHeader(http.StatusBadRequest)
					return
				}
				txn.WriteHeader(http.StatusInternalServerError)
				return
			}
			for _, a := range m.Acordes {
				acordes = append(acordes, a)
			}
		}
		buscaSimilares := newrelic.StartSegment(txn, "busca_similares")
		musicasSimilares, err := s.db.BuscaMusicas(acordes, generosRequisitados(r))
		buscaSimilares.End()

		acordesSet := sets.NewThreadUnsafeSet()
		for _, a := range acordes {
			acordesSet.Add(a)
		}
		for _, m := range musicasSimilares {
			mAcordesSet := sets.NewThreadUnsafeSet()
			for _, a := range m.Acordes {
				mAcordesSet.Add(a)
			}
			if mAcordesSet.Cardinality() > 1 && queryValues.Get("id_unico_musica") != m.UniqueID {
				response = append(response, &SimilaresResponse{
					UniqueID:     m.UniqueID,
					IDArtista:    m.IDArtista,
					ID:           m.ID,
					Artista:      m.Artista,
					Nome:         m.Nome,
					Popularidade: m.Popularidade,
					Acordes:      m.Acordes,
					Genero:       m.Genero,
					URL:          m.URL,
					Diferenca:    mAcordesSet.Difference(acordesSet).ToSlice(),
					Intersecao:   mAcordesSet.Intersect(acordesSet).ToSlice(),
				})
			}
		}
		b, err := s.toBytes(r.URL.RawQuery, response, pagina)
		if err != nil {
			log.Printf("Erro processando request [%s]: '%q'", r.URL.String(), err)
			txn.WriteHeader(http.StatusInternalServerError)
			return
		}
		txn.Header().Add("Access-Control-Allow-Origin", "*")
		fmt.Fprintf(txn, string(b))
	}
}

func (s *HandlerFactory) buscaNoCache(query string, txn newrelic.Transaction) []*SimilaresResponse {
	defer newrelic.StartSegment(txn, "busca_cache").End()
	var response []*SimilaresResponse
	if err := s.cache.Get(query, &response); err != nil && err != cache.ErrCacheMiss {
		log.Printf("Erro buscando no cache: %q", err)
		return []*SimilaresResponse{} // Garantir que mandamos uma lista vazia.
	}
	return response
}

func marshal(response []*SimilaresResponse, pagina int, txn newrelic.Transaction) (string, error) {
	defer newrelic.StartSegment(txn, "marshal").End()

	// Para retornar, primeiro ordenamos
	sort.Sort(PorMenorDiferenca(response))

	// Consideramos os limites da página.
	i, f := limitesDaPagina(len(response), pagina)

	// Convertemos para JSON.
	b, err := json.Marshal(response[i:f])
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (s *HandlerFactory) toBytes(cacheKey string, response []*SimilaresResponse, pagina int) ([]byte, error) {
	// Para retornar, primeiro ordenamos
	sort.Sort(PorMenorDiferenca(response))

	// Consideramos os limites da página.
	i, f := limitesDaPagina(len(response), pagina)

	if len(response) > 0 {
		// Colocamos no cache.
		s.cache.Set(&cache.Item{
			Key:        cacheKey,
			Object:     response[i:f],
			Expiration: 6 * time.Hour,
		})
	}

	// Convertemos para JSON.
	b, err := json.Marshal(response[i:f])
	if err != nil {
		return nil, err
	}
	return b, nil
}

func limitesDaPagina(size int, pagina int) (int, int) {
	i := (pagina - 1) * TAM_PAGINA
	return i, int(math.Min(float64(i+TAM_PAGINA), float64(size)))
}

func paginaRequisitada(r *http.Request) (int, error) {
	pagina := 1
	if r.URL.Query().Get("pagina") != "" {
		p, err := strconv.Atoi(r.URL.Query().Get("pagina"))
		if err != nil {
			return -1, err
		}
		pagina = p
	}
	return pagina, nil
}

// retorna generos do request (podem ser separados por vírgula).
func generosRequisitados(r *http.Request) []string {
	var res []string
	if r.URL.Query().Get("generos") != "" {
		for _, g := range strings.Split(r.URL.Query().Get("generos"), ",") {
			res = append(res, g)
		}
	}
	return res
}
