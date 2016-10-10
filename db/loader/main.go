package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/danielfireman/deciframe-api/db"
	sets "github.com/deckarep/golang-set"

	"gopkg.in/mgo.v2"
)

const (
	ARTISTA_ID   = 0
	MUSICA_ID    = 1
	ARTISTA      = 2
	MUSICA       = 3
	GENERO       = 4
	POPULARIDADE = 5
	TOM          = 6
	SEQ_FAMOSA   = 7
	CIFRA        = 8
)

func main() {
	mongoDB, err := db.Mongo(os.Getenv("MONGODB_URI"))
	if err != nil {
		log.Fatalf("Ocorreu um erro no parse da MONGODB_URI. err:'%q'\n", err)
	}
	defer mongoDB.Close()

	scanner := bufio.NewScanner(os.Stdin)
	var musicas []interface{}
	for scanner.Scan() {
		// Pré-processando cada linha.
		linha := scanner.Text()
		linha = strings.Replace(linha, "\"", "", -1)
		linha = strings.Replace(linha, "NA", "", -1)
		dados := strings.Split(linha, ",")
		m := &db.M{
			IDUnicoMusica: db.IDUnicoMusica(dados[ARTISTA_ID], dados[MUSICA_ID]),
			Artista:       dados[ARTISTA],
			IDArtista:     dados[ARTISTA_ID],
			ID:            dados[MUSICA_ID],
			Nome:          dados[MUSICA],
			Genero:        dados[GENERO],
			Tom:           dados[TOM],
		}

		m.Popularidade, err = strconv.Atoi(strings.Replace(dados[POPULARIDADE], ".", "", -1))
		if err != nil {
			log.Fatal(err)
		}

		seqFTrim := strings.Trim(dados[SEQ_FAMOSA], " ")
		if seqFTrim != "" && seqFTrim != "NA" {
			m.SeqFamosas = strings.Split(seqFTrim, ";")
		}

		// Tratando acordes como um campo obrigatório. Não adicionando se não tiver acordes.
		a := acordes(dados[CIFRA])
		if len(a) > 0 {
			m.Acordes = a
			musicas = append(musicas, m)
		}
	}

	fmt.Printf("Inserindo %d músicas. \n", len(musicas))
	c := mongoDB.GetColecaoMusicas()
	if err := c.EnsureIndex(mgo.Index{
		Key:        []string{"genero"},
		Unique:     false,
		DropDups:   false,
		Background: false,
		Sparse:     true,
	}); err != nil {
		log.Fatalf("Erro criando índice de gêneros: %q", err)
	}
	fmt.Println("Índice de gêneros criado com sucesso.")
	if err = c.EnsureIndex(mgo.Index{
		Key:        []string{"acordes"},
		Unique:     false,
		DropDups:   false,
		Background: false,
		Sparse:     true,
	}); err != nil {
		log.Fatalf("Erro criando índice de acordes: %q", err)
	}
	fmt.Println("Índice de acordes criado com sucesso.")
	if err = c.EnsureIndex(mgo.Index{
		Key:        []string{"id_unico_musica"},
		Unique:     true,
		DropDups:   true,
		Background: false,
		Sparse:     true,
	}); err != nil {
		log.Fatalf("Erro criando índice de id_unico_musica: %q", err)
	}
	fmt.Println("Índice de id_unico_musica criado com sucesso.")
	if err = c.EnsureIndex(mgo.Index{
		Key:        []string{"seq_famosas"},
		Unique:     false,
		DropDups:   false,
		Background: false,
		Sparse:     true,
	}); err != nil {
		log.Fatalf("Erro criando índice de seq_famosas: %q", err)
	}
	fmt.Println("Índice de seq_famosas criado com sucesso.")
	if err := c.Insert(musicas...); err != nil {
		log.Fatalf("Erro inserindo músicas: %q", err)
	}
	fmt.Printf("%d músicas inseridas com sucesso.\n", len(musicas))
}

func acordes(strCifra string) []string {
	acordes := sets.NewSet()
	cifra := limpaCifra(strCifra)
	for _, c := range cifra {
		acordes.Add(c)
	}
	var result []string
	for c := range acordes.Iter() {
		result = append(result, c.(string))
	}
	return result
}

func limpaCifra(strCifra string) []string {
	if strCifra == "" {
		return []string{}
	}
	var cifra []string
	rawCifra := strings.Split(strCifra, ";")
	for _, m := range rawCifra {
		m = strings.Trim(m, " ")
		if len(m) != 0 {
			if strings.Contains(m, "|") {
				// filtra tablaturas
				acorde := strings.Split(m, "|")[0]
				acorde = pythonSplit(acorde)[0]
				cifra = append(cifra, acorde)
			} else {
				// lida com acordes separados por espaço
				cifra = append(cifra, pythonSplit(m)...)
			}
		}
	}
	return cifra
}

// Mais perto que consegui da função split() em python.
// A idéia é converter múltiplos espaços consecutivos em um espaço e então fazer split.
var multiplosEspacos = regexp.MustCompile(" +")

func pythonSplit(s string) []string {
	return strings.Split(multiplosEspacos.ReplaceAllString(s, " "), " ")
}
