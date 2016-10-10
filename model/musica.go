package model

type Musica struct {
	IDArtista    string   `json:"id_artista"`
	UniqueID     string   `json:"id_unico_musica"`
	Genero       string   `json:"genero"`
	ID           string   `json:"id_musica"`
	Artista      string   `json:"nome_artista"`
	Nome         string   `json:"nome_musica"`
	URL          string   `json:"url"`
	Popularidade int      `json:"popularidade"`
	Cifra        []string `json:"cifra"`
	SeqFamosas   []string `json:"seq_famosas"`
	Tom          string   `json:"tom"`
	Acordes      []string `json:"acordes"`
}
