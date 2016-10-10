package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"

	"gopkg.in/go-redis/cache.v4"
	"gopkg.in/redis.v4"

	"github.com/danielfireman/deciframe-api/db"
	"github.com/danielfireman/deciframe-api/similares"
	"github.com/julienschmidt/httprouter"
	"github.com/newrelic/go-agent"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		log.Fatal("$PORT must be set")
	}
	log.Println("Porta utilizada", port)

	mgoDB, err := db.Mongo(os.Getenv("MONGODB_URI"))
	if err != nil {
		log.Fatalf("Error connecting to DB: %q", err)
	}
	log.Println("MongoDB conectado.")

	redisCache, err := Redis(os.Getenv("REDIS_URL"))
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Redis cache conectado.")

	nrLicence := os.Getenv("NEW_RELIC_LICENSE_KEY")
	if nrLicence == "" {
		log.Fatal("$NEW_RELIC_LICENSE_KEY must be set")
	}
	app, err := newrelic.NewApplication(newrelic.NewConfig("deciframe-api", nrLicence))
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Monitoramento NewRelic configurado com sucesso.")

	router := httprouter.New()
	s := similares.FabricaDeTratadores(mgoDB, redisCache, app)
	router.GET("/similares", s.GetHandler())

	log.Println("Serviço inicializado na porta ", port)
	log.Fatal(http.ListenAndServe(":"+port, router))
}

func Redis(u string) (*cache.Codec, error) {
	if u == "" {
		return nil, fmt.Errorf("$REDIS_URL must be set")
	}
	redisURL, err := url.Parse(u)
	if err != nil || redisURL.User == nil {
		return nil, fmt.Errorf("Ocorreu um erro no parse da URL REDIS ou o usuário não foi determinado. err:'%q'\n", err)
	}
	pwd, ok := redisURL.User.Password()
	if !ok {
		return nil, fmt.Errorf("Não foi possível extrair a senha de REDIS_URL: %s", redisURL)
	}
	return &cache.Codec{
		Redis: redis.NewClient(&redis.Options{
			Addr:     redisURL.Host,
			Password: pwd,
		}),
		Marshal:   json.Marshal,
		Unmarshal: json.Unmarshal,
	}, nil
}
