package main

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/segmentio/kafka-go"

	"datavant-style-exchange/services/ingest-api-go/internal/config"
	"datavant-style-exchange/services/ingest-api-go/internal/httpapi"
)

const (
	readTimeout  = 10 * time.Second
	writeTimeout = 15 * time.Second
	idleTimeout  = 30 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	db, err := sql.Open("pgx", cfg.PostgresDSN)
	if err != nil {
		logger.Error("failed to open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	dbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := db.PingContext(dbCtx); err != nil {
		cancel()
		logger.Error("failed to connect database", "error", err)
		os.Exit(1)
	}
	cancel()

	minioClient, err := minio.New(cfg.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
		Secure: cfg.MinioSecure,
	})
	if err != nil {
		logger.Error("failed to init minio client", "error", err)
		os.Exit(1)
	}

	kafkaWriter := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers...),
		Topic:    cfg.TopicRecordIngestedV1,
		Balancer: &kafka.LeastBytes{},
	}
	defer func() {
		_ = kafkaWriter.Close()
	}()

	handler := httpapi.New(cfg, logger, minioClient, kafkaWriter, db)
	router := chi.NewRouter()

	router.Get("/healthz", handler.Healthz)
	router.Route("/v1", func(r chi.Router) {
		r.Use(httpapi.AuthMiddleware(cfg))
		r.Post("/ingest", handler.IngestV1)
	})

	srv := &http.Server{
		Addr:         cfg.Addr,
		Handler:      router,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		logger.Info("ingest api listening", "addr", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
}
