package httpapi

import (
	"net/http"
	"time"

	"github.com/rs/zerolog"
)

func AccessLog(log zerolog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rid := GetRequestID(r.Context())

		reqLog := log.With().
			Str("rid", rid).
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Logger()

		reqLog.Info().Msg("request started")

		next.ServeHTTP(w, r)

		reqLog.Info().
			Dur("dur", time.Since(start)).
			Msg("request completed")
	})
}
