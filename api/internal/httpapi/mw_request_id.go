package httpapi

import (
	"context"
	"crypto/rand"
	"net/http"
)

type ctxKey int

const requestIDKey ctxKey = 1

var alphabet = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func newReqID8() string {
	b := make([]byte, 8)
	rnd := make([]byte, 8)
	_, _ = rand.Read(rnd)
	for i := 0; i < 8; i++ {
		b[i] = alphabet[int(rnd[i])%len(alphabet)]
	}
	return string(b)
}

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rid := r.Header.Get("X-Request-ID")
		if rid == "" || len(rid) != 8 {
			rid = newReqID8()
		}
		w.Header().Set("X-Request-ID", rid)
		ctx := context.WithValue(r.Context(), requestIDKey, rid)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func GetRequestID(ctx context.Context) string {
	if v := ctx.Value(requestIDKey); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
