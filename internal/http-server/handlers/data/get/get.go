package get

import (
	"kvdb/core/lsm"
	"log/slog"
	"net/http"

	res "kvdb/internal/lib/api/response"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/render"
)

type Response struct {
	res.Response
	Value string `json:"value"`
}

func New(log *slog.Logger, lsm *lsm.LSMTree) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const op = "handlers.data.get.New"

		log = log.With(
			slog.String("op", op),
			slog.String("request_id", middleware.GetReqID(r.Context())),
		)

		key := r.URL.Query().Get("key")

		value, exists := lsm.Get(key)
		if !exists {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		render.JSON(w, r, Response{
			Response: res.OK(),
			Value:    value,
		})
	}
}
