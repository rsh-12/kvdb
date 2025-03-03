package scan

import (
	"kvdb/core/lsm"
	"kvdb/core/lsm/pagination"
	res "kvdb/internal/lib/api/response"
	"kvdb/types"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/render"
)

type Response struct {
	res.Response
	Items []types.Item `json:"items"`
}

func New(log *slog.Logger, lsm *lsm.LSMTree) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const op = "handlers.data.scan.New"

		log = log.With(
			slog.String("op", op),
			slog.String("request_id", middleware.GetReqID(r.Context())),
		)

		limitVar := r.URL.Query().Get("limit")
		offsetVar := r.URL.Query().Get("offset")

		limit, err := strconv.Atoi(limitVar)
		if err != nil {
			log.Error("unimplemented")
		}

		offset, err := strconv.Atoi(offsetVar)
		if err != nil {
			log.Error("unimplemented")
		}

		items, err := pagination.Paginate(lsm, pagination.Page{Limit: limit, Offset: offset})
		if err != nil {
			log.Error("unimplemented")
		}

		render.JSON(w, r, Response{
			Response: res.OK(),
			Items:    items,
		})
	}
}
