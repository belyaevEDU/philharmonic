package auth

import (
	"context"
	"log"
	"net/http"
	"strings"

	"github.com/belyaevedu/philharmonic/handlers"
)

type contextKey struct{}

func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, contextKey{}, id)
}

func IdentityFromContext(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(contextKey{}).(Identity)
	return id, ok
}

// middleware that authenticates requests via the
// "Authorization: Bearer <token>" header against the given TokenStore.
// invalid or missing credentials produce a 401 Unauthorized
func BearerAuth(store *TokenStore) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id, ok := authenticate(r, store)
			if !ok {
				w.Header().Set("WWW-Authenticate", "Bearer")
				err := handlers.HttpResponseHelper(w, "unauthorized: missing or invalid bearer token", http.StatusUnauthorized)
				if err != nil {
					log.Printf("Error raised in BearerAuth middleware: %s\n", err)
				}
				return
			}
			next.ServeHTTP(w, r.WithContext(WithIdentity(r.Context(), id)))
		})
	}
}

func authenticate(r *http.Request, store *TokenStore) (Identity, bool) {
	header := r.Header.Get("Authorization")
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return Identity{}, false
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return Identity{}, false
	}
	return store.Verify(token)
}

// middleware enforcing a minimum role.
// must run after BearerAuth:
// requests without an authenticated identity are rejected with 401 Unauthorized,
// authenticated but insufficient roles get 403 Forbidden.
func RequireRole(min Role) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if roleDenied(w, r, min) {
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// per-route form of RequireRole for routers that attach authorization
// to individual handler
//
// (chi cannot mount the same Route path inside two middleware-scoped groups,
// so the manager API wraps each handler instead :/)
func RequireRoleHandler(min Role, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if roleDenied(w, r, min) {
			return
		}
		h(w, r)
	}
}

// roleDenied writes a 401 Unauthorized/403 Forbidden response and reports
// whether the request must be rejected
func roleDenied(w http.ResponseWriter, r *http.Request, min Role) bool {
	id, ok := IdentityFromContext(r.Context())
	if !ok {
		w.Header().Set("WWW-Authenticate", "Bearer")
		_ = handlers.HttpResponseHelper(w, "unauthorized: authentication required", http.StatusUnauthorized)
		return true
	}
	if !id.Role.Allows(min) {
		msg := "forbidden: role " + string(id.Role) + " does not allow this operation (requires " + string(min) + ")"
		_ = handlers.HttpResponseHelper(w, msg, http.StatusForbidden)
		return true
	}
	return false
}
