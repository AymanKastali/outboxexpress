// Package httpapi is the accounts context's HTTP presentation layer: the only
// place that knows what a request looks like, and the only place that turns a
// domain error into a status code.
//
// The directory is presentation/http per the spec's layout; the package is
// httpapi because a package named http that imports net/http is a riddle.
package httpapi

// registerRequest is the wire shape of a registration. It is unexported and
// separate from the command: the API's JSON contract and the use case's input
// change for different reasons, and a shared struct would couple them.
type registerRequest struct {
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
}

type registerResponse struct {
	UserID string `json:"user_id"`
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
