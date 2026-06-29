package series

import "errors"

var errNoField = errors.New("series: key has no field separator")

// ErrNoField is exported for callers that want to detect and skip fieldless keys.
var ErrNoField = errNoField
