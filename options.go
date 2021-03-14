package octobe

// suppressError
type suppressError struct {
	err error
}

// Type follows the Option interface
func (s suppressError) Type() string {
	return "suppressError"
}

// SuppressError will tell the library to suppress the error if it occurs in this lib or sql libs
func SuppressError(err error) Option {
	return suppressError{err: err}
}

// structureOptions helper function that convert slice of Option into an option struct
func convertOptions(opts ...Option) (opt option) {
	for _, option := range opts {
		switch option.Type() {
		case "suppressError":
			err, _ := option.(suppressError)
			opt.suppressErrs = append(opt.suppressErrs, err.err)
			break
		}
	}
	return
}
