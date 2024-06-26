package desc_source

import (
	"fmt"
	"github.com/jhump/protoreflect/grpcreflect"
)

type notFoundError string

func notFound(kind, name string) error {
	return notFoundError(fmt.Sprintf("%s not found: %s", kind, name))
}

func (e notFoundError) Error() string {
	return string(e)
}

func IsNotFoundError(err error) bool {
	if grpcreflect.IsElementNotFoundError(err) {
		return true
	}

	_, ok := err.(notFoundError)
	return ok
}
