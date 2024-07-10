package utils

import (
	"fmt"
	"maps"
	"slices"
)

type Set[T comparable] map[T]struct{}

func NewSet[T comparable](elements ...T) Set[T] {
	s := make(Set[T], len(elements))
	for _, t := range elements {
		s.Add(t)
	}
	return s
}

func (s Set[T]) Add(t T) bool {
	_, ok := s[t]
	if ok {
		return false
	}
	s[t] = struct{}{}
	return true
}

func (s Set[T]) Contains(t T) bool {
	_, ok := s[t]
	return ok
}

func (s Set[T]) Remove(t T) bool {
	_, ok := s[t]
	if !ok {
		return false
	}
	delete(s, t)
	return true
}

func (s Set[T]) String() string {
	return fmt.Sprint(slices.Collect(maps.Keys(s)))
}
