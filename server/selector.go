package server

import (
	_ "math/rand"
)

// Selector is an interface to select source and target store to schedule.
type Selector interface {
	SelectSource(stores []*storeInfo, filters ...Filter) *storeInfo
	SelectTarget(stores []*storeInfo, filters ...Filter) *storeInfo
}

type balanceSelector struct {
	kind    ResourceKind
	filters []Filter
}

func newBalanceSelector(kind ResourceKind, filters []Filter) *balanceSelector {
	return &balanceSelector{
		kind:    kind,
		filters: filters,
	}
}

func (s *balanceSelector) SelectSource(stores []*storeInfo, filters ...Filter) *storeInfo {
	filters = append(filters, s.filters...)

	var result *storeInfo
	for _, store := range stores {
		if filterSource(store, filters) {
			continue
		}
		if result == nil || result.resourceScore(s.kind) < store.resourceScore(s.kind) {
			result = store
		}
	}
	return result
}

func (s *balanceSelector) SelectTarget(stores []*storeInfo, filters ...Filter) *storeInfo {
	filters = append(filters, s.filters...)

	var result *storeInfo
	for _, store := range stores {
		if filterTarget(store, filters) {
			continue
		}
		if result == nil || result.resourceScore(s.kind) > store.resourceScore(s.kind) {
			result = store
		}
	}
	return result
}
