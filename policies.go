package tulip

import (
	"sort"
)

// Policies represents a list of sortable policies
type Policies [][]string

func (p Policies) Len() int {
	return len(p)
}

func (p Policies) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p Policies) Less(i, j int) bool {
	n := len(p[i])
	for k := 0; k < n; k++ {
		if p[i][k] < p[j][k] {
			return true
		} else if p[i][k] > p[j][k] {
			return false
		}
	}
	return false
}

// search returns insert index for rule
func (p Policies) search(rule []string) int {
	return sort.Search(p.Len(), func(i int) bool {
		n := len(rule)
		for k := 0; k < n; k++ {
			if p[i][k] > rule[k] {
				return true
			} else if p[i][k] < rule[k] {
				return false
			}
		}
		return true
	})
}

func stringSliceEqual(a, b []string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i, s := range a {
		if b[i] != s {
			return false
		}
	}
	return true
}

func (p *Policies) Insert(rule []string) {
	i := p.search(rule)
	if i < p.Len() && stringSliceEqual((*p)[i], rule) {
		return
	}
	sl := make([]string, len(rule))
	copy(sl, rule)
	if i >= p.Len() {
		*p = append(*p, sl)
	} else {
		*p = append((*p)[:i+1], (*p)[i:]...)
		(*p)[i] = sl
	}
}

func (p *Policies) Remove(rule []string) {
	i := p.search(rule)
	if i < p.Len() && stringSliceEqual((*p)[i], rule) {
		*p = append((*p)[:i], (*p)[i+1:]...)
	}
}

func (p Policies) Find(rule []string) []string {
	i := p.search(rule)
	if i < p.Len() && stringSliceEqual(p[i][:len(rule)], rule) {
		return p[i]
	}
	return nil
}

func (p Policies) Filter(rule ...string) Policies {
	// narrow down policies using binary search until encountering an empty slot
	var (
		j int
		s string
	)
	for j, s = range rule {
		if s == "" {
			break
		}
		start := sort.Search(p.Len(), func(i int) bool {
			return p[i][j] >= s
		})
		p = p[start:]
		end := sort.Search(p.Len(), func(i int) bool {
			return p[i][j] > s
		})
		p = p[:end]
	}
	if len(p) == 0 {
		return nil
	}

	// brute force the rest
	n := len(p)
	excluded := make([]byte, n)
	for k := j + 1; k < len(rule); k++ {
		s = rule[k]
		if s == "" {
			continue
		}
		for i := 0; i < len(p); i++ {
			if excluded[i] == 0 && p[i][k] != s {
				excluded[i] = 1
				n--
			}
		}
	}
	res := make([][]string, 0, n)
	for i, v := range excluded {
		if v == 0 {
			res = append(res, p[i])
		}
	}

	return res
}
