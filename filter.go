package tulip

// Matcher encapsulates the logic of matching a query request against
// internal policies and grouping polcies.
type Matcher func(m *Manager, request ...string) bool

// RBACWithDomain matcher encapsulates the matching logic of the following
// casbin model:
//     r = sub, dom, obj, act
//     p = sub, dom, obj, act
//     g = _, _, _
//     e = some(where (p.eft == allow))
//     m = (r.sub == p.sub || g(r.sub, p.sub, r.dom)) && r.dom == p.dom && r.obj == p.obj && r.act == p.act
func RBACWithDomain(m *Manager, request ...string) bool {
	sub, dom, obj, act := request[0], request[1], request[2], request[3]
	if p := m.FindExact(sub, dom, obj, act); p != nil {
		return true
	}
	groups := m.FilterGroups(sub, "", dom)
	policies := m.FilterWithGroups(0, groups, 1)
	if len(policies) == 0 {
		return false
	}
	policies = policies.Filter("", dom, obj, act)
	return len(policies) > 0
}

// FindExact finds the policy that match this rule exactly
func (m *Manager) FindExact(rule ...string) []string {
	return m.p.Find(rule)
}

// Filter filters policies
func (m *Manager) Filter(rule ...string) Policies {
	return m.p.Filter(rule...)
}

// Filter filters grouping policies
func (m *Manager) FilterGroups(rule ...string) Policies {
	return m.g.Filter(rule...)
}

func (m *Manager) FilterWithGroups(policyValueIndex int, groups Policies, groupValueIndex int) Policies {
	if len(groups) == 0 {
		return nil
	}
	var result Policies
	filterSlice := make([]string, policyValueIndex+1)
	for _, g := range groups {
		filterSlice[policyValueIndex] = g[groupValueIndex]
		result = append(result, m.p.Filter(filterSlice...)...)
	}
	return result
}

func (m *Manager) Enforce(request ...string) bool {
	return m.matcher(m, request...)
}
