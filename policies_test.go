package tulip

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPolicies(t *testing.T) {
	p := Policies([][]string{
		{"c", "e"},
		{"c", "u"},
		{"b", "o"},
		{"a", "f"},
		{"b", "o"},
		{"a", "d"},
	})
	sort.Sort(p)
	assert.Equal(t, Policies([][]string{
		{"a", "d"},
		{"a", "f"},
		{"b", "o"},
		{"b", "o"},
		{"c", "e"},
		{"c", "u"},
	}), p)

	assert.Nil(t, p.Find([]string{"a", "e"}))
	assert.NotNil(t, p.Find([]string{"c", "e"}))

	p.Remove([]string{"b", "o"})
	assert.Equal(t, Policies([][]string{
		{"a", "d"},
		{"a", "f"},
		{"b", "o"},
		{"c", "e"},
		{"c", "u"},
	}), p)

	p.Insert([]string{"a", "f"})
	p.Insert([]string{"a", "e"})
	p.Insert([]string{"c", "y"})
	assert.Equal(t, Policies([][]string{
		{"a", "d"},
		{"a", "e"},
		{"a", "f"},
		{"b", "o"},
		{"c", "e"},
		{"c", "u"},
		{"c", "y"},
	}), p)

	assert.Equal(t, Policies([][]string{
		{"a", "d"},
		{"a", "e"},
		{"a", "f"},
	}), p.Filter("a"))
	assert.Equal(t, Policies([][]string{
		{"a", "e"},
		{"c", "e"},
	}), p.Filter("", "e"))
	assert.Len(t, p.Filter("e"), 0)

	p = Policies([][]string{
		{"b", "h", "i"},
		{"a", "d", "f"},
		{"b", "e", "g"},
		{"a", "f", "g"},
		{"b", "n", "j"},
		{"a", "b", "c"},
	})
	sort.Sort(p)
	assert.Equal(t, Policies([][]string{
		{"a", "b", "c"},
		{"a", "d", "f"},
		{"a", "f", "g"},
		{"b", "e", "g"},
		{"b", "h", "i"},
		{"b", "n", "j"},
	}), p)

	assert.Equal(t, Policies([][]string{
		{"a", "b", "c"},
	}), p.Filter("a", "b"))
	assert.Equal(t, Policies([][]string{
		{"a", "d", "f"},
	}), p.Filter("a", "", "f"))
	assert.Equal(t, Policies([][]string{
		{"a", "f", "g"},
		{"b", "e", "g"},
	}), p.Filter("", "", "g"))
	assert.Equal(t, Policies([][]string{
		{"b", "n", "j"},
	}), p.Filter("", "n", "j"))
}
