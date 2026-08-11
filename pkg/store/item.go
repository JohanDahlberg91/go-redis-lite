package store

import "time"

// Item is a single stored value. A zero ExpiresAt means the key never
// expires.
type Item struct {
	Value     interface{}
	ExpiresAt time.Time
}

// HasTTL reports whether the item carries an expiration.
func (i Item) HasTTL() bool { return !i.ExpiresAt.IsZero() }

// ExpiredAt reports whether the item is expired as of now.
func (i Item) ExpiredAt(now time.Time) bool {
	return i.HasTTL() && now.After(i.ExpiresAt)
}
