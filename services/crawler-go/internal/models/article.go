package models //

import "time"

type Article struct {
	URL         string    `json:"url"`
	Title       string    `json:"title"`
	Content     string    `json:"content"`
	Source      string    `json:"source"`
	PublishedAt time.Time `json:"published_at"`
}
