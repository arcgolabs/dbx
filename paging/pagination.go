package paging

import "github.com/DaiYuANg/arcgo/collectionx"

const (
	// DefaultPage is used when a page request has no valid page number.
	DefaultPage = 1
	// DefaultPageSize is used when a page request has no valid page size.
	DefaultPageSize = 20
)

// Request is the shared offset-pagination request model used by query and repository APIs.
type Request struct {
	Page        int
	PageSize    int
	MaxPageSize int
}

// Page creates a normalized page request.
func Page(page, pageSize int) Request {
	return NewRequest(page, pageSize)
}

// NewRequest creates a normalized page request.
func NewRequest(page, pageSize int) Request {
	return Request{Page: page, PageSize: pageSize}.Normalize()
}

// WithMaxPageSize applies an upper bound to the page size.
func (r Request) WithMaxPageSize(maxPageSize int) Request {
	r.MaxPageSize = maxPageSize
	return r.Normalize()
}

// Normalize returns a request with valid page and page size values.
func (r Request) Normalize() Request {
	if r.Page < 1 {
		r.Page = DefaultPage
	}
	if r.PageSize < 1 {
		r.PageSize = DefaultPageSize
	}
	if r.MaxPageSize > 0 && r.PageSize > r.MaxPageSize {
		r.PageSize = r.MaxPageSize
	}
	return r
}

// Offset returns the zero-based row offset for this request.
func (r Request) Offset() int {
	r = r.Normalize()
	return safeOffset(r.Page, r.PageSize)
}

// Limit returns the normalized page size as a SQL LIMIT value.
func (r Request) Limit() int {
	return r.Normalize().PageSize
}

// Result contains the items and metadata for a paginated query.
type Result[E any] struct {
	Items       collectionx.List[E]
	Total       int64
	Page        int
	PageSize    int
	Offset      int
	TotalPages  int
	HasNext     bool
	HasPrevious bool
}

// NewResult creates a page result from items, total row count, and request metadata.
func NewResult[E any](items collectionx.List[E], total int64, request Request) Result[E] {
	request = request.Normalize()
	totalPages := totalPages(total, request.PageSize)
	return Result[E]{
		Items:       items,
		Total:       total,
		Page:        request.Page,
		PageSize:    request.PageSize,
		Offset:      request.Offset(),
		TotalPages:  totalPages,
		HasNext:     request.Page < totalPages,
		HasPrevious: request.Page > DefaultPage,
	}
}

// MapResult maps page items while preserving pagination metadata.
func MapResult[E any, R any](result Result[E], mapper func(index int, item E) R) Result[R] {
	return Result[R]{
		Items:       collectionx.MapList[E, R](result.Items, mapper),
		Total:       result.Total,
		Page:        result.Page,
		PageSize:    result.PageSize,
		Offset:      result.Offset,
		TotalPages:  result.TotalPages,
		HasNext:     result.HasNext,
		HasPrevious: result.HasPrevious,
	}
}

func safeOffset(page, pageSize int) int {
	if page <= DefaultPage || pageSize <= 0 {
		return 0
	}
	multiplier := page - 1
	maxValue := maxInt()
	if multiplier > maxValue/pageSize {
		return maxValue
	}
	return multiplier * pageSize
}

func totalPages(total int64, pageSize int) int {
	if total <= 0 || pageSize <= 0 {
		return 0
	}
	pages := ((total - 1) / int64(pageSize)) + 1
	if pages > int64(maxInt()) {
		return maxInt()
	}
	return int(pages)
}

func maxInt() int {
	return int(^uint(0) >> 1)
}
