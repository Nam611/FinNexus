package crawler

import (
	"context"
	"crawler-go/internal/config"
	"crawler-go/internal/models"
	"crawler-go/internal/storage"
	"strings"
	"time"

	"github.com/gocolly/colly/v2"
	"go.uber.org/zap"
)

type Engine struct {
	cfg    *config.Config
	store  storage.Storage // <--- QUAN TRỌNG: Dùng Interface thay vì KafkaStore cụ thể
	logger *zap.Logger
}

// NewEngine: Inject dependencies (Config, Storage, Logger)
func NewEngine(cfg *config.Config, store storage.Storage, logger *zap.Logger) *Engine {
	return &Engine{
		cfg:    cfg,
		store:  store,
		logger: logger,
	}
}

// Start: Chạy Crawler với Context để hỗ trợ Timeout (Automation)
func (e *Engine) Start(ctx context.Context) error {
	// 1. Setup Collector A (Danh sách bài viết)
	c_list := colly.NewCollector(
		colly.AllowedDomains("cafef.vn", "m.cafef.vn", "c.cafef.vn"),
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
		colly.IgnoreRobotsTxt(),
		colly.Async(true), // Chạy đa luồng
	)

	// Giới hạn tốc độ để không bị chặn (Lấy từ Config nếu có, hoặc hardcode an toàn)
	c_list.Limit(&colly.LimitRule{
		DomainGlob:  "*cafef.vn*",
		Parallelism: 2,
		RandomDelay: 2 * time.Second,
	})

	// 2. Setup Collector B (Chi tiết bài viết) - Clone từ A để kế thừa cấu hình
	c_detail := c_list.Clone()

	// --- THIẾT LẬP LOGIC ---
	e.setupListHandler(c_list, c_detail)
	e.setupDetailHandler(c_detail)

	// 3. CƠ CHẾ BATCH MODE & TIMEOUT (Automation)
	// Tạo channel để lắng nghe khi nào cào xong
	done := make(chan bool)

	go func() {
		targetURL := "https://cafef.vn/tai-chinh-ngan-hang.chn" // Có thể đưa vào config
		e.logger.Info("🚀 Bắt đầu Crawler Clean Architecture...", zap.String("target", targetURL))

		c_list.Visit(targetURL)

		// Đợi cả 2 collector hoàn thành công việc
		c_list.Wait()
		c_detail.Wait()

		// Báo hiệu xong việc
		close(done)
	}()

	// 4. LẮNG NGHE SỰ KIỆN: Xong việc hoặc Hết giờ
	select {
	case <-done:
		e.logger.Info("✅ Batch Job hoàn thành xuất sắc!")
		return nil
	case <-ctx.Done():
		e.logger.Warn("⚠️ Batch Job bị Timeout (Hết giờ), buộc dừng!")
		return ctx.Err()
	}
}

// Xử lý trang danh sách: Tìm link bài viết
func (e *Engine) setupListHandler(c_list *colly.Collector, c_detail *colly.Collector) {
	// Logic cũ của bạn rất ổn, giữ nguyên
	c_list.OnHTML("a[href]", func(el *colly.HTMLElement) {
		link := el.Attr("href")
		title := strings.TrimSpace(el.Text)

		if title == "" {
			title = el.Attr("title")
		}

		// Lọc link rác
		if strings.Contains(link, ".chn") && !strings.Contains(link, "javascript:") && len(title) > 10 {
			// Xử lý link tương đối
			if !strings.HasPrefix(link, "http") {
				if strings.HasPrefix(link, "/") {
					link = "https://cafef.vn" + link
				} else {
					link = "https://cafef.vn/" + link
				}
			}

			// Truyền Title sang Collector chi tiết để đỡ phải parse lại
			ctx := colly.NewContext()
			ctx.Put("title", title)

			// Gọi Collector chi tiết
			c_detail.Request("GET", link, nil, ctx, nil)
		}
	})

	c_list.OnResponse(func(r *colly.Response) {
		e.logger.Info("📂 Đã truy cập danh sách", zap.String("url", r.Request.URL.String()))
	})
}

// Xử lý trang chi tiết: Lấy nội dung và Lưu vào Storage
func (e *Engine) setupDetailHandler(c_detail *colly.Collector) {
	// Selector bao quát các loại layout của CafeF
	c_detail.OnHTML("div.detail-content, div#mainContent, div.contentdetail", func(el *colly.HTMLElement) {
		// Lấy lại title từ Context (hoặc parse lại nếu cần)
		title := el.Request.Ctx.Get("title")
		if title == "" {
			title = strings.TrimSpace(el.ChildText("h1.title"))
		}

		link := el.Request.URL.String()
		var contentBuilder strings.Builder

		// Lấy nội dung text từ các thẻ p
		el.ForEach("p", func(_ int, p *colly.HTMLElement) {
			text := strings.TrimSpace(p.Text)
			if text != "" {
				contentBuilder.WriteString(text)
				contentBuilder.WriteString("\n")
			}
		})

		content := contentBuilder.String()

		// Bỏ qua bài quá ngắn (có thể là bài video hoặc lỗi)
		if len(content) < 100 {
			return
		}

		// Map sang Model chuẩn (Article)
		article := models.Article{
			URL:         link,
			Title:       title,
			Source:      "CafeF",
			Content:     content,
			PublishedAt: time.Now(), // Tạm thời lấy giờ hiện tại
		}

		// --- ĐIỂM KHÁC BIỆT: GỌI INTERFACE ---
		// Dù Main đang config là Kafka hay Postgres, dòng này đều chạy đúng!
		err := e.store.Save(article)

		if err == nil {
			e.logger.Info("💾 LƯU THÀNH CÔNG", zap.String("title", title))
		} else {
			e.logger.Error("❌ Lỗi lưu Storage", zap.String("url", link), zap.Error(err))
		}
	})
}
