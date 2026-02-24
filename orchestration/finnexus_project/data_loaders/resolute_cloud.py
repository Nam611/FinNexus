-- 1. Xóa ràng buộc cũ (nếu có rác) để tránh lỗi
ALTER TABLE public.news_articles DROP CONSTRAINT IF EXISTS unique_url;

-- 2. Thêm ràng buộc UNIQUE cho cột URL
-- (Lệnh này đảm bảo không bao giờ có 2 bài báo cùng URL, giúp lệnh ON CONFLICT hoạt động)
ALTER TABLE public.news_articles ADD CONSTRAINT unique_url UNIQUE (url);