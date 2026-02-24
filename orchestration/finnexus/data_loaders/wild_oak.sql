-- 1. Dọn dẹp dữ liệu trùng
DELETE FROM public.news_articles a USING (
    SELECT MIN(ctid) as ctid, url
    FROM public.news_articles 
    GROUP BY url HAVING COUNT(*) > 1
) b
WHERE a.url = b.url 
AND a.ctid <> b.ctid;

-- 2. Xóa ràng buộc cũ
ALTER TABLE public.news_articles DROP CONSTRAINT IF EXISTS unique_url;

-- 3. Tạo ràng buộc mới
ALTER TABLE public.news_articles ADD CONSTRAINT unique_url UNIQUE (url);

-- 👇 THÊM DÒNG NÀY ĐỂ MAGE KHÔNG BÁO LỖI 👇
SELECT 1 as thanh_cong;