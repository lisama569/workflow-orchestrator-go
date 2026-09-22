package workflow

import "time"

// validDate 校验逻辑日期格式 YYYY-MM-DD（同时校验真实日历日期）。
func validDate(d string) bool {
	if len(d) != 10 || d[4] != '-' || d[7] != '-' {
		return false
	}
	_, err := time.Parse("2006-01-02", d)
	return err == nil
}

// datesBetween 返回闭区间 [start,end] 内的全部日期（含端点）。
func datesBetween(start, end string) []string {
	t0, _ := time.Parse("2006-01-02", start)
	t1, _ := time.Parse("2006-01-02", end)
	var out []string
	for t := t0; !t.After(t1); t = t.AddDate(0, 0, 1) {
		out = append(out, t.Format("2006-01-02"))
	}
	return out
}
