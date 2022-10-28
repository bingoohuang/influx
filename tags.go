package influx

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/influxdata/influxdb1-client/models"
	client "github.com/influxdata/influxdb1-client/v2"
)

var measurementRe = regexp.MustCompile(`select\s+.+\s+from (\S+)`)

func (c *Cli) queryTagKeys(cq *client.Query, series []models.Row) (map[string]bool, error) {
	if len(series) == 0 {
		return nil, nil
	}

	measurement := series[0].Name
	oldDb := cq.Database

	// 尝试从 select ... from 语句中提取完整表名（例如：metrics.autogen.QPS_dsvsServer）
	// 然后尝试从中获取库名，因为执行 `show tag keys from "measurement"` 时必须有库名
	if subs := measurementRe.FindStringSubmatch(cq.Command); len(subs) > 0 {
		if strings.Contains(subs[1], ".") {
			cq.Database = subs[1][:strings.Index(subs[1], ".")]
			// defer reset old DB
			defer func() { cq.Database = oldDb }()
		}
	}

	// 缓存 tag 键值列表，减少一次查询操作
	key := cacheKey{Addr: c.Addr, DB: cq.Database, Measurement: measurement}
	return cache.Get(key, func(k cacheKey) (map[string]bool, error) {
		return c.showTagKeys(cq, k)
	})
}

func (c *Cli) showTagKeys(cq *client.Query, k cacheKey) (map[string]bool, error) {
	// 名称可能像 QPS_dsvsServer，需要双引号引用起来
	cq.Command = `show tag keys from "` + k.Measurement + `"`
	rsp, err := c.Query(*cq)
	if err != nil {
		return nil, fmt.Errorf("execute %s %w", cq.Command, err)
	}
	if err := rsp.Error(); err != nil {
		return nil, fmt.Errorf("execute %s %w", cq.Command, err)
	}

	if r := rsp.Results; len(r) > 0 && len(r[0].Series) > 0 && len(r[0].Series[0].Values) > 0 {
		keys := make(map[string]bool)
		for _, k := range r[0].Series[0].Values {
			keys[k[0].(string)] = true
		}
		return keys, nil
	}

	return nil, nil
}

type tagsCollector interface {
	collect(k string, v interface{})
	complete(tags *map[string][]string)
}

type noopTagsConnector struct{}

func (t *noopTagsConnector) complete(tags *map[string][]string) {}
func (t *noopTagsConnector) collect(k string, v interface{})    {}

type tagsCollectorImpl struct {
	tagValues   map[string]*set
	tagKeys     map[string]bool
	valuesLimit int
}

func (t *tagsCollectorImpl) complete(tags *map[string][]string) {
	for k := range t.tagKeys {
		(*tags)[k] = t.tagValues[k].toSlice()
	}
}

func makeTagsCollector(option *QueryOption) tagsCollector {
	if option.ReturnTags == nil || len(option.tagKeys) == 0 {
		return &noopTagsConnector{}
	}

	return &tagsCollectorImpl{
		tagValues:   make(map[string]*set),
		tagKeys:     option.tagKeys,
		valuesLimit: option.ReturnTagValuesLimit,
	}
}

func (t *tagsCollectorImpl) collect(k string, v interface{}) {
	if t.tagKeys[k] {
		if cv, ok := v.(string); ok {
			m, ok2 := t.tagValues[k]
			if !ok2 {
				m = newSet(t.valuesLimit)
				t.tagValues[k] = m
			}
			m.add(cv)
		}
	}
}

type set struct {
	v     map[string]struct{}
	limit int
}

func (s *set) add(v string) {
	if s.limit <= 0 || s.len() < s.limit {
		s.v[v] = struct{}{}
	}
}

func (s *set) len() int { return len(s.v) }

func (s *set) toSlice() []string {
	mk := make([]string, 0, s.len())
	for v := range s.v {
		mk = append(mk, v)
	}
	sort.Strings(mk)
	return mk
}

func newSet(limit int) *set {
	return &set{v: make(map[string]struct{}), limit: limit}
}
