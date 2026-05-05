package program

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

type benchSample struct {
	Records       int
	AddLatencyNs  int64
	TickLatencyNs int64
	TopKLatencyNs int64
	MemMB         float64
}

type latencyStats struct {
	P50, P95, P99, Mean time.Duration
}

type benchResult struct {
	Method          string
	Samples         []benchSample
	Total           time.Duration
	Records         int
	Add, Tick, TopK latencyStats
	PeakMemMB       float64
}

func (r benchResult) totalService(queryCount int) time.Duration {
	return r.Total + time.Duration(queryCount)*r.TopK.P50
}

var topkSink uint64

const realisticQueries = 1000

func RunBench() {
	var (
		inputPath     = flag.String("in", "./data/access.log", "")
		maxLines      = flag.Int("n", 0, "")
		k             = flag.Int("k", 20, "")
		windowTicks   = flag.Int("window", 60, "")
		chunkSize     = flag.Int("chunk", 10000, "")
		ticksPerChunk = flag.Int("ticks-per-chunk", 10, "")
		outDir        = flag.String("out", "./results", "")
	)
	flag.Parse()

	fmt.Printf("Reading %s ...\n", *inputPath)
	records, err := loadRecords(*inputPath, *maxLines)
	if err != nil {
		log.Fatalf("Error reading file: %v", err)
	}
	fmt.Printf("Loaded %d records\n\n", len(records))

	factories := []func() Method{
		func() Method { return NewNaive() },
		func() Method { return NewSlidingNaive(*windowTicks) },
		func() Method { return NewProposed(*k, *windowTicks) },
	}

	results := make([]benchResult, 0, len(factories))
	for _, factory := range factories {
		name := factory().Name()
		fmt.Printf("Running %s ... ", name)
		r := runBench(factory, records, *k, *chunkSize, *ticksPerChunk)
		results = append(results, r)
		fmt.Printf("done (%v)\n", r.Total)
	}
	fmt.Println()

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatal(err)
	}
	saveCSV(filepath.Join(*outDir, "latency.csv"), results)
	saveSummary(filepath.Join(*outDir, "summary.txt"), results, *k, *windowTicks)
	saveHTML(filepath.Join(*outDir, "chart.html"), results)

	printComparison(results)
	fmt.Printf("\nResults: %s/chart.html\n", *outDir)
}

func loadRecords(path string, maxLines int) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	cap := maxLines
	if cap <= 0 {
		cap = 1 << 20
	}
	records := make([]string, 0, cap)
	for scanner.Scan() {
		line := scanner.Text()
		ip, _, ok := strings.Cut(line, " - - [")
		if !ok || ip == "" {
			continue
		}
		records = append(records, ip)
		if maxLines > 0 && len(records) >= maxLines {
			break
		}
	}
	return records, scanner.Err()
}

func runBench(factory func() Method, records []string, k, chunkSize, ticksPerChunk int) benchResult {
	runtime.GC()
	var mb runtime.MemStats
	runtime.ReadMemStats(&mb)
	baseMB := float64(mb.HeapInuse) / 1024 / 1024

	m := factory()

	n := (len(records) + chunkSize - 1) / chunkSize
	samples := make([]benchSample, 0, n)
	addLat := make([]time.Duration, 0, n)
	tickLat := make([]time.Duration, 0, n*ticksPerChunk)
	topkLat := make([]time.Duration, 0, n)

	var peakMB float64
	var ingest time.Duration

	for i := 0; i < len(records); i += chunkSize {
		end := i + chunkSize
		if end > len(records) {
			end = len(records)
		}
		chunk := records[i:end]

		t0 := time.Now()
		for _, r := range chunk {
			m.Add(r)
		}
		addEl := time.Since(t0)
		ingest += addEl
		addAvg := addEl / time.Duration(len(chunk))

		var totalTick time.Duration
		for t := 0; t < ticksPerChunk; t++ {
			s := time.Now()
			m.Tick()
			e := time.Since(s)
			totalTick += e
			tickLat = append(tickLat, e)
		}
		ingest += totalTick
		var tickAvg time.Duration
		if ticksPerChunk > 0 {
			tickAvg = totalTick / time.Duration(ticksPerChunk)
		}

		topkE := measureTopK(m, k)

		runtime.GC()
		runtime.ReadMemStats(&mb)
		memMB := float64(mb.HeapInuse)/1024/1024 - baseMB
		if memMB < 0 {
			memMB = 0
		}
		if memMB > peakMB {
			peakMB = memMB
		}

		samples = append(samples, benchSample{
			Records:       end,
			AddLatencyNs:  addAvg.Nanoseconds(),
			TickLatencyNs: tickAvg.Nanoseconds(),
			TopKLatencyNs: topkE.Nanoseconds(),
			MemMB:         memMB,
		})
		addLat = append(addLat, addAvg)
		topkLat = append(topkLat, topkE)
	}

	return benchResult{
		Method:    m.Name(),
		Samples:   samples,
		Total:     ingest,
		Records:   len(records),
		Add:       stats(addLat),
		Tick:      stats(tickLat),
		TopK:      stats(topkLat),
		PeakMemMB: peakMB,
	}
}

func measureTopK(m Method, k int) time.Duration {
	repeats := 10
	for {
		t0 := time.Now()
		for j := 0; j < repeats; j++ {
			items := m.TopK(k)
			if len(items) > 0 {
				topkSink += uint64(items[0].Count)
			}
		}
		e := time.Since(t0)
		if e >= time.Millisecond || repeats >= 100000 {
			return e / time.Duration(repeats)
		}
		repeats *= 10
	}
}

func stats(durs []time.Duration) latencyStats {
	if len(durs) == 0 {
		return latencyStats{}
	}
	s := append([]time.Duration(nil), durs...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	var sum time.Duration
	for _, d := range s {
		sum += d
	}
	pct := func(p int) time.Duration {
		i := len(s) * p / 100
		if i >= len(s) {
			i = len(s) - 1
		}
		return s[i]
	}
	return latencyStats{P50: pct(50), P95: pct(95), P99: pct(99), Mean: sum / time.Duration(len(s))}
}

func toMs(d time.Duration) float64     { return float64(d.Nanoseconds()) / 1e6 }
func toMicros(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1000 }

func formatTopK(us float64) string {
	switch {
	case us >= 1000:
		return fmt.Sprintf("%.2f ms", us/1000)
	case us >= 10:
		return fmt.Sprintf("%.0f µs", us)
	case us >= 1:
		return fmt.Sprintf("%.1f µs", us)
	default:
		return fmt.Sprintf("%.2f µs", us)
	}
}

func formatService(ms float64) string {
	switch {
	case ms >= 1000:
		return fmt.Sprintf("%.1f s", ms/1000)
	case ms >= 10:
		return fmt.Sprintf("%.0f ms", ms)
	default:
		return fmt.Sprintf("%.2f ms", ms)
	}
}

func printComparison(results []benchResult) {
	fmt.Printf("%-15s %12s %14s %10s\n", "Method", "TopK", "Total service", "Memory")
	fmt.Println(strings.Repeat("-", 56))
	for _, r := range results {
		fmt.Printf("%-15s %12s %14s %8.2f MB\n",
			r.Method,
			formatTopK(toMs(r.TopK.P50)*1000),
			formatService(toMs(r.totalService(realisticQueries))),
			r.PeakMemMB)
	}
}

func saveCSV(path string, results []benchResult) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	fmt.Fprintln(w, "method,records,add_ns,tick_ns,topk_ns,mem_mb")
	for _, r := range results {
		for _, s := range r.Samples {
			fmt.Fprintf(w, "%s,%d,%d,%d,%d,%.4f\n",
				r.Method, s.Records, s.AddLatencyNs, s.TickLatencyNs, s.TopKLatencyNs, s.MemMB)
		}
	}
	return nil
}

func saveSummary(path string, results []benchResult, k, windowTicks int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if len(results) == 0 {
		return nil
	}
	fmt.Fprintf(f, "Benchmark Top-K Sliding Window\n")
	fmt.Fprintf(f, "K=%d, Window=%d ticks, %d records\n\n", k, windowTicks, results[0].Records)
	fmt.Fprintf(f, "%-15s %12s %14s %10s\n", "Method", "TopK", "Total service", "Memory")
	fmt.Fprintln(f, strings.Repeat("-", 56))
	for _, r := range results {
		fmt.Fprintf(f, "%-15s %12s %14s %8.2f MB\n",
			r.Method,
			formatTopK(toMs(r.TopK.P50)*1000),
			formatService(toMs(r.totalService(realisticQueries))),
			r.PeakMemMB)
	}
	fmt.Fprintf(f, "\n* Total service = ingest + %d TopK queries\n", realisticQueries)
	return nil
}

func saveHTML(path string, results []benchResult) error {
	if len(results) == 0 {
		return nil
	}

	type info struct {
		name    string
		topk    float64
		service float64
		mem     float64
	}

	list := make([]info, len(results))
	for i, r := range results {
		list[i] = info{
			name:    r.Method,
			topk:    toMicros(r.TopK.P50),
			service: float64(r.totalService(realisticQueries).Nanoseconds()) / 1e6,
			mem:     r.PeakMemMB,
		}
	}

	bestTopK, bestService, bestMem := list[0].topk, list[0].service, list[0].mem
	for _, x := range list {
		if x.topk < bestTopK {
			bestTopK = x.topk
		}
		if x.service < bestService {
			bestService = x.service
		}
		if x.mem < bestMem {
			bestMem = x.mem
		}
	}

	var rows strings.Builder
	for _, x := range list {
		fmt.Fprintf(&rows,
			"<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>\n",
			x.name,
			highlight(formatTopK(x.topk), x.topk == bestTopK),
			highlight(formatService(x.service), x.service == bestService),
			highlight(fmt.Sprintf("%.2f MB", x.mem), x.mem == bestMem))
	}

	var prop info
	for _, x := range list {
		if x.name == "Proposed" {
			prop = x
		}
	}
	cmpRow := func(label string, b info) {
		topkR := b.topk / prop.topk
		svcR := b.service / prop.service
		memR := prop.mem / b.mem
		memCell := fmt.Sprintf(`<span class="worse">%.1fx more</span>`, memR)
		if memR < 1 {
			memCell = fmt.Sprintf(`<span class="best">%.1fx less</span>`, 1/memR)
		}
		fmt.Fprintf(&rows,
			`<tr class="cmp"><td>Proposed %s</td><td><span class="best">%.0fx faster</span></td><td><span class="best">%.1fx faster</span></td><td>%s</td></tr>`+"\n",
			label, topkR, svcR, memCell)
	}
	for _, x := range list {
		if x.name == "Naive" {
			cmpRow("vs Naive", x)
		}
	}
	for _, x := range list {
		if x.name == "Sliding Naive" {
			cmpRow("vs Sliding Naive", x)
		}
	}

	type series struct {
		Label string    `json:"label"`
		Data  []float64 `json:"data"`
		Color string    `json:"color"`
	}
	colors := []string{"#ef4444", "#f59e0b", "#10b981"}

	var labels []int
	for _, s := range results[0].Samples {
		labels = append(labels, s.Records)
	}

	topkS := make([]series, len(results))
	cumtopkS := make([]series, len(results))
	for i, r := range results {
		topkData := make([]float64, len(r.Samples))
		cum := make([]float64, len(r.Samples))
		var sum float64
		for j, s := range r.Samples {
			ms := float64(s.TopKLatencyNs) / 1e6
			topkData[j] = ms
			sum += ms
			cum[j] = sum
		}
		c := colors[i%len(colors)]
		topkS[i] = series{r.Method, topkData, c}
		cumtopkS[i] = series{r.Method, cum, c}
	}

	labelsJSON, _ := json.Marshal(labels)
	topkJSON, _ := json.Marshal(topkS)
	cumtopkJSON, _ := json.Marshal(cumtopkS)

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintf(f, htmlTemplate,
		rows.String(),
		labelsJSON, topkJSON,
		labelsJSON, cumtopkJSON,
	)
	return nil
}

func highlight(s string, best bool) string {
	if best {
		return `<span class="best">` + s + `</span>`
	}
	return s
}

const htmlTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Benchmark Results</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
* { box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
       max-width: 980px; margin: 30px auto; padding: 0 20px;
       color: #1f2937; background: #fafafa; }
h1 { color: #111827; font-weight: 600; }
h2 { color: #374151; font-weight: 500; margin-top: 36px; }
table { border-collapse: collapse; width: 100%%; margin: 14px 0;
        background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; overflow: hidden; }
th, td { padding: 12px 16px; border-bottom: 1px solid #e5e7eb; text-align: right; }
tr:last-child td { border-bottom: 0; }
th { background: #f9fafb; font-weight: 500; color: #6b7280; font-size: 0.88em; }
td:first-child, th:first-child { text-align: left; font-weight: 500; }
.best { color: #047857; font-weight: 600; }
.worse { color: #b91c1c; font-weight: 600; }
tr.cmp td { background: #f9fafb; font-style: italic; }
tr.cmp td:first-child { color: #6b7280; }
.chart-box { background: #fff; padding: 16px; border-radius: 8px;
             border: 1px solid #e5e7eb; margin: 12px 0; }
</style>
</head>
<body>

<h1>Top-K Sliding Window — Benchmark</h1>

<h2>Comparison</h2>
<table>
<tr>
  <th>Method</th>
  <th>Top-K latency</th>
  <th>Total service</th>
  <th>Memory</th>
</tr>
%s
</table>

<h2>1. Top-K latency by records</h2>
<div class="chart-box"><canvas id="topkChart" height="100"></canvas></div>

<h2>2. Cumulative Top-K query time</h2>
<div class="chart-box"><canvas id="cumChart" height="100"></canvas></div>

<script>
const labels = %s;
const topkSeries = %s;
const labels2 = %s;
const cumSeries = %s;

function powerOf10Ticks(axis) {
  const range = Math.ceil(Math.log10(axis.max)) - Math.floor(Math.log10(axis.min));
  const step = range > 5 ? 2 : 1;
  const lo = Math.floor(Math.log10(axis.min) / step) * step;
  const hi = Math.ceil(Math.log10(axis.max) / step) * step;
  const ticks = [];
  for (let p = lo; p <= hi; p += step) ticks.push({ value: Math.pow(10, p) });
  axis.ticks = ticks;
  axis.min = Math.pow(10, lo);
  axis.max = Math.pow(10, hi);
}

function makeLine(canvasId, series, yLabel) {
  new Chart(document.getElementById(canvasId), {
    type: 'line',
    data: {
      labels: labels,
      datasets: series.map(s => ({
        label: s.label,
        data: s.data.map(v => v > 0 ? v : 0.001),
        borderColor: s.color, backgroundColor: s.color,
        tension: 0.1, pointRadius: 0, borderWidth: 2,
      }))
    },
    options: {
      responsive: true,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { position: 'top' },
        tooltip: {
          callbacks: {
            label: c => c.dataset.label + ': ' + Number(c.parsed.y).toFixed(5)
          }
        }
      },
      scales: {
        x: {
          title: { display: true, text: 'Records' },
          ticks: { autoSkip: true, maxTicksLimit: 11, maxRotation: 0 }
        },
        y: {
          type: 'logarithmic',
          title: { display: true, text: yLabel },
          afterBuildTicks: powerOf10Ticks,
          ticks: { callback: v => Number(v).toFixed(5) }
        }
      }
    }
  });
}

makeLine('topkChart', topkSeries, 'ms (log)');
makeLine('cumChart',  cumSeries,  'ms (log)');
</script>

</body>
</html>
`
