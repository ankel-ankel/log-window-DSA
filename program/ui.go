package program

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/list"
	tui "github.com/charmbracelet/bubbletea"
	styles "github.com/charmbracelet/lipgloss"
	plot "github.com/chriskim06/drawille-go"
)

type uiConfig struct {
	K              int
	WindowTicks    int
	TickSize       time.Duration
	WindowSize     time.Duration
	PlotFPS        int
	ItemsFPS       int
	ViewSplit      int
	InputPath      string
	ReplaySpeed    float64
	ReplayMaxSleep time.Duration
	StatsWindow    int
	PlotPoints     int
}

func defaultUIConfig() uiConfig {
	return uiConfig{
		K:              20,
		WindowTicks:    60,
		TickSize:       time.Minute,
		WindowSize:     time.Hour,
		PlotFPS:        15,
		ItemsFPS:       2,
		ViewSplit:      30,
		InputPath:      "./data/access.log",
		ReplaySpeed:    500,
		ReplayMaxSleep: 10 * time.Millisecond,
		StatsWindow:    256,
		PlotPoints:     60,
	}
}

var (
	selectedColor = styles.AdaptiveColor{Light: "0", Dark: "9"}
	borderColor   = styles.AdaptiveColor{Light: "#555", Dark: "#555"}
	selectedFg    = styles.NewStyle().Foreground(selectedColor)
	borderFg      = styles.NewStyle().Foreground(borderColor)
	plotStyle     = styles.NewStyle().
			BorderStyle(styles.NormalBorder()).
			Foreground(borderColor).
			BorderForeground(borderColor)
)

type uiModel struct {
	cfg            uiConfig
	method         Method
	methodName     string
	width, height  int
	leftPaneWidth  int
	rightPaneWidth int

	track    bool
	logScale atomic.Bool
	err      error

	paused    bool
	pauseMu   sync.Mutex
	pauseCond *sync.Cond

	list         list.Model
	listStyle    styles.Style
	listDelegate *list.DefaultDelegate
	help         help.Model
	plot         *plot.Canvas

	methodMu       sync.Mutex
	plotData       [][]float64
	plotLineColors []plot.Color
	listItems      []Item
	latestTick     time.Time

	timestampsFromData atomic.Bool

	metrics *latencyMetrics

	done chan struct{}
	mu   sync.Mutex
}

func RunUI(m Method, name string) {
	cfg := defaultUIConfig()
	mdl := newUIModel(m, name, cfg)
	opts := []tui.ProgramOption{tui.WithInputTTY(), tui.WithAltScreen()}
	if _, err := tui.NewProgram(mdl, opts...).Run(); err != nil {
		log.Fatal(err)
	}
}

func newUIModel(m Method, name string, cfg uiConfig) *uiModel {
	const defaultWidth, defaultHeight = 80, 20

	d := list.NewDefaultDelegate()
	d.Styles.SelectedTitle = styles.NewStyle().
		Border(styles.NormalBorder(), false, false, false, true).
		BorderForeground(borderColor).
		Foreground(selectedColor).
		Bold(false).
		Padding(0, 0, 0, 1)
	d.Styles.SelectedDesc = d.Styles.SelectedTitle.Foreground(selectedColor)
	d.ShowDescription = true

	l := list.New(make([]list.Item, 0), d, defaultWidth/2-2, defaultHeight)
	l.Styles.NoItems = l.Styles.NoItems.Padding(0, 2)
	l.SetFilteringEnabled(true)
	l.SetShowHelp(false)
	l.SetShowTitle(false)
	l.SetShowStatusBar(false)

	p := plot.NewCanvas(defaultWidth, defaultHeight)
	p.NumDataPoints = cfg.PlotPoints
	p.ShowAxis = false
	p.LineColors = make([]plot.Color, cfg.K+1)

	mdl := &uiModel{
		cfg:            cfg,
		method:         m,
		methodName:     name,
		help:           help.New(),
		list:           l,
		listDelegate:   &d,
		plot:           &p,
		plotData:       make([][]float64, cfg.K+1),
		plotLineColors: make([]plot.Color, cfg.K+1),
		metrics:        newLatencyMetrics(cfg.StatsWindow),
		done:           make(chan struct{}),
	}
	mdl.leftPaneWidth, mdl.rightPaneWidth = computePaneWidths(defaultWidth, cfg.ViewSplit)
	mdl.pauseCond = sync.NewCond(&mdl.pauseMu)
	mdl.timestampsFromData.Store(false)
	mdl.metrics.setEnabled(true)
	for i := range mdl.plotData {
		mdl.plotData[i] = make([]float64, cfg.PlotPoints)
	}
	mdl.plot.Fill(mdl.plotData)
	return mdl
}

func (m *uiModel) leftWidth() int {
	if m.leftPaneWidth > 0 {
		return m.leftPaneWidth
	}
	left, _ := computePaneWidths(m.width, m.cfg.ViewSplit)
	return left
}

func (m *uiModel) rightWidth() int {
	if m.rightPaneWidth > 0 {
		return m.rightPaneWidth
	}
	_, right := computePaneWidths(m.width, m.cfg.ViewSplit)
	return right
}

func (m *uiModel) readAccessLog() tui.Cmd {
	return func() tui.Msg {
		f, err := os.Open(m.cfg.InputPath)
		if err != nil {
			return errMsg{err}
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		const layout = "02/Jan/2006:15:04:05 -0700"

		var last, prevEvent time.Time
		useEventTime := false

		for scanner.Scan() {
			if m.isDone() {
				return nil
			}
			m.waitIfPaused()
			line := scanner.Text()
			ip, ts, ok := strings.Cut(line, " - - [")
			if !ok {
				continue
			}
			ts, _, ok = strings.Cut(ts, "]")
			if !ok {
				continue
			}

			eventTime, _ := time.Parse(layout, ts)
			if !eventTime.IsZero() {
				if !useEventTime {
					useEventTime = true
					m.timestampsFromData.Store(true)
				}
				if !prevEvent.IsZero() {
					sleep := time.Duration(float64(eventTime.Sub(prevEvent)) / m.cfg.ReplaySpeed)
					if sleep > 0 {
						if m.cfg.ReplayMaxSleep > 0 && sleep > m.cfg.ReplayMaxSleep {
							sleep = m.cfg.ReplayMaxSleep
						}
						time.Sleep(sleep)
					}
				}
				prevEvent = eventTime
				m.metrics.observeEventTime(eventTime)
				last = m.doMethodTicks(eventTime, last)
				m.mu.Lock()
				m.latestTick = last
				m.mu.Unlock()
			}

			now := time.Now()
			m.methodMu.Lock()
			m.method.Add(ip)
			m.methodMu.Unlock()
			m.metrics.observeIngest(now)
		}
		_ = io.EOF
		return nil
	}
}

func (m *uiModel) methodTickCmd() tui.Cmd {
	return func() tui.Msg {
		var last time.Time
		ticker := time.NewTicker(m.cfg.TickSize)
		defer ticker.Stop()
		for {
			select {
			case <-m.done:
				return nil
			case t := <-ticker.C:
				m.waitIfPaused()
				if m.timestampsFromData.Load() {
					continue
				}
				t = t.Truncate(m.cfg.TickSize)
				m.mu.Lock()
				m.latestTick = t
				m.mu.Unlock()
				last = m.doMethodTicks(t, last)
			}
		}
	}
}

func (m *uiModel) doMethodTicks(t, last time.Time) time.Time {
	t = t.Truncate(m.cfg.TickSize)
	if last.IsZero() {
		return t
	}
	if ticks := int(t.Sub(last) / m.cfg.TickSize); ticks > 0 {
		m.methodMu.Lock()
		for i := 0; i < ticks; i++ {
			m.method.Tick()
		}
		m.methodMu.Unlock()
		last = t
	}
	return last
}

type itemsTickMsg time.Time
type plotTickMsg time.Time
type errMsg struct{ err error }

func doItemsTick(fps int) tui.Cmd {
	return tui.Every(time.Second/time.Duration(fps), func(t time.Time) tui.Msg {
		return itemsTickMsg(t)
	})
}

func doPlotTick(fps int) tui.Cmd {
	return tui.Every(time.Second/time.Duration(fps), func(t time.Time) tui.Msg {
		return plotTickMsg(t)
	})
}

func (m *uiModel) Init() tui.Cmd {
	return tui.Batch(m.methodTickCmd(), m.readAccessLog(), doPlotTick(m.cfg.PlotFPS), doItemsTick(m.cfg.ItemsFPS))
}

func (m *uiModel) Update(msg tui.Msg) (tui.Model, tui.Cmd) {
	switch msg := msg.(type) {
	case errMsg:
		m.mu.Lock()
		m.err = msg.err
		m.mu.Unlock()
		return m, nil
	case itemsTickMsg:
		if m.isPaused() {
			return m, doItemsTick(m.cfg.ItemsFPS)
		}
		m.refreshTopK()
		cmdList := m.updateList(msg)
		return m, tui.Batch(cmdList, doItemsTick(m.cfg.ItemsFPS))
	case plotTickMsg:
		cmd := m.updatePlot(msg)
		return m, tui.Batch(cmd, doPlotTick(m.cfg.PlotFPS))
	case tui.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.leftPaneWidth, m.rightPaneWidth = computePaneWidths(m.width, m.cfg.ViewSplit)
		statsLines := 7
		helpLines := 1
		available := max(1, m.height-statsLines-helpLines)

		leftW := max(1, m.leftWidth())
		rightW := max(1, m.rightWidth())

		m.list.SetSize(leftW, available)
		m.list.Styles.Title = styles.NewStyle()
		m.list.Styles.PaginationStyle = styles.NewStyle()
		m.list.Styles.HelpStyle = styles.NewStyle()
		m.listStyle = styles.NewStyle().Width(leftW).Height(available)

		plotHeight := max(1, available-3)
		plotWidth := max(1, rightW-2)
		m.resizePlot(plotWidth, plotHeight)
		return m, nil
	case tui.KeyMsg:
		switch {
		case key.Matches(msg, keys.Quit):
			m.shutdown()
			return m, tui.Quit
		case key.Matches(msg, keys.Pause):
			m.togglePause()
			return m, nil
		case key.Matches(msg, keys.Track):
			m.toggleTracking()
			return m, nil
		case key.Matches(msg, keys.Scale):
			m.toggleScale()
			return m, nil
		}
	}
	var cmd tui.Cmd
	m.list, cmd = m.list.Update(msg)
	return m, cmd
}

func (m *uiModel) toggleTracking() {
	m.mu.Lock()
	m.track = !m.track
	m.mu.Unlock()
}

func (m *uiModel) toggleScale() {
	for {
		old := m.logScale.Load()
		if m.logScale.CompareAndSwap(old, !old) {
			return
		}
	}
}

func (m *uiModel) togglePause() {
	m.pauseMu.Lock()
	m.paused = !m.paused
	m.pauseMu.Unlock()
	m.pauseCond.Broadcast()
}

func (m *uiModel) isPaused() bool {
	m.pauseMu.Lock()
	defer m.pauseMu.Unlock()
	return m.paused
}

func (m *uiModel) waitIfPaused() {
	m.pauseMu.Lock()
	for m.paused && !m.isDone() {
		m.pauseCond.Wait()
	}
	m.pauseMu.Unlock()
}

func (m *uiModel) isDone() bool {
	select {
	case <-m.done:
		return true
	default:
		return false
	}
}

func (m *uiModel) shutdown() {
	close(m.done)
	m.pauseCond.Broadcast()
}

func (m *uiModel) refreshTopK() {
	m.methodMu.Lock()
	items := m.method.TopK(m.cfg.K)
	m.methodMu.Unlock()
	m.mu.Lock()
	m.listItems = items
	m.mu.Unlock()
}

func (m *uiModel) resizePlot(w, h int) {
	p := plot.NewCanvas(w, h)
	p.NumDataPoints = m.plot.NumDataPoints
	p.ShowAxis = m.plot.ShowAxis
	p.LineColors = m.plot.LineColors
	m.plot = &p
}

func (m *uiModel) updateList(msg tui.Msg) tui.Cmd {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := make([]list.Item, len(m.listItems))
	order := make(map[string]int)

	m.listDelegate.Styles.SelectedTitle = m.listDelegate.Styles.SelectedTitle.Bold(m.track)
	m.listDelegate.Styles.SelectedDesc = m.listDelegate.Styles.SelectedDesc.Bold(m.track)
	m.list.SetDelegate(m.listDelegate)

	numDecimals := 1 + int(math.Ceil(math.Log10(float64(m.cfg.K+1))))
	pad := strings.Repeat(" ", numDecimals+1)
	rankFmt := "#%-" + fmt.Sprint(numDecimals) + "d"
	for i, it := range m.listItems {
		items[i] = listItem{
			DescriptionPrefix: pad,
			TitlePrefix:       fmt.Sprintf(rankFmt, i+1),
			Item:              it,
		}
		order[it.Item] = i
	}
	selected := m.list.SelectedItem()
	set := m.list.SetItems(items)
	var cmd tui.Cmd
	if m.track && selected != nil {
		if i, ok := order[selected.(listItem).Item.Item]; ok {
			m.list.Select(i)
		}
	}
	m.list, cmd = m.list.Update(msg)
	return tui.Batch(set, cmd)
}

func (m *uiModel) updatePlot(_ tui.Msg) tui.Cmd {
	logScale := m.logScale.Load()

	var highlight, dim plot.Color
	if styles.DefaultRenderer().HasDarkBackground() {
		highlight, dim = plot.Red, plot.DimGray
	} else {
		highlight, dim = plot.Black, plot.LightGray
	}

	m.mu.Lock()
	selected := m.list.Index()
	items := make([]Item, len(m.listItems))
	copy(items, m.listItems)
	m.mu.Unlock()
	if len(items) == 0 {
		return nil
	}

	for i := range m.plotData {
		m.plotLineColors[i] = dim
	}
	m.methodMu.Lock()
	for i := range items {
		series := m.plotData[i]
		item := items[(1+selected+i)%len(items)]
		hist := m.method.PlotHistory(item.Item, len(series))
		if hist == nil {
			for j := range series {
				series[j] = 0
			}
		} else {
			copy(series, hist)
			if logScale {
				for j := range series {
					if series[j] < 1 {
						series[j] = 0
					} else {
						series[j] = math.Log(series[j])
					}
				}
			}
		}
	}
	m.methodMu.Unlock()
	n := len(items)
	m.plotLineColors[n] = highlight
	m.plotLineColors[n-1] = dim
	last := m.plotData[n]
	for j := range last {
		last[j] = 0
	}
	m.plotData[n], m.plotData[n-1] = m.plotData[n-1], m.plotData[n]
	m.mu.Lock()
	m.plotLineColors, m.plot.LineColors = m.plot.LineColors, m.plotLineColors
	m.mu.Unlock()
	m.plot.Fill(m.plotData[:n+1])
	return nil
}

func (m *uiModel) View() string {
	left := m.listStyle.Render(m.list.View())
	plotStr := m.plot.String()

	if plotStr == "" {
		sb := emptyPlot(m)
		plotStr = sb.String()
	}

	linColor, logColor := borderFg, borderFg
	if m.logScale.Load() {
		logColor = selectedFg
	} else {
		linColor = selectedFg
	}
	linLog := linColor.Render("LIN") + " " + logColor.Render("LOG")

	labels := ""
	m.mu.Lock()
	latestTick := m.latestTick
	m.mu.Unlock()
	if !latestTick.IsZero() {
		w := max(0, m.rightWidth()-2)
		leftLabel := latestTick.Add(-m.cfg.WindowSize).UTC().Format(time.RFC3339)
		rightLabel := latestTick.UTC().Format(time.RFC3339)
		minWidth := len(leftLabel) + len(rightLabel) + len("LIN LOG") + 4

		if w < minWidth {
			leftLabel = latestTick.Add(-m.cfg.WindowSize).UTC().Format("15:04:05")
			rightLabel = latestTick.UTC().Format("15:04:05")
			minWidth = len(leftLabel) + len(rightLabel) + len("LIN LOG") + 4
		}
		if w < minWidth {
			labels = " " + linLog
		} else {
			spaceTotal := w - (len(leftLabel) + len(rightLabel) + len("LIN LOG"))
			if spaceTotal < 2 {
				spaceTotal = 2
			}
			leftGap := spaceTotal / 2
			rightGap := spaceTotal - leftGap
			labels = leftLabel +
				strings.Repeat(" ", leftGap) +
				linLog +
				strings.Repeat(" ", rightGap) +
				borderFg.Render(rightLabel)
		}
	}
	right := plotStyle.Render(styles.JoinVertical(styles.Top, plotStr, labels))
	view := styles.JoinHorizontal(styles.Top, left, right)

	m.mu.Lock()
	err := m.err
	m.mu.Unlock()
	if err != nil {
		errStyle := styles.NewStyle().Foreground(styles.AdaptiveColor{Light: "1", Dark: "9"})
		return styles.JoinVertical(styles.Left, view, errStyle.Render("ERROR: "+err.Error()), m.help.View(keys))
	}

	snap := m.metrics.snapshot()
	title := fmt.Sprintf("STATS (%s)", m.methodName)
	if m.isPaused() {
		title += " — PAUSED"
	}

	topItem, topCount := "-", uint32(0)
	m.mu.Lock()
	if len(m.listItems) > 0 {
		topItem = m.listItems[0].Item
		topCount = m.listItems[0].Count
	}
	m.mu.Unlock()

	tracked := "off"
	if m.track {
		tracked = "-"
		selected := m.list.SelectedItem()
		if selected != nil {
			if li, ok := selected.(listItem); ok {
				tracked = fmt.Sprintf("%s (%d)", li.Item.Item, li.Item.Count)
			}
		}
	}
	statsBlock := []string{
		title,
		fmt.Sprintf("records: %d", snap.records),
		fmt.Sprintf("throughput: %d rec/s", snap.ingestRps),
	}
	if !snap.lastEventTime.IsZero() {
		statsBlock = append(statsBlock, fmt.Sprintf("replay position: %s", snap.lastEventTime.UTC().Format(time.RFC3339)))
	}
	statsBlock = append(statsBlock,
		fmt.Sprintf("top-1: %s (%d)", topItem, topCount),
		fmt.Sprintf("track: %s", tracked),
	)

	statsStyle := styles.NewStyle().Foreground(styles.AdaptiveColor{Light: "1", Dark: "9"})
	return styles.JoinVertical(styles.Left, view, statsStyle.Render(strings.Join(statsBlock, "\n")), m.help.View(keys))
}

func emptyPlot(m *uiModel) strings.Builder {
	var sb strings.Builder
	if m.width < 2 || m.height < 4 {
		return sb
	}
	w := max(1, m.rightWidth()-2)
	h := max(1, m.list.Height()-2)
	sb.Grow(w * h)
	spaces := strings.Repeat(" ", w)
	for range h {
		sb.WriteString(spaces)
		sb.WriteRune('\n')
	}
	return sb
}

func computePaneWidths(totalWidth, splitPercent int) (left, right int) {
	if totalWidth <= 1 {
		return 1, 1
	}
	left = totalWidth * splitPercent / 100
	if left < 1 {
		left = 1
	}
	if left > totalWidth-1 {
		left = totalWidth - 1
	}
	right = totalWidth - left

	const minPane = 18
	if totalWidth >= minPane*2 {
		if left < minPane {
			left = minPane
			right = totalWidth - left
		}
		if right < minPane {
			right = minPane
			left = totalWidth - right
		}
	}
	if left < 1 {
		left = 1
	}
	if right < 1 {
		right = 1
	}
	return left, right
}

type listItem struct {
	DescriptionPrefix string
	TitlePrefix       string
	Item              Item
}

func (i listItem) Title() string       { return fmt.Sprintf("%s %s", i.TitlePrefix, i.Item.Item) }
func (i listItem) Description() string { return fmt.Sprintf("%s %d", i.DescriptionPrefix, i.Item.Count) }
func (i listItem) FilterValue() string { return i.Item.Item }

type keyMap struct {
	Track key.Binding
	Scale key.Binding
	Pause key.Binding
	Quit  key.Binding
}

func (k keyMap) ShortHelp() []key.Binding { return []key.Binding{k.Quit, k.Pause, k.Track, k.Scale} }
func (k keyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{{k.Quit, k.Pause}, {k.Track, k.Scale}}
}

var keys = keyMap{
	Track: key.NewBinding(key.WithKeys("t", " "), key.WithHelp("t/space", "track")),
	Scale: key.NewBinding(key.WithKeys("s"), key.WithHelp("s", "log/lin")),
	Pause: key.NewBinding(key.WithKeys("p"), key.WithHelp("p", "pause")),
	Quit:  key.NewBinding(key.WithKeys("q", "ctrl+c"), key.WithHelp("q/ctrl+c", "quit")),
}

type latencyMetrics struct {
	enabled         atomic.Bool
	ingestedRecords atomic.Uint64
	lastEventTimeNs atomic.Int64
	mu              sync.Mutex
	rateCounter     int64
	rateLastBucket  int64
	rateBuckets     *int64Ring
}

func newLatencyMetrics(window int) *latencyMetrics {
	if window < 16 {
		window = 16
	}
	return &latencyMetrics{rateBuckets: newInt64Ring(window)}
}

func (m *latencyMetrics) setEnabled(v bool) { m.enabled.Store(v) }

func (m *latencyMetrics) observeIngest(now time.Time) {
	if !m.enabled.Load() {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	m.ingestedRecords.Add(1)
	bucket := now.Unix()
	m.mu.Lock()
	if bucket == m.rateLastBucket {
		m.rateCounter++
	} else {
		if m.rateLastBucket > 0 {
			m.rateBuckets.add(m.rateCounter)
			idle := bucket - m.rateLastBucket - 1
			if idle > int64(len(m.rateBuckets.buf)) {
				idle = int64(len(m.rateBuckets.buf))
			}
			for i := int64(0); i < idle; i++ {
				m.rateBuckets.add(0)
			}
		}
		m.rateLastBucket = bucket
		m.rateCounter = 1
	}
	m.mu.Unlock()
}

func (m *latencyMetrics) observeEventTime(t time.Time) {
	if !t.IsZero() {
		m.lastEventTimeNs.Store(t.UnixNano())
	}
}

type metricsSnapshot struct {
	records       uint64
	ingestRps     int64
	lastEventTime time.Time
}

func (m *latencyMetrics) snapshot() metricsSnapshot {
	if !m.enabled.Load() {
		return metricsSnapshot{}
	}
	records := m.ingestedRecords.Load()
	lastEventNs := m.lastEventTimeNs.Load()
	m.mu.Lock()
	rps := m.currentRate()
	m.mu.Unlock()
	var lastEventTime time.Time
	if lastEventNs > 0 {
		lastEventTime = time.Unix(0, lastEventNs)
	}
	return metricsSnapshot{records: records, ingestRps: rps, lastEventTime: lastEventTime}
}

func (m *latencyMetrics) currentRate() int64 {
	if m.rateLastBucket == 0 {
		return 0
	}
	r := &int64Ring{
		buf:   append([]int64(nil), m.rateBuckets.buf...),
		idx:   m.rateBuckets.idx,
		count: m.rateBuckets.count,
	}
	r.add(m.rateCounter)
	idle := time.Now().Unix() - m.rateLastBucket - 1
	if idle > int64(len(r.buf)) {
		idle = int64(len(r.buf))
	}
	for i := int64(0); i < idle; i++ {
		r.add(0)
	}
	return medianRate(r)
}

func medianRate(r *int64Ring) int64 {
	if r == nil || r.count == 0 {
		return 0
	}
	vals := ringValues(r)
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	return vals[len(vals)/2]
}

func ringValues(r *int64Ring) []int64 {
	vals := make([]int64, 0, r.count)
	for i := 0; i < r.count; i++ {
		idx := r.idx - r.count + i
		for idx < 0 {
			idx += len(r.buf)
		}
		vals = append(vals, r.buf[idx%len(r.buf)])
	}
	return vals
}

type int64Ring struct {
	buf   []int64
	idx   int
	count int
}

func newInt64Ring(n int) *int64Ring {
	if n < 1 {
		n = 1
	}
	return &int64Ring{buf: make([]int64, n)}
}

func (r *int64Ring) add(v int64) {
	if len(r.buf) == 0 {
		return
	}
	r.buf[r.idx] = v
	r.idx++
	if r.idx >= len(r.buf) {
		r.idx = 0
	}
	if r.count < len(r.buf) {
		r.count++
	}
}
