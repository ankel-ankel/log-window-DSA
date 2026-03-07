package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/list"
	tui "github.com/charmbracelet/bubbletea"
	styles "github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/term"
	plot "github.com/chriskim06/drawille-go"
	"github.com/keilerkonzept/topk"
	"github.com/keilerkonzept/topk/heap"
	"github.com/keilerkonzept/topk/sliding"
)

type Config struct {
	// sketch
	K            int
	Width        int
	Depth        int
	Decay        float64
	DecayLUTSize int
	TickSize     time.Duration
	WindowSize   time.Duration

	// render
	PlotFPS       int
	ItemsFPS      int
	ItemCountsFPS int
	TrackSelected bool
	LogScale      bool
	ViewSplit     int

	// input
	InputPath       string
	MaxLines        int
	Pace            time.Duration
	Replay          bool
	ReplaySpeed     float64
	ReplayMaxSleep  time.Duration
	AccessLog       bool
	JSON            bool
	TimestampLayout string

	// experiment
	SearchEnabled bool
	FullRefresh   time.Duration
	PartialSize   int

	StatsEnabled bool
	StatsWindow  int

	AltScreen bool
}

var config = Config{
	K:            50,
	Width:        3000,
	Depth:        3,
	Decay:        0.9,
	DecayLUTSize: 8192,
	TickSize:     time.Second,
	WindowSize:   10 * time.Second,

	ViewSplit:     50,
	PlotFPS:       20,
	ItemsFPS:      1,
	ItemCountsFPS: 5,

	InputPath:       "",
	MaxLines:        0,
	Pace:            0,
	Replay:          false,
	ReplaySpeed:     1.0,
	ReplayMaxSleep:  0,
	AccessLog:       false,
	JSON:            false,
	TimestampLayout: time.RFC3339,

	SearchEnabled: true,
	FullRefresh:   2 * time.Second,
	PartialSize:   0,

	StatsEnabled: true,
	StatsWindow:  256,

	AltScreen: true,
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

func main() {
	flag.IntVar(&config.K, "k", config.K, "Track the top K items")
	flag.IntVar(&config.Width, "width", config.Width, "Sketch width")
	flag.IntVar(&config.Depth, "depth", config.Depth, "Sketch depth")
	flag.DurationVar(&config.WindowSize, "window", config.WindowSize, "Window size")
	flag.DurationVar(&config.TickSize, "tick", config.TickSize, "Sliding window tick size (time bucket precision)")
	flag.Float64Var(&config.Decay, "decay", config.Decay, "Counter decay probability on collisions")
	flag.IntVar(&config.DecayLUTSize, "decay-lut-size", config.DecayLUTSize, "Sketch decay look-up table size")
	flag.IntVar(&config.PlotFPS, "plot-fps", config.PlotFPS, "Plot refresh rate (frames per second)")
	flag.IntVar(&config.ItemsFPS, "items-fps", config.ItemsFPS, "Item refresh rate (frames per second)")
	flag.IntVar(&config.ItemCountsFPS, "item-counts-fps", config.ItemCountsFPS, "Item counts refresh rate (frames per second; 0 disables)")
	flag.StringVar(&config.InputPath, "in", config.InputPath, "Read input from this file instead of stdin")
	flag.IntVar(&config.MaxLines, "max-lines", config.MaxLines, "Stop after reading this many records (0 = unlimited)")
	flag.DurationVar(&config.Pace, "pace", config.Pace, "Sleep between input records (e.g. 5ms, 50ms)")
	flag.BoolVar(&config.Replay, "replay", config.Replay, "Replay timestamped input in (scaled) real time (requires -access-log or -json with timestamps)")
	flag.Float64Var(&config.ReplaySpeed, "replay-speed", config.ReplaySpeed, "Replay speed factor (1=real-time, 2=2x faster, 0.5=2x slower)")
	flag.DurationVar(&config.ReplayMaxSleep, "replay-max-sleep", config.ReplayMaxSleep, "Cap per-record replay sleep (0 = no cap)")
	flag.BoolVar(&config.AccessLog, "access-log", config.AccessLog, "Parse access log lines into {item,timestamp} records (item=client IP)")
	flag.BoolVar(&config.JSON, "json", config.JSON, "Read JSON records {item,[count],[timestamp]} instead of text lines")
	flag.BoolVar(&config.TrackSelected, "track-selected", config.TrackSelected, "Keep the selected item focused")
	flag.BoolVar(&config.LogScale, "log-scale", config.LogScale, "Use a logarithmic Y axis scale (default: linear)")
	flag.StringVar(&config.TimestampLayout, "json-timestamp-layout", config.TimestampLayout, "Layout for string values of the timestamp field")
	flag.IntVar(&config.ViewSplit, "view-split", config.ViewSplit, "Split the view at this % of the total screen width [20,80]")

	flag.BoolVar(&config.SearchEnabled, "search", config.SearchEnabled, "Enable search/filtering in the leaderboard list")
	flag.DurationVar(&config.FullRefresh, "full-refresh", config.FullRefresh, "How often to do a full Top-K refresh (0 = always)")
	flag.IntVar(&config.PartialSize, "partial-size", config.PartialSize, "How many items to partially refresh/sort per tick (0 = auto budget, about half of K)")
	flag.BoolVar(&config.StatsEnabled, "stats", config.StatsEnabled, "Show runtime performance stats")
	flag.IntVar(&config.StatsWindow, "stats-window", config.StatsWindow, "Number of recent samples kept per metric")
	flag.BoolVar(&config.AltScreen, "alt-screen", config.AltScreen, "Use the terminal alternate screen buffer (recommended inside IDE terminals)")

	flag.Parse()

	if err := validateAndNormalizeConfig(); err != nil {
		log.Fatal(err)
	}

	if config.ReplaySpeed <= 0 {
		config.ReplaySpeed = 1.0
	}
	config.ViewSplit = max(20, config.ViewSplit)
	config.ViewSplit = min(80, config.ViewSplit)
	if config.StatsWindow < 16 {
		config.StatsWindow = 16
	}

	sketch := sliding.New(config.K,
		int(config.WindowSize/config.TickSize),
		sliding.WithWidth(config.Width),
		sliding.WithDepth(config.Depth),
		sliding.WithDecay(float32(config.Decay)),
		sliding.WithDecayLUTSize(config.DecayLUTSize),
	)

	m := newModel(sketch)
	opts := []tui.ProgramOption{tui.WithInputTTY()}
	if config.AltScreen {
		opts = append(opts, tui.WithAltScreen())
	}
	if _, err := tui.NewProgram(m, opts...).Run(); err != nil {
		log.Fatal(err)
	}
}

func validateAndNormalizeConfig() error {
	if config.K < 1 {
		return fmt.Errorf("-k must be >= 1")
	}
	if config.Width < 1 {
		return fmt.Errorf("-width must be >= 1")
	}
	if config.Depth < 1 {
		return fmt.Errorf("-depth must be >= 1")
	}
	if config.Decay < 0 || config.Decay > 1 {
		return fmt.Errorf("-decay must be in [0,1]")
	}
	if config.DecayLUTSize < 1 {
		return fmt.Errorf("-decay-lut-size must be >= 1")
	}
	if config.TickSize <= 0 {
		return fmt.Errorf("-tick must be > 0")
	}
	if config.WindowSize <= 0 {
		return fmt.Errorf("-window must be > 0")
	}
	if config.WindowSize < config.TickSize {
		return fmt.Errorf("-window must be >= -tick")
	}
	if config.WindowSize%config.TickSize != 0 {
		return fmt.Errorf("-window must be a multiple of -tick (got window=%s tick=%s)", config.WindowSize, config.TickSize)
	}
	if config.PlotFPS < 1 {
		return fmt.Errorf("-plot-fps must be >= 1")
	}
	if config.ItemsFPS < 1 {
		return fmt.Errorf("-items-fps must be >= 1")
	}
	if config.ItemCountsFPS < 0 {
		return fmt.Errorf("-item-counts-fps must be >= 0")
	}
	if config.MaxLines < 0 {
		return fmt.Errorf("-max-lines must be >= 0")
	}
	if config.Pace < 0 {
		return fmt.Errorf("-pace must be >= 0")
	}
	if config.ReplaySpeed <= 0 {
		return fmt.Errorf("-replay-speed must be > 0")
	}
	if config.ReplayMaxSleep < 0 {
		return fmt.Errorf("-replay-max-sleep must be >= 0")
	}
	if config.Replay && !(config.AccessLog || config.JSON) {
		return fmt.Errorf("-replay requires -access-log or -json")
	}
	if config.AccessLog && config.JSON {
		return fmt.Errorf("choose only one: -access-log or -json")
	}
	if config.FullRefresh < 0 {
		return fmt.Errorf("-full-refresh must be >= 0")
	}
	if config.PartialSize < 0 {
		return fmt.Errorf("-partial-size must be >= 0")
	}
	return nil
}

type model struct {
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

	sketch         *sliding.Sketch
	sketchMu       sync.Mutex
	plotData       [][]float64
	plotLineColors []plot.Color
	listItems      []heap.Item
	latestTick     time.Time

	timestampsFromData atomic.Bool

	ranker  *IncrementalRanker
	metrics *latencyMetrics

	done chan struct{}
	mu   sync.Mutex
}

func newModel(sketch *sliding.Sketch) *model {
	const (
		defaultWidth  = 80
		defaultHeight = 20
	)

	d := list.NewDefaultDelegate()
	d.Styles.SelectedTitle = styles.NewStyle().
		Border(styles.NormalBorder(), false, false, false, true).
		BorderForeground(borderColor).
		Foreground(selectedColor).
		Bold(false).
		Padding(0, 0, 0, 1)
	d.Styles.SelectedDesc = d.Styles.SelectedTitle.
		Foreground(selectedColor)
	d.ShowDescription = true

	l := list.New(make([]list.Item, 0), d, defaultWidth/2-2, defaultHeight)
	l.Styles.NoItems = l.Styles.NoItems.
		Padding(0, 2)
	l.SetFilteringEnabled(config.SearchEnabled)
	l.SetShowHelp(false)
	l.SetShowTitle(false)
	l.SetShowStatusBar(false)

	p := plot.NewCanvas(defaultWidth, defaultHeight)
	p.NumDataPoints = sketch.BucketHistoryLength
	p.ShowAxis = false
	p.LineColors = make([]plot.Color, config.K+1)

	help := help.New()

	ranker := NewIncrementalRanker(config.K, config.FullRefresh, config.PartialSize)
	metrics := newLatencyMetrics(config.StatsWindow)
	metrics.setEnabled(config.StatsEnabled)

	m := &model{
		track:          config.TrackSelected,
		sketch:         sketch,
		help:           help,
		list:           l,
		listDelegate:   &d,
		plot:           &p,
		plotData:       make([][]float64, config.K+1),
		plotLineColors: make([]plot.Color, config.K+1),
		ranker:         ranker,
		metrics:        metrics,
		done:           make(chan struct{}),
	}
	m.leftPaneWidth, m.rightPaneWidth = computePaneWidths(defaultWidth, config.ViewSplit)
	m.pauseCond = sync.NewCond(&m.pauseMu)
	// Default: advance time in real-time (stdin has no timestamps).
	// Timestamped modes (-json with timestamps, -access-log) will enable this.
	m.timestampsFromData.Store(false)
	m.logScale.Store(config.LogScale)
	for i := range m.plotData {
		m.plotData[i] = make([]float64, m.sketch.BucketHistoryLength)
	}
	m.plot.Fill(m.plotData)
	return m
}

func (m *model) leftWidth() int {
	if m.leftPaneWidth > 0 {
		return m.leftPaneWidth
	}
	left, _ := computePaneWidths(m.width, config.ViewSplit)
	return left
}

func (m *model) rightWidth() int {
	if m.rightPaneWidth > 0 {
		return m.rightPaneWidth
	}
	_, right := computePaneWidths(m.width, config.ViewSplit)
	return right
}

func (m *model) readAndCountInput() tui.Cmd {
	return func() tui.Msg {
		r, ok, err := m.openInput()
		if err != nil {
			return errMsg{err}
		}
		if !ok {
			return nil
		}
		defer func() { _ = r.Close() }()
		switch {
		case config.AccessLog:
			// Stay in realtime-tick mode until we see a valid timestamp.
			m.timestampsFromData.Store(false)
			if err := m.readAccessLogItems(r); err != nil {
				return errMsg{err}
			}
		case config.JSON:
			// Enable data timestamps only after we see a valid timestamp.
			m.timestampsFromData.Store(false)
			if err := m.readJSONItems(r); err != nil {
				return errMsg{err}
			}
		default:
			m.timestampsFromData.Store(false)
			if err := m.readTextItems(r); err != nil {
				return errMsg{err}
			}
		}
		return nil
	}
}

func (m *model) openInput() (io.ReadCloser, bool, error) {
	if config.InputPath != "" {
		f, err := os.Open(config.InputPath)
		if err != nil {
			return nil, false, err
		}
		return f, true, nil
	}
	if term.IsTerminal(os.Stdin.Fd()) {
		return nil, false, nil
	}
	return io.NopCloser(os.Stdin), true, nil
}

func (m *model) readTextItems(r io.Reader) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	n := 0
	for scanner.Scan() {
		if m.isDone() {
			return nil
		}
		m.waitIfPaused()
		if config.MaxLines > 0 && n >= config.MaxLines {
			return nil
		}
		item := scanner.Text()
		now := time.Now()
		m.sketchMu.Lock()
		m.sketch.Incr(item)
		m.sketchMu.Unlock()
		m.metrics.observeIngest(now)
		n++
		if config.Pace > 0 {
			time.Sleep(config.Pace)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func (m *model) readJSONItems(r io.Reader) error {
	dec := json.NewDecoder(bufio.NewReader(r))
	var last time.Time
	var prevEvent time.Time
	useEventTime := false
	n := 0
	for {
		item := struct {
			Item      string `json:"item"`
			Count     int    `json:"count"`
			Timestamp any    `json:"timestamp"`
		}{}

		if m.isDone() {
			return nil
		}
		m.waitIfPaused()
		if config.MaxLines > 0 && n >= config.MaxLines {
			return nil
		}
		err := dec.Decode(&item)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		eventTime := time.Time{}
		if item.Timestamp != nil {
			switch timestamp := item.Timestamp.(type) {
			case int:
				eventTime = time.Unix(int64(timestamp), 0)
			case float64:
				eventTime = time.Unix(int64(timestamp), 0)
			case string:
				eventTime, _ = time.Parse(config.TimestampLayout, timestamp)
			}

		}

		if config.Replay && eventTime.IsZero() {
			return fmt.Errorf("replay enabled but JSON record has missing/invalid timestamp")
		}

		if !eventTime.IsZero() {
			if !useEventTime {
				useEventTime = true
				m.timestampsFromData.Store(true)
			}
			if config.Replay && !prevEvent.IsZero() {
				sleep := time.Duration(float64(eventTime.Sub(prevEvent)) / config.ReplaySpeed)
				if sleep > 0 {
					if config.ReplayMaxSleep > 0 && sleep > config.ReplayMaxSleep {
						sleep = config.ReplayMaxSleep
					}
					time.Sleep(sleep)
				}
			}
			prevEvent = eventTime
			last = m.doSketchTicks(eventTime, last)
			m.mu.Lock()
			m.latestTick = last
			m.mu.Unlock()
		} else if !useEventTime {
			// Stay in realtime-tick mode until we see a valid timestamp.
			m.timestampsFromData.Store(false)
		}

		inc := item.Count
		if inc < 1 {
			inc = 1
		}
		now := time.Now()
		m.sketchMu.Lock()
		m.sketch.Add(item.Item, uint32(inc))
		m.sketchMu.Unlock()
		m.metrics.observeIngest(now)

		n++
		if !config.Replay && config.Pace > 0 {
			time.Sleep(config.Pace)
		}
	}
}

func (m *model) readAccessLogItems(r io.Reader) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var last time.Time
	var prevEvent time.Time
	useEventTime := false
	n := 0
	for scanner.Scan() {
		if m.isDone() {
			return nil
		}
		m.waitIfPaused()
		if config.MaxLines > 0 && n >= config.MaxLines {
			return nil
		}
		line := scanner.Text()
		ip, ts, ok := strings.Cut(line, " - - [")
		if !ok {
			continue
		}
		ts, _, ok = strings.Cut(ts, "]")
		if !ok {
			continue
		}

		eventTime, err := time.Parse(config.TimestampLayout, ts)
		if err == nil && !eventTime.IsZero() {
			if !useEventTime {
				useEventTime = true
				m.timestampsFromData.Store(true)
			}
			if config.Replay && !prevEvent.IsZero() {
				sleep := time.Duration(float64(eventTime.Sub(prevEvent)) / config.ReplaySpeed)
				if sleep > 0 {
					if config.ReplayMaxSleep > 0 && sleep > config.ReplayMaxSleep {
						sleep = config.ReplayMaxSleep
					}
					time.Sleep(sleep)
				}
			}
			prevEvent = eventTime
			last = m.doSketchTicks(eventTime, last)
			m.mu.Lock()
			m.latestTick = last
			m.mu.Unlock()
		} else if config.Replay {
			return fmt.Errorf("replay enabled but access-log record has missing/invalid timestamp")
		}

		now := time.Now()
		m.sketchMu.Lock()
		m.sketch.Incr(ip)
		m.sketchMu.Unlock()
		m.metrics.observeIngest(now)

		n++
		if !config.Replay && config.Pace > 0 {
			time.Sleep(config.Pace)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func (m *model) sketchTickCmd() tui.Cmd {
	return func() tui.Msg {
		var last time.Time
		ticker := time.NewTicker(time.Duration(config.TickSize))
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
				t = t.Truncate(config.TickSize)
				m.mu.Lock()
				m.latestTick = t
				m.mu.Unlock()
				last = m.doSketchTicks(t, last)
			}
		}
	}
}

func (m *model) doSketchTicks(t time.Time, last time.Time) time.Time {
	t = t.Truncate(config.TickSize)
	if last.IsZero() {
		last = t
		return last
	}
	if ticks := int(t.Sub(last) / config.TickSize); ticks > 0 {
		m.sketchMu.Lock()
		m.sketch.Ticks(ticks)
		m.sketchMu.Unlock()
		last = t
	}
	return last
}

type ItemsTickMsg time.Time

func doItemsTick() tui.Cmd {
	return tui.Every(time.Second/time.Duration(config.ItemsFPS), func(t time.Time) tui.Msg {
		return ItemsTickMsg(t)
	})
}

type ItemCountsTickMsg time.Time

func doItemCountsTick() tui.Cmd {
	if config.ItemCountsFPS <= 0 || config.ItemCountsFPS == config.ItemsFPS {
		return nil
	}
	return tui.Every(time.Second/time.Duration(config.ItemCountsFPS), func(t time.Time) tui.Msg {
		return ItemCountsTickMsg(t)
	})
}

type PlotTickMsg time.Time

func doPlotTick() tui.Cmd {
	return tui.Every(time.Second/time.Duration(config.PlotFPS), func(t time.Time) tui.Msg {
		return PlotTickMsg(t)
	})
}

type errMsg struct{ err error }

func (m *model) Init() tui.Cmd {
	return tui.Batch(m.sketchTickCmd(), m.readAndCountInput(), doPlotTick(), doItemsTick(), doItemCountsTick())
}

func (m *model) Update(msg tui.Msg) (tui.Model, tui.Cmd) {
	switch msg := msg.(type) {
	case errMsg:
		m.mu.Lock()
		m.err = msg.err
		m.mu.Unlock()
		return m, nil
	case ItemCountsTickMsg:
		if m.isPaused() {
			return m, doItemCountsTick()
		}
		m.updateListItemCountsFromSketch()
		m.list.Update(msg)
		cmdList := m.updateList(msg)
		return m, tui.Batch(cmdList, doItemCountsTick())
	case ItemsTickMsg:
		if m.isPaused() {
			return m, doItemsTick()
		}
		m.updateTopKIncremental()
		cmdList := m.updateList(msg)
		return m, tui.Batch(cmdList, doItemsTick())
	case PlotTickMsg:
		cmdPlot := m.updatePlot(msg)
		return m, tui.Batch(cmdPlot, doPlotTick())
	case tui.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.leftPaneWidth, m.rightPaneWidth = computePaneWidths(m.width, config.ViewSplit)
		statsLines := 0
		if config.StatsEnabled {
			// title + 6 metric lines
			statsLines = 7
		}
		helpLines := 1
		bottomLines := statsLines + helpLines
		available := m.height - bottomLines
		available = max(1, available)

		leftW := max(1, m.leftWidth())
		rightW := max(1, m.rightWidth())

		m.list.SetSize(leftW, available)
		m.list.Styles.Title = styles.NewStyle()
		m.list.Styles.PaginationStyle = styles.NewStyle()
		m.list.Styles.HelpStyle = styles.NewStyle()
		m.listStyle = styles.NewStyle().Width(leftW).Height(available)

		// Right side is: plot canvas + 1 label line, wrapped in a border (adds 2 lines).
		plotHeight := available - 3
		plotHeight = max(1, plotHeight)
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

func (m *model) toggleTracking() {
	m.mu.Lock()
	m.track = !m.track
	m.mu.Unlock()
}

func (m *model) toggleScale() {
	for {
		old := m.logScale.Load()
		if m.logScale.CompareAndSwap(old, !old) {
			return
		}
	}
}

func (m *model) togglePause() {
	m.pauseMu.Lock()
	m.paused = !m.paused
	m.pauseMu.Unlock()
	m.pauseCond.Broadcast()
}

func (m *model) isPaused() bool {
	m.pauseMu.Lock()
	defer m.pauseMu.Unlock()
	return m.paused
}

func (m *model) waitIfPaused() {
	m.pauseMu.Lock()
	for m.paused && !m.isDone() {
		m.pauseCond.Wait()
	}
	m.pauseMu.Unlock()
}

func (m *model) isDone() bool {
	select {
	case <-m.done:
		return true
	default:
		return false
	}
}

func (m *model) shutdown() {
	close(m.done)
	m.pauseCond.Broadcast()
}

func (m *model) updateListItemCountsFromSketch() {
	m.mu.Lock()
	items := make([]heap.Item, len(m.listItems))
	copy(items, m.listItems)
	m.mu.Unlock()

	m.sketchMu.Lock()
	for i := range items {
		items[i].Count = m.sketch.Count(items[i].Item)
	}
	m.sketchMu.Unlock()

	m.mu.Lock()
	m.listItems = items
	m.mu.Unlock()
}

func (m *model) updateTopKIncremental() {
	start := time.Now()
	items, _ := m.ranker.Refresh(
		start,
		0,
		func() []heap.Item {
			m.sketchMu.Lock()
			s := m.sketch.SortedSlice()
			m.sketchMu.Unlock()
			return s
		},
		func(items []heap.Item, limit int) {
			m.sketchMu.Lock()
			for i := 0; i < limit; i++ {
				items[i].Count = m.sketch.Count(items[i].Item)
			}
			m.sketchMu.Unlock()
		},
	)
	m.metrics.observeTopKRefresh(start)
	m.mu.Lock()
	m.listItems = items
	m.mu.Unlock()
}

func (m *model) resizePlot(w int, h int) {
	p := plot.NewCanvas(w, h)
	p.NumDataPoints = m.plot.NumDataPoints
	p.ShowAxis = m.plot.ShowAxis
	p.LineColors = m.plot.LineColors
	m.plot = &p
}

func (m *model) updateList(msg tui.Msg) tui.Cmd {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := make([]list.Item, len(m.listItems))
	order := make(map[string]int)

	m.listDelegate.Styles.SelectedTitle = m.listDelegate.Styles.SelectedTitle.Bold(m.track)
	m.listDelegate.Styles.SelectedDesc = m.listDelegate.Styles.SelectedDesc.Bold(m.track)
	m.list.SetDelegate(m.listDelegate)

	numDecimals := 1 + int(math.Ceil(math.Log10(float64(config.K+1))))
	padToItemRankWidth := strings.Repeat(" ", numDecimals+1)
	itemRankFormat := "#%-" + fmt.Sprint(numDecimals) + "d"
	for i, item := range m.listItems {
		items[i] = listItem{
			DescriptionPrefix: padToItemRankWidth,
			TitlePrefix:       fmt.Sprintf(itemRankFormat, i+1),
			Item:              item,
		}
		order[item.Item] = i
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

func (m *model) updatePlot(_ tui.Msg) tui.Cmd {
	logScale := m.logScale.Load()

	var highlight, dim plot.Color
	if styles.DefaultRenderer().HasDarkBackground() {
		highlight, dim = plot.Red, plot.DimGray
	} else {
		highlight, dim = plot.Black, plot.LightGray
	}

	m.mu.Lock()
	selected := m.list.Index()
	items := make([]heap.Item, len(m.listItems))
	copy(items, m.listItems)
	m.mu.Unlock()
	if len(items) == 0 {
		return nil
	}

	for i := range m.plotData {
		m.plotLineColors[i] = dim
	}
	m.sketchMu.Lock()
	for i := range items {
		series := m.plotData[i]
		item := items[(1+selected+i)%len(items)]

		m.fillSeriesFromSketch(item, series, logScale)
	}
	m.sketchMu.Unlock()
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

func (m *model) fillSeriesFromSketch(item heap.Item, series []float64, logScale bool) {
	bucketIdx := make([]int, 0, m.sketch.Depth)
	for k := 0; k < m.sketch.Depth; k++ {
		idx := topk.BucketIndex(item.Item, k, m.sketch.Width)
		b := m.sketch.Buckets[idx]
		if b.Fingerprint == item.Fingerprint && len(b.Counts) > 0 {
			bucketIdx = append(bucketIdx, idx)
		}
	}

	if len(bucketIdx) == 0 {
		for j := range series {
			series[len(series)-1-j] = 0
		}
		return
	}

	for j := range series {
		var maxCount uint32
		for _, idx := range bucketIdx {
			b := m.sketch.Buckets[idx]
			c := b.Counts[(int(b.First)+j)%len(b.Counts)]
			maxCount = max(maxCount, c)
		}
		value := float64(maxCount)
		if logScale {
			value = math.Log(max(1, value))
		}
		series[len(series)-1-j] = value
	}
}

func (m *model) View() string {
	left := m.listStyle.Render(m.list.View())
	plot := m.plot.String()

	if plot == "" {
		sb := emptyPlot(m)
		plot = sb.String()
	}

	linColor := borderFg
	logColor := borderFg
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
		w := m.rightWidth() - 2
		if w < 0 {
			w = 0
		}
		leftLabel := latestTick.Add(-config.WindowSize).UTC().Format(time.RFC3339)
		rightLabel := latestTick.UTC().Format(time.RFC3339)
		minWidth := len(leftLabel) + len(rightLabel) + len("LIN LOG") + 4

		// Fall back to short timestamps when the pane is narrow.
		if w < minWidth {
			leftLabel = latestTick.Add(-config.WindowSize).UTC().Format("15:04:05")
			rightLabel = latestTick.UTC().Format("15:04:05")
			minWidth = len(leftLabel) + len(rightLabel) + len("LIN LOG") + 4
		}
		// If still too narrow, show only the scale hint to avoid wrapping.
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
	right := plotStyle.Render(styles.JoinVertical(styles.Top, plot, labels))
	view := styles.JoinHorizontal(styles.Top, left, right)

	m.mu.Lock()
	err := m.err
	m.mu.Unlock()
	if err != nil {
		errStyle := styles.NewStyle().Foreground(styles.AdaptiveColor{Light: "1", Dark: "9"})
		return styles.JoinVertical(styles.Left, view, errStyle.Render("ERROR: "+err.Error()), m.help.View(keys))
	}

	var statsBlock []string
	if config.StatsEnabled {
		snap := m.metrics.snapshot()
		title := "PERF STATS (RUNNING)"
		if m.isPaused() {
			title = "PERF STATS (PAUSED)"
		}

		topItem := "-"
		var topCount uint32
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
					tracked = fmt.Sprintf("%s (%d)", li.Item.Item, li.Count)
				}
			}
		}
		lag := "n/a"
		if snap.records > 0 {
			if m.isPaused() {
				lag = "paused"
			} else {
				lag = formatMetricDuration(snap.ingestLag)
			}
		}

		statsBlock = []string{
			title,
			fmt.Sprintf("records: %d", snap.records),
			fmt.Sprintf("ingest rate: %d rec/s", snap.ingestRps),
			fmt.Sprintf("pipeline lag p95: %s", formatMetricDuration(snap.rankLagP95)),
			fmt.Sprintf("data freshness lag: %s", lag),
			fmt.Sprintf("top-1: %s (%d)", topItem, topCount),
			fmt.Sprintf("track: %s", tracked),
		}
	}

	if len(statsBlock) != 0 {
		statsStyle := styles.NewStyle().Foreground(styles.AdaptiveColor{Light: "1", Dark: "9"})
		statsText := strings.Join(statsBlock, "\n")
		return styles.JoinVertical(styles.Left, view, statsStyle.Render(statsText), m.help.View(keys))
	}
	return styles.JoinVertical(styles.Left, view, m.help.View(keys))
}

func emptyPlot(m *model) strings.Builder {
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

func formatMetricDuration(d time.Duration) string {
	if d <= 0 {
		return "0.000ms"
	}
	return fmt.Sprintf("%.3fms", float64(d)/float64(time.Millisecond))
}

func computePaneWidths(totalWidth int, splitPercent int) (left, right int) {
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

	// Keep panes readable when the terminal is wide enough.
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
	heap.Item
}

func (i listItem) Title() string       { return fmt.Sprintf("%s %s", i.TitlePrefix, i.Item.Item) }
func (i listItem) Description() string { return fmt.Sprintf("%s %d", i.DescriptionPrefix, i.Count) }
func (i listItem) FilterValue() string { return i.Item.Item }

func (k keyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.Quit, k.Pause, k.Track, k.Scale}
}

func (k keyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{
		{k.Quit, k.Pause},
		{k.Track, k.Scale},
	}
}

type keyMap struct {
	Track key.Binding
	Scale key.Binding
	Pause key.Binding
	Quit  key.Binding
}

var keys = keyMap{
	Track: key.NewBinding(
		key.WithKeys("t", " "),
		key.WithHelp("t/space", "track"),
	),
	Scale: key.NewBinding(
		key.WithKeys("s"),
		key.WithHelp("s", "log/lin"),
	),
	Pause: key.NewBinding(
		key.WithKeys("p"),
		key.WithHelp("p", "pause"),
	),
	Quit: key.NewBinding(
		key.WithKeys("q", "ctrl+c"),
		key.WithHelp("q/ctrl+c", "quit"),
	),
}
